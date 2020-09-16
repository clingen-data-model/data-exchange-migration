(ns data-exchange-migration.core
  (:require [data-exchange-migration.config :as cfg]
            [data-exchange-migration.util :as util]
            [jackdaw.client :as jc]
            [jackdaw.admin :as ja]
            [jackdaw.data :as jd]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [clojure.string :as s])
  (:import [org.apache.kafka.common TopicPartition PartitionInfo]
           [org.apache.kafka.clients.consumer ConsumerRecord]
           [java.lang Thread]
           [java.time Duration]
           (org.apache.kafka.clients.admin AdminClient)
           (java.util Properties))
  (:gen-class))

(log/set-level! :debug)
(def consumer-poll-timeout 250)

(defn reset-topic-offset
  "Force seek to beginning of the topic for this consumer group"
  [consumer-config topic-name]
  ; Doesn't always work
  (with-open [consumer (jc/consumer consumer-config)]
    (doseq [partition-info (.partitionsFor consumer topic-name)]
      (log/info "resetting partition" partition-info)
      (jc/subscribe consumer [{:topic-name topic-name}])
      (jc/poll consumer 0)
      (jc/seek-to-beginning-eager consumer [(TopicPartition. topic-name (.partition partition-info))]))))

(defn reset-consumer-offset
  "Resets the consumer offset to the beginning of the topic. Is stateful on the consumer, but also returns the consumer."
  [consumer]
  (log/info "Resetting consumer to beginning of all topics")
  (jc/seek-to-beginning-eager consumer)
  consumer)

;(defn reset-topic-offset-lazy
;  "Lazy seek to beginning of stream. Does not alter stored offset in kafka broker until next read from this group"
;  [consumer-config topic-name]
;  (with-open [consumer (jc/consumer consumer-config)]
;    (doseq [^PartitionInfo partition-info (.partitionsFor consumer topic-name)]
;      (println "resetting partition" partition-info)
;      ; Open another consumer per partition
;      (with-open [reset-consumer (jc/consumer consumer-config)]
;        (jc/subscribe reset-consumer [{:topic-name topic-name}])
;        ; Subscribe is lazy, so do throwaway poll() call
;        ; Unfortunately this will still read some messages
;        (jc/poll reset-consumer 1)
;        (let [partition (TopicPartition. topic-name (.partition partition-info))]
;          ;(jc/assign reset-consumer partition)
;          (jc/seek reset-consumer
;                   partition
;                   0)))
;      )))

(def do-reset-input-offsets (util/to-bool (util/get-env-required "DX_MIGRATION_RESET_OFFSET")))

;(defn create-pipe
;  ""
;  [{:keys [consumer-config producer-config consumer-topic producer-topic reset-consumer]
;    :or {reset-consumer true}}]
;  (with-open [producer (jc/producer producer-config)
;              consumer (jc/consumer consumer-config)]
;    (jc/subscribe consumer [{:topic-name consumer-topic}])
;    (if reset-input-offsets
;      (reset-consumer-offset consumer))
;    (while true
;      (let [msgs (jc/poll consumer consumer-poll-timeout)]
;        (if (> (count msgs) 0)
;          (infof "Sending %d messages to %s" (count msgs) producer-topic)
;          (doseq [msg msgs]
;            ; Reproduce the message using the same key as in the consumer topic
;            ; timestamp and partition are not copied, and partitioning of the stream is not mirrored
;            ; https://github.com/FundingCircle/jackdaw/blob/daa838757a90e54d57165e29ff3c65824730e4f5/src/jackdaw/data/producer.clj#L15
;            (jc/send! producer (jd/->ProducerRecord {:topic-name producer-topic}
;                                           (:key msg)
;                                           (:value msg)))))
;        ))))

(defn create-pipe
  "Creates a pipe from one topic to another, which can span kafka clusters based on producer/consumer configs."
  [{:keys [consumer-config producer-config topic-map reset-consumer]
    :or {reset-consumer true}}]
  (with-open [producer (jc/producer producer-config)
              consumer (jc/consumer consumer-config)]
    ; Subscribe to keys (consumer topics) from topic map (consumer-topic -> producer-topic)
    ; key keywords must be converted to string (name)
    (let [consumer-topics (for [[k v] topic-map] {:topic-name (name k)})]
      (log/debug "Subscribing to consumer topics " (s/join "," consumer-topics))
      (jc/subscribe consumer consumer-topics))

    (if do-reset-input-offsets
      (reset-consumer-offset consumer))
    (log/info "Starting to poll")
    (while true
      (let [msgs (jc/poll consumer (Duration/ofMillis consumer-poll-timeout))]
        (if (< 0 (count msgs))
          (do
            (log/infof "Sending %d messages to producer" (count msgs))
            (doseq [msg msgs]
              ; msg is a datified ConsumerRecord, but .topic is :topic-name
              (let [consumer-topic (:topic-name msg)
                    producer-topic ((keyword (:topic-name msg)) topic-map)
                    key (:key msg)
                    value (:value msg)]
                (log/debugf "sending message (key=%s) from %s to %s" key consumer-topic producer-topic)
                ; Reproduce the message using the same key as in the consumer topic
                ; timestamp and partition are not copied, and partitioning of the stream is not mirrored
                ; https://github.com/FundingCircle/jackdaw/blob/daa838757a90e54d57165e29ff3c65824730e4f5/src/jackdaw/data/producer.clj#L15
                (jc/send! producer (jd/->ProducerRecord {:topic-name producer-topic} key value))
;                (jc/produce! producer {:topic-name producer-topic} key value)
                )
              ))
          )))
    ))

(defn write-map-to-file
  [m filename]
  (with-open [writer (io/writer filename)]
    (doseq [[k v] m]
      (.write writer (str k "=" v "\n")))))

(def topics ["gene_dosage"
             "gene_validity"
             "gene_validity_events"
             "gene_validity_events_dev"
             "actionability"
             "actionability_dev"
             "gene_validity_dev"
             "gene_validity_raw"
             "gene_validity_raw_dev"
             "variant_interpretation"
             "variant_interpretation_dev"
;             "gene_dosage_beta"
;             "test"
;             "variant_path_interp_dev"
;             "variant_path_interp"
             ])

; TODO complete topic-config
; This is handy so that a series of topics don't need to be created via the UI or cli.
; And resetting a topic can be done via the UI, and this can automatically re-create it.
; The difficulty is in providing a complete and desirable topic config.
; To start out this should just be copied directly from an auto-filled topic config from confluent UI.
(defn create-output-topic-if-not-exists
  "Creates a topic on the destination cluster if it doesn't exist.
  Requires producer config to have admin authorization."
  [topic-name]
  (let [admin-client-props (Properties.)]
    (doseq [[k v] cfg/producer-client-properties]
      (.put admin-client-props (str k) (str v)))
    (let [admin-client (AdminClient/create admin-client-props)]
      (if (not (ja/topic-exists? admin-client {:topic-name topic-name}))
        ; create-topics mapping for NewTopic
        ; https://github.com/FundingCircle/jackdaw/blob/04f654f54489b2c9bcf0984fa613fff925c549e5/src/jackdaw/data/admin.clj#L54
        (.createTopics admin-client [{:topic-name topic-name
                                      :partition-count 1
                                      :replication-factor 1
                                      ; https://docs.confluent.io/current/installation/configuration/topic-configs.html
                                      :topic-config {"cleanup.policy"   "delete"
                                                     "compression.type" "gzip"
                                                     "retention-ms"     -1
                                                     ; TODO
                                                     }}])
        (log/infof "Topic %s already exists" topic-name)))))

(defn -main [& args]
  (write-map-to-file cfg/consumer-client-properties "kafka-consumer.properties")
  (write-map-to-file cfg/producer-client-properties "kafka-producer.properties")

  ; Construct topic map {:<source-topic> "<dest-topic>"
  ;                      ...}
  (let [topic-map (into {} (for [t topics] [(keyword t) t]))]
    (create-pipe {:consumer-config cfg/consumer-client-properties
                  :producer-config cfg/producer-client-properties
                  :topic-map topic-map})))
