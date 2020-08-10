(ns data-exchange-migration.config
  (:import java.util.Properties
           [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecord]))

(def consumer-client-properties
  {"bootstrap.servers" (System/getenv "DX_MIGRATION_SOURCE_EXCHANGE_HOST")
   "group.id" (System/getenv "DX_MIGRATION_GROUP")
   "enable.auto.commit" "true"
   "auto.commit.interval.ms" "1000"
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "security.protocol" "SSL"
   "ssl.truststore.location" (System/getenv "DX_MIGRATION_TRUSTSTORE")
   "ssl.truststore.password" (System/getenv "DX_MIGRATION_TRUSTSTORE_PASS")
   "ssl.keystore.location" (System/getenv "DX_MIGRATION_KEYSTORE")
   "ssl.keystore.password" (System/getenv "DX_MIGRATION_KEYSTORE_PASS")
   "ssl.key.password" (System/getenv "DX_MIGRATION_KEY_PASS")
   "ssl.endpoint.identification.algorithm" "" ; Disable host name / CN identification
   })

; Producing to topics using confluent plain auth, not cert based auth
(def producer-client-properties
  {"ssl.endpoint.identification.algorithm" "https"
   "compression.type"                      "gzip"
   "sasl.mechanism"                        "PLAIN"
   "request.timeout.ms"                    "20000"
   "application.id"                        "clinvar-submissions-local"
   "bootstrap.servers"                     (System/getenv "DX_MIGRATION_DESTINATION_EXCHANGE_HOST")
   "retry.backoff.ms"                      "500"
   "security.protocol"                     "SASL_SSL"
   "key.serializer"                        "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                      "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"                    "org.apache.kafka.common.serialization.StringDeserializer"
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                "username=\"" (System/getenv "DX_MIGRATION_DESTINATION_USER") "\" "
                                                "password=\"" (System/getenv "DX_MIGRATION_DESTINATION_PASS") "\";")})

;;; Java Properties object defining configuration of Kafka client
;(defn client-configuration
;      "Create client "
;      []
;      (let [props (new Properties)]
;           (doseq [p client-properties]
;                  (.put props (p 0) (p 1)))
;           props))
;
;(defn consumer
;      []
;      (let [props (client-configuration)]
;           (new KafkaConsumer props)))