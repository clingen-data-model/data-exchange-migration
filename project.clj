(defproject data-exchange-migration "0.1.0-SNAPSHOT"
  :description "Migration to Confluent Cloud for ClinGen Data Exchange"
  :url "https://github.com/clingen-data-model/data-exchange-migration"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [fundingcircle/jackdaw "0.7.4"]
                 [com.taoensso/timbre "4.10.0"]]
  :main ^:skip-aot data-exchange-migration.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :uberjar-name "data-exchange-migration.jar"}}
  )
