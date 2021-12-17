;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns jepsen.flink.flink
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [cli :as cli]
             [generator :as gen]
             [tests :as tests]
             [zookeeper :as zk]]
            [jepsen.os.debian :as debian]
            [jepsen.flink
             [client :refer :all]
             [checker :as flink-checker]
             [db :as fdb]
             [generator :as fg]
             [hadoop :as hadoop]
             [kafka :as kafka]
             [nemesis :as fn]]))

(def default-flink-dist-url "https://archive.apache.org/dist/flink/flink-1.6.0/flink-1.6.0-bin-hadoop28-scala_2.11.tgz")
(def hadoop-dist-url "https://archive.apache.org/dist/hadoop/common/hadoop-2.8.3/hadoop-2.8.3.tar.gz")
(def kafka-dist-url "http://mirror.funkfreundelandshut.de/apache/kafka/2.2.2/kafka_2.11-2.2.2.tgz")
(def deb-zookeeper-package "3.4.9-3+deb9u2")

(def dbs
  {:flink-yarn-job           (fdb/yarn-job-db)
   :flink-yarn-session       (fdb/yarn-session-db)
   :flink-standalone-session (fdb/start-flink-db)
   :hadoop                   (hadoop/db hadoop-dist-url)
   :kafka                    (kafka/db kafka-dist-url)
   :zookeeper                (zk/db deb-zookeeper-package)})

(def poll-jobs-running {:type :invoke, :f :jobs-running?, :value nil})
(def cancel-jobs {:type :invoke, :f :cancel-jobs, :value nil})
(def poll-jobs-running-loop (gen/seq (cycle [poll-jobs-running (gen/sleep 5)])))

(defn default-client-gen
  "Client generator that polls for the job running status."
  []
  (->
    poll-jobs-running-loop
    (gen/singlethreaded)))

(defn cancelling-client-gen
  "Client generator that polls for the job running status, and cancels the job after 15 seconds."
  []
  (->
    (gen/concat (gen/time-limit 15 (default-client-gen))
                (gen/once cancel-jobs)
                (default-client-gen))
    (gen/singlethreaded)))

(def client-gens
  {:poll-job-running default-client-gen
   :cancel-jobs      cancelling-client-gen})

(defn flink-test
  [opts]
  (merge tests/noop-test
         (let [dbs (->> opts :test-spec :dbs (map dbs))
               {:keys [job-running-healthy-threshold job-recovery-grace-period]} opts
               client-gen ((:client-gen opts) client-gens)]
           {:name      "Apache Flink"
            :os        debian/os
            :db        (fdb/combined-db dbs)
            :nemesis   (fn/nemesis)
            :generator (let [stop (atom nil)]
                         (->> (fg/stoppable-generator stop (client-gen))
                              (gen/nemesis
                                (fg/stop-generator stop
                                                   ((fn/nemesis-generator-factories (:nemesis-gen opts)) opts)
                                                   job-running-healthy-threshold
                                                   job-recovery-grace-period))))
            :client    (create-client)
            :checker   (flink-checker/job-running-checker job-running-healthy-threshold
                                                          job-recovery-grace-period)})
         (assoc opts :concurrency 1)))

(defn- keys->allowed-values-help-text
  "Takes a map and returns a string explaining which values are allowed.
  This is a CLI helper function."
  [m]
  (->> (keys m)
       (map name)
       (clojure.string/join ", ")
       (str "Must be one of: ")))

(defn read-test-spec
  [path]
  (clojure.edn/read-string (slurp path)))

(defn -main
  [& args]
  (cli/run!
    (merge
      (cli/single-test-cmd
        {:test-fn  flink-test
         :tarball  default-flink-dist-url
         :opt-spec [[nil "--test-spec FILE" "Path to a test specification (.edn)"
                     :parse-fn read-test-spec
                     :validate [#(->> % :dbs (map dbs) (every? (complement nil?)))
                                (str "Invalid :dbs specification. " (keys->allowed-values-help-text dbs))]]
                    [nil "--nemesis-gen GEN" (str "Which nemesis should be used? "
                                                  (keys->allowed-values-help-text fn/nemesis-generator-factories))
                     :parse-fn keyword
                     :default :kill-task-managers
                     :validate [#(fn/nemesis-generator-factories %)
                                (keys->allowed-values-help-text fn/nemesis-generator-factories)]]
                    [nil "--client-gen GEN" (str "Which client should be used? "
                                                 (keys->allowed-values-help-text client-gens))
                     :parse-fn keyword
                     :default :poll-job-running
                     :validate [#(client-gens %)
                                (keys->allowed-values-help-text client-gens)]]
                    [nil "--job-running-healthy-threshold TIMES" "Number of consecutive times the job must be running to be considered healthy."
                     :default 5
                     :parse-fn #(Long/parseLong %)
                     :validate [pos? "Must be positive"]]
                    [nil "--job-recovery-grace-period SECONDS" "Time period in which the job must become healthy."
                     :default 180
                     :parse-fn #(Long/parseLong %)
                     :validate [pos? "Must be positive" (fn [v] (<= 60 v)) "Should be greater than 60"]]]})
      (cli/serve-cmd))
    args))
