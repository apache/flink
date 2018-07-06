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
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [cli :as cli]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.flink.client :refer :all]
            [jepsen.flink.checker :as flink-checker]
            [jepsen.flink.db :as fdb]
            [jepsen.flink.nemesis :as fn])
  (:import (jepsen.flink.client Client)))

(def flink-test-config
  {:yarn-session  {:db                  (fdb/flink-yarn-db)
                   :deployment-strategy fdb/start-yarn-session!}
   :yarn-job      {:db                  (fdb/flink-yarn-db)
                   :deployment-strategy fdb/start-yarn-job!}
   :mesos-session {:db                  (fdb/flink-mesos-db)
                   :deployment-strategy fdb/start-mesos-session!}})

(defn client-gen
  []
  (->
    (cons {:type :invoke, :f :submit, :value nil}
          (cycle [{:type :invoke, :f :job-running?, :value nil}
                  (gen/sleep 5)]))
    (gen/seq)
    (gen/singlethreaded)))

(defn flink-test
  [opts]
  (merge tests/noop-test
         (let [{:keys [db deployment-strategy]} (-> opts :deployment-mode flink-test-config)
               {:keys [job-running-healthy-threshold job-recovery-grace-period]} opts]
           {:name      "Apache Flink"
            :os        debian/os
            :db        db
            :nemesis   (fn/nemesis)
            :model     (flink-checker/job-running-within-grace-period
                         job-running-healthy-threshold
                         job-recovery-grace-period)
            :generator (let [stop (atom nil)]
                         (->> (fn/stoppable-generator stop (client-gen))
                              (gen/nemesis
                                (fn/stop-generator stop
                                                   ((fn/nemesis-generator-factories (:nemesis-gen opts)) opts)
                                                   job-running-healthy-threshold
                                                   job-recovery-grace-period))))
            :client    (Client. deployment-strategy nil nil nil nil)
            :checker   (flink-checker/job-running-checker)})
         (assoc opts :concurrency 1)))

(defn keys-as-allowed-values-help-text
  "Takes a map and returns a string explaining which values are allowed.
  This is a CLI helper function."
  [m]
  (->> (keys m)
       (map name)
       (clojure.string/join ", ")
       (str "Must be one of: ")))

(defn -main
  [& args]
  (cli/run!
    (merge
      (cli/single-test-cmd
        {:test-fn  flink-test
         :tarball  fdb/default-flink-dist-url
         :opt-spec [[nil "--ha-storage-dir DIR" "high-availability.storageDir"]
                    [nil "--job-jar JAR" "Path to the job jar"]
                    [nil "--job-args ARGS" "CLI arguments for the flink job"]
                    [nil "--main-class CLASS" "Job main class"]
                    [nil "--nemesis-gen GEN" (str "Which nemesis should be used?"
                                                  (keys-as-allowed-values-help-text fn/nemesis-generator-factories))
                     :parse-fn keyword
                     :default :kill-task-managers
                     :validate [#(fn/nemesis-generator-factories (keyword %))
                                (keys-as-allowed-values-help-text fn/nemesis-generator-factories)]]
                    [nil "--deployment-mode MODE" (keys-as-allowed-values-help-text flink-test-config)
                     :parse-fn keyword
                     :default :yarn-session
                     :validate [#(flink-test-config (keyword %))
                                (keys-as-allowed-values-help-text flink-test-config)]]
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
