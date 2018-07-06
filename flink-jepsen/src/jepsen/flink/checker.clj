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

(ns jepsen.flink.checker
  (:require [jepsen
             [checker :as checker]
             [util :as ju]]
            [knossos.model :as model])
  (:import (knossos.model Model)))

(defn stoppable-op? [op]
  (clojure.string/includes? (name (:f op)) "-start"))

(defn stop-op? [op]
  (clojure.string/includes? (name (:f op)) "-stop"))

(defn strip-op-suffix [op]
  (clojure.string/replace (name (:f op)) #"-start|-stop" ""))

(def safe-inc
  (fnil inc 0))

(defn nemeses-active?
  [active-nemeses]
  (->> (vals active-nemeses)
       (reduce +)
       pos?))

(defn dissoc-if
  [f m]
  (->> (remove f m)
       (into {})))

(defn zero-value?
  [[_ v]]
  (zero? v))

(defrecord
  JobRunningWithinGracePeriod
  ^{:doc "A Model which is consistent iff. the Flink job became available within
  `job-recovery-grace-period` seconds after the last fault injected by the nemesis.
  Note that some faults happen at a single point in time (e.g., killing of processes). Other faults,
  such as network splits, happen during a period of time, and can thus be interleaving. As long as
  there are active faults, the job is allowed not to be available."}
  [active-nemeses                                           ; stores active failures
   healthy-count                                            ; how many consecutive times was the job running?
   last-failure                                             ; timestamp when the last failure was injected/ended
   healthy-threshold                                        ; after how many times is the job considered healthy
   job-recovery-grace-period]                               ; after how many seconds should the job be recovered
  Model
  (step [this op]
    (case (:process op)
      :nemesis (cond
                 (nil? (:value op)) this
                 (stoppable-op? op) (assoc
                                      this
                                      :active-nemeses (update active-nemeses
                                                              (strip-op-suffix op)
                                                              safe-inc))
                 (stop-op? op) (assoc
                                 this
                                 :active-nemeses (dissoc-if zero-value?
                                                            (update active-nemeses (strip-op-suffix op) dec))
                                 :last-failure (:time op))
                 :else (assoc this :last-failure (:time op)))
      (case (:f op)
        :job-running? (case (:type op)
                        :info this                          ; ignore :info operations
                        :fail this                          ; ignore :fail operations
                        :invoke this                        ; ignore :invoke operations
                        :ok (if (:value op)                 ; check if job is running
                              (assoc                        ; job is running
                                this
                                :healthy-count
                                (inc healthy-count))
                              (if (and                      ; job is not running
                                    (not (nemeses-active? active-nemeses))
                                    (< healthy-count healthy-threshold)
                                    (> (ju/nanos->secs (- (:time op) last-failure)) job-recovery-grace-period))
                                ; job is not running but it should be running
                                ; because grace period passed
                                (model/inconsistent "Job is not running.")
                                (conj this
                                      [:healthy-count 0]))))
        ; ignore other client operations
        this))))

(defn job-running-within-grace-period
  [job-running-healthy-threshold job-recovery-grace-period]
  (JobRunningWithinGracePeriod. {} 0 nil job-running-healthy-threshold job-recovery-grace-period))

(defn job-running-checker
  []
  (reify
    checker/Checker
    (check [_ test model history _]
      (let [final (reduce model/step (assoc model :last-failure (:time (first history))) history)
            result-map (conj {}
                             (find test :nemesis-gen)
                             (find test :deployment-mode))]
        (if (or (model/inconsistent? final) (zero? (:healthy-count final 0)))
          (into result-map {:valid? false
                            :error  (:msg final)})
          (into result-map {:valid?      true
                            :final-model final}))))))

(defn get-job-running-history
  [history]
  (->>
    history
    (remove #(= (:process %) :nemesis))
    (remove #(= (:type %) :invoke))
    (map :value)
    (map boolean)
    (remove nil?)))
