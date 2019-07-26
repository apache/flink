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

(defn- set-job-not-running
  [model] (assoc model :healthy-count 0))

(defn- track-job-running
  [model]
  (update model :healthy-count inc))

(defn- elapsed-seconds
  [start end]
  (ju/nanos->secs (- end start)))

(defn- should-cluster-be-healthy?
  [model op]
  (let [{:keys [active-nemeses last-failure job-recovery-grace-period]} model]
    (and
      (not (nemeses-active? active-nemeses))
      (> (elapsed-seconds last-failure (:time op)) job-recovery-grace-period))))

(defn- start-fault
  [model op]
  (let [{:keys [active-nemeses]} model]
    (assoc
      model
      :active-nemeses (update active-nemeses
                              (strip-op-suffix op)
                              safe-inc))))

(defn- stop-fault
  [model op]
  (let [{:keys [active-nemeses]} model]
    (assoc
      model
      :active-nemeses (dissoc-if zero-value?
                                 (update active-nemeses (strip-op-suffix op) dec))
      :last-failure (:time op))))

(defn- job-allowed-to-be-running?
  [model op]
  (let [{:keys [job-canceled? job-canceled-time job-cancellation-grace-period]} model
        now (:time op)]
    (cond
      (not job-canceled?) true
      :else (> job-cancellation-grace-period (elapsed-seconds job-canceled-time now)))))

(defn- handle-job-running?-op
  "Returns the new model for an op {:f :job-running? ...}."
  [model op]
  (assert (#{:ok :fail :info} (:type op)) "Unexpected type")
  (let [{:keys [job-canceled?]} model
        job-running (:value op)
        request-failed (#{:info :fail} (:type op))]
    (if (and request-failed
             (should-cluster-be-healthy? model op))
      (model/inconsistent "Cluster is not running.")
      (if job-running                                       ; cluster is running, check if job is running
        (if (job-allowed-to-be-running? model op)           ; job is running but is it supposed to be running?
          (track-job-running model)
          (model/inconsistent
            "Job is running after cancellation."))
        (if (and                                            ; job is not running
              (should-cluster-be-healthy? model op)
              (not job-canceled?))
          (model/inconsistent "Job is not running.")        ; job is not running but it should be running because grace period passed
          (set-job-not-running model))))))

(defrecord
  JobRunningWithinGracePeriod
  ^{:doc "A Model which is consistent if the Flink job and the Flink cluster became available within
  `job-recovery-grace-period` seconds after the last fault injected by the nemesis.
  Note that some faults happen at a single point in time (e.g., killing of processes). Other faults,
  such as network splits, happen during a period of time, and can thus be interleaving. As long as
  there are active faults, the job and the cluster are allowed to be unavailable.

  Note that this model assumes that the client dispatches the operations reliably, i.e., in case of
  exceptions, the operations are retried or failed fatally."}
  [active-nemeses                                           ; stores active failures
   healthy-count                                            ; how many consecutive times was the job running?
   last-failure                                             ; timestamp when the last failure was injected/ended
   healthy-threshold                                        ; after how many times is the job considered healthy
   job-recovery-grace-period                                ; after how many seconds should the job be recovered
   job-cancellation-grace-period                            ; after how many seconds should the job be canceled?
   job-canceled?                                            ; is the job canceled?
   job-canceled-time]                                       ; timestamp of cancellation
  Model
  (step [this op]
    (case (:process op)
      :nemesis (cond
                 (nil? (:value op)) this
                 (stoppable-op? op) (start-fault this op)
                 (stop-op? op) (stop-fault this op)
                 :else (assoc this :last-failure (:time op)))
      (if (= :invoke (:type op))
        this                                                ; ignore :invoke operations
        (case (:f op)
          :job-running? (handle-job-running?-op this op)
          :cancel-job (do
                        (assert (= :ok (:type op)) ":cancel-job must not fail")
                        (assoc this :job-canceled? true :job-canceled-time (:time op)))
          ; ignore other client operations
          this)))))

(defn- job-running-within-grace-period
  ([job-running-healthy-threshold job-recovery-grace-period job-cancellation-grace-period]
   (JobRunningWithinGracePeriod. {} 0 nil job-running-healthy-threshold job-recovery-grace-period job-cancellation-grace-period false nil)))

(defn- history->jobs-running?-value
  [history]
  (->>
    history
    (filter #(= (:f %) :jobs-running?))
    (remove #(= (:type %) :invoke))
    (map :value)))

(defn- history->job-ids
  "Extracts all job ids from a history."
  [history]
  (set (->> history
            (history->jobs-running?-value)
            (map keys)
            (flatten)
            (remove nil?))))

(defn all-jobs-running?-history
  [history]
  (->>
    history
    (history->jobs-running?-value)
    (map vals)
    (map #(and
            (not (empty? %))
            (every? true? %)))))

(defn- healthy?
  [model]
  (or (>= (:healthy-count model) (:healthy-threshold model))
      (:job-canceled? model)))

(defn- jobs-running?->job-running?
  "Rewrites history entries of the form {:f :jobs-running? :value {...}}

  Example: {:type ok :f :jobs-running? :value {job-id-1 true}} -> {:type ok :f :job-running? :value true}"
  [history-entry job-id]
  (let [job-running?-entry (assoc history-entry :f :job-running?)
        job-running?-entry-ok (update job-running?-entry :value #(get % job-id))]
    (if (= (:type history-entry) :ok)
      job-running?-entry-ok
      job-running?-entry)))

(defn- history->single-job-history
  "Rewrites a history to one that appears to run a single Flink job."
  [history job-id]
  (let [transform-history-entry (fn [history-entry]
                                  (case (:f history-entry)
                                    :jobs-running? (jobs-running?->job-running? history-entry job-id)
                                    :cancel-jobs (assoc history-entry :f :cancel-job)
                                    history-entry))]
    (map transform-history-entry history)))

(defn- compute-final-model
  [model history]
  (let [start-time (-> history first :time)]
    (reduce knossos.model/step
            (assoc model :last-failure start-time)
            history)))

(defn job-running-checker
  ([job-running-healthy-threshold job-recovery-grace-period]
   (job-running-checker job-running-healthy-threshold job-recovery-grace-period 10))
  ([job-running-healthy-threshold job-recovery-grace-period job-cancellation-grace-period]
   (reify
     checker/Checker
     (check [_ test history _]
       (let [job-ids (history->job-ids history)
             individual-job-histories (map (partial history->single-job-history history) job-ids)
             model (job-running-within-grace-period job-running-healthy-threshold
                                                    job-recovery-grace-period
                                                    job-cancellation-grace-period)
             final-models (map (partial compute-final-model model) individual-job-histories)
             inconsistent-or-unhealthy (or (empty? job-ids)
                                           (some model/inconsistent? final-models)
                                           (some (complement healthy?) final-models))
             result-map (select-keys test [:nemesis-gen :deployment-mode])]
         (if inconsistent-or-unhealthy
           (into result-map {:valid?       false
                             :final-models final-models})
           (into result-map {:valid?       true
                             :final-models final-models})))))))
