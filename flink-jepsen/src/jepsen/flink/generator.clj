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

(ns jepsen.flink.generator
  (:require [jepsen.util :as util]
            [jepsen.generator :as gen]
            [jepsen.flink.checker :as flink-checker]))

(gen/defgenerator TimeLimitGen
                  [dt source deadline-atom]
                  [dt (when-let [deadline @deadline-atom]
                        (util/nanos->secs deadline)) source]
                  (gen/op [_ test process]
                          (compare-and-set! deadline-atom nil (+ (util/linear-time-nanos)
                                                                 (util/secs->nanos dt)))
                          (when (<= (util/linear-time-nanos) @deadline-atom)
                            (gen/op source test process))))

;; In Jepsen 0.1.9 jepsen.generator/time-limit was re-written to interrupt Threads.
;; Unfortunately the logic has race conditions which can cause spurious failures
;; (https://github.com/jepsen-io/jepsen/issues/268).
;;
;; In our tests we do not need interrupts. Therefore, we use a time-limit implementation that is
;; similar to the one shipped with Jepsen 0.1.8.
(defn time-limit
  [dt source]
  (TimeLimitGen. dt source (atom nil)))

(defn stoppable-generator
  "Given an atom and a source generator, returns a generator that stops emitting operations from
  the source if the atom is set to true."
  [stop source]
  (reify gen/Generator
    (op [_ test process]
      (if @stop
        nil
        (gen/op source test process)))))

(defn- take-last-with-default
  [n default coll]
  (->>
    (cycle [default])
    (concat (reverse coll))
    (take n)
    (reverse)))

(defn- inc-by-factor
  [n factor]
  (assert (>= factor 1))
  (int (* n factor)))

(defn stop-generator
  "Returns a generator that emits operations from a given source generator. If the source is
  exhausted and either job-recovery-grace-period has passed or the job has been running
  job-running-healthy-threshold times consecutively, the stop atom is set to true."
  [stop source job-running-healthy-threshold job-recovery-grace-period]
  (gen/concat source
              (let [t (atom nil)]
                (reify gen/Generator
                  (op [_ test process]
                    (when (nil? @t)
                      (compare-and-set! t nil (util/relative-time-nanos)))
                    (let [history (->>
                                    (:active-histories test)
                                    deref
                                    first
                                    deref)
                          job-running-history (->>
                                                history
                                                (filter (fn [op] (>= (- (:time op) @t) 0)))
                                                (flink-checker/all-jobs-running?-history)
                                                (take-last-with-default job-running-healthy-threshold false))]
                      (if (or
                            (every? true? job-running-history)
                            (> (util/relative-time-nanos) (+ @t
                                                             (util/secs->nanos
                                                               (inc-by-factor
                                                                 job-recovery-grace-period
                                                                 1.1)))))
                        (do
                          (reset! stop true)
                          nil)
                        (do
                          (Thread/sleep 1000)
                          (recur test process)))))))))
