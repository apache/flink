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

(ns jepsen.flink.nemesis
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]
             [util :as ju]]
            [jepsen.control.util :as cu]
            [jepsen.flink.client :refer :all]
            [jepsen.flink.generator :as fgen]
            [jepsen.flink.hadoop :as fh]
            [jepsen.flink.zookeeper :refer :all]
            [slingshot.slingshot :refer [try+]]))

(def job-submit-grace-period
  "Period after job submission in which job managers must not fail."
  60)

(defn- grepkill!
  [pattern]
  (try+
    (cu/grepkill! pattern)
    ;; HACK:
    ;; On Debian Stretch, Jepsen's grepkill! throws an exception if the pattern does not match any
    ;; processes. We are swallowing the exception here because the process we are attempting to kill
    ;; might not be (re-)started yet.
    ;; For details, see https://github.com/jepsen-io/jepsen/issues/366
    (catch [:type :jepsen.control/nonzero-exit :exit 123] _)))

(defn kill-processes
  ([pattern] (kill-processes rand-nth pattern))
  ([targeter pattern]
   (reify nemesis/Nemesis
     (setup! [this test] this)
     (invoke! [this test op]
       (let [nodes (-> test :nodes targeter ju/coll)]
         (c/on-many nodes (c/su (grepkill! pattern)))
         (assoc op :value nodes)))
     (teardown! [this test]))))

(defn- non-empty-random-sample
  [coll]
  (let [sample (random-sample 0.5 coll)]
    (if (empty? sample)
      (first (shuffle coll))
      sample)))

(defn kill-taskmanager
  ([] (kill-taskmanager identity))
  ([targeter]
   (kill-processes targeter "TaskExecutorRunner")))

(defn kill-jobmanager
  []
  (kill-processes identity "ClusterEntrypoint"))

(defn start-stop-name-node
  "Nemesis stopping and starting the HDFS NameNode."
  []
  (nemesis/node-start-stopper
    fh/name-node
    (fn [test node] (c/su (fh/stop-name-node!)))
    (fn [test node] (c/su (fh/start-name-node! test node)))))

;;; Generators

(defn kill-taskmanagers-gen
  [time-limit dt op]
  (fgen/time-limit time-limit (gen/stagger dt (gen/seq (cycle [{:type :info, :f op}])))))

(defn kill-taskmanagers-bursts-gen
  [time-limit]
  (fgen/time-limit time-limit
                   (gen/seq (cycle (concat (repeat 20 {:type :info, :f :kill-task-managers})
                                           [(gen/sleep 300)])))))

(defn kill-jobmanagers-gen
  [time-limit]
  (fgen/time-limit (+ time-limit job-submit-grace-period)
                   (gen/seq (cons (gen/sleep job-submit-grace-period)
                                  (cycle [{:type :info, :f :kill-job-manager}])))))

(defn fail-name-node-during-recovery
  []
  (gen/seq [(gen/sleep job-submit-grace-period)
            {:type :info, :f :partition-start}
            {:type :info, :f :fail-name-node-start}
            (gen/sleep 20)
            {:type :info, :f :partition-stop}
            (gen/sleep 60)
            {:type :info, :f :fail-name-node-stop}]))

(def nemesis-generator-factories
  {:kill-task-managers             (fn [opts] (kill-taskmanagers-gen (:time-limit opts) 3 :kill-task-managers))
   :kill-single-task-manager       (fn [opts] (kill-taskmanagers-gen (:time-limit opts) 3 :kill-single-task-manager))
   :kill-random-task-managers      (fn [opts] (kill-taskmanagers-gen (:time-limit opts) 3 :kill-random-task-managers))
   :kill-task-managers-bursts      (fn [opts] (kill-taskmanagers-bursts-gen (:time-limit opts)))
   :kill-job-managers              (fn [opts] (kill-jobmanagers-gen (:time-limit opts)))
   :fail-name-node-during-recovery (fn [_] (fail-name-node-during-recovery))
   :utopia                         (fn [_] (gen/sleep 60))})

(defn nemesis
  []
  (nemesis/compose
    {{:partition-start :start
      :partition-stop  :stop}            (nemesis/partition-random-halves)
     {:fail-name-node-start :start
      :fail-name-node-stop  :stop}       (start-stop-name-node)
     {:kill-task-managers :start}        (kill-taskmanager)
     {:kill-single-task-manager :start}  (kill-taskmanager (fn [coll] (rand-nth coll)))
     {:kill-random-task-managers :start} (kill-taskmanager non-empty-random-sample)
     {:kill-job-manager :start}          (kill-jobmanager)}))
