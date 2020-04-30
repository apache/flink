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

(ns jepsen.flink.kafka
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [db :as db]
             [control :as c]
             [util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.flink.zookeeper :as fzk]
            [jepsen.flink.utils :as fu]))

(def install-dir "/opt/kafka")
(def application-log-dir "/opt/kafka/logs")
(def log-dirs "/opt/kafka/kafka-logs")
(def server-properties (str install-dir "/config/server.properties"))
(def start-script (str install-dir "/bin/kafka-server-start.sh"))
(def topic-script (str install-dir "/bin/kafka-topics.sh"))
(def stop-script (str install-dir "/bin/kafka-server-stop.sh"))

(defn- broker-id
  [nodes node]
  (.indexOf (sort nodes) node))

(defn- override-property
  [name value]
  (str "--override " name "=" value))

(defn- start-server-command
  [{:keys [nodes] :as test} node]
  (fu/join-space
    start-script
    "-daemon"
    server-properties
    (override-property "zookeeper.connect" (fzk/zookeeper-quorum test))
    (override-property "broker.id" (broker-id nodes node))
    (override-property "log.dirs" log-dirs)
    (override-property "retention.ms" "1800000")))

(defn- start-server!
  [test node]
  (c/exec (c/lit (start-server-command test node))))

(defn- stop-server!
  []
  (info "Stopping Kafka")
  (cu/grepkill! "kafka"))

(defn- create-topic-command
  [{:keys [nodes] :as test}]
  (fu/join-space
    topic-script
    "--create"
    "--topic kafka-test-topic"
    (str "--partitions " (count nodes))
    "--replication-factor 1"
    "--zookeeper"
    (fzk/zookeeper-quorum test)))

(defn- create-topic!
  [test]
  (info "Attempting to create Kafka topic")
  (fu/retry (fn [] (c/exec (c/lit (create-topic-command test))))))

(defn- delete-kafka!
  []
  (info "Deleting Kafka distribution and logs")
  (c/exec :rm :-rf install-dir))

(defn db
  [kafka-dist-url]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (cu/install-archive! kafka-dist-url install-dir)
        (start-server! test node)
        (when (zero? (broker-id (:nodes test) node))
          (create-topic! test))))
    (teardown! [_ _ _]
      (c/su
        (stop-server!)
        (delete-kafka!)))
    db/LogFiles
    (log-files [_ _ _]
      (fu/find-files! application-log-dir))))
