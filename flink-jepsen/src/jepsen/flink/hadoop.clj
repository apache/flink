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

(ns jepsen.flink.hadoop
  (:require [clojure.data.xml :as xml]
            [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [db :as db]]
            [jepsen.control.util :as cu]))

(def install-dir "/opt/hadoop")
(def hadoop-conf-dir (str install-dir "/etc/hadoop"))
(def yarn-log-dir "/tmp/logs/yarn")

(defn name-node
  [nodes]
  (first (sort nodes)))

(defn resource-manager
  [nodes]
  (second (sort nodes)))

(defn data-nodes
  [nodes]
  (drop 2 (sort nodes)))

(defn yarn-site-config
  [test]
  {:yarn.resourcemanager.hostname        (resource-manager (:nodes test))
   :yarn.log-aggregation-enable          "true"
   :yarn.nodemanager.resource.cpu-vcores "8"
   :yarn.resourcemanager.am.max-attempts "99999"
   :yarn.nodemanager.log-dirs            yarn-log-dir})

(defn core-site-config
  [test]
  {:fs.defaultFS (str "hdfs://" (name-node (:nodes test)) ":9000")})

(defn property-value
  [property value]
  (xml/element :property {}
               [(xml/element :name {} (name property))
                (xml/element :value {} value)]))

(defn write-config!
  [^String config-file config]
  (info "Writing config" config-file)
  (let [config-xml (xml/indent-str
                     (xml/element :configuration
                                  {}
                                  (map (fn [[k v]] (property-value k v)) (seq config))))]
    (c/exec :echo config-xml :> config-file)
    ))

(defn start-name-node!
  [test node]
  (when (= node (name-node (:nodes test)))
    (info "Start NameNode daemon.")
    (c/exec (str install-dir "/sbin/hadoop-daemon.sh") :--config hadoop-conf-dir :--script :hdfs :start :namenode)))

(defn start-name-node-formatted!
  [test node]
  (when (= node (name-node (:nodes test)))
    (info "Format HDFS")
    (c/exec (str install-dir "/bin/hdfs") :namenode :-format :-force :-clusterId "0000000")
    (start-name-node! test node)))

(defn stop-name-node!
  []
  (c/exec (str install-dir "/sbin/hadoop-daemon.sh") :--config hadoop-conf-dir :--script :hdfs :stop :namenode))

(defn start-data-node!
  [test node]
  (when (some #{node} (data-nodes (:nodes test)))
    (info "Start DataNode")
    (c/exec (str install-dir "/sbin/hadoop-daemon.sh") :--config hadoop-conf-dir :--script :hdfs :start :datanode)))

(defn start-resource-manager!
  [test node]
  (when (= node (resource-manager (:nodes test)))
    (info "Start ResourceManager")
    (c/exec (str install-dir "/sbin/yarn-daemon.sh") :--config hadoop-conf-dir :start :resourcemanager)))

(defn start-node-manager!
  [test node]
  (when (some #{node} (data-nodes (:nodes test)))
    (info "Start NodeManager")
    (c/exec (str install-dir "/sbin/yarn-daemon.sh") :--config hadoop-conf-dir :start :nodemanager)))

(defn find-files!
  [dir]
  (->>
    (clojure.string/split (c/exec :find dir :-type :f) #"\n")
    (remove clojure.string/blank?)))

(defn db
  [url]
  (reify db/DB
    (setup! [_ test node]
      (info "Install Hadoop from" url)
      (c/su
        (cu/install-archive! url install-dir)
        (write-config! (str install-dir "/etc/hadoop/yarn-site.xml") (yarn-site-config test))
        (write-config! (str install-dir "/etc/hadoop/core-site.xml") (core-site-config test))
        (c/exec :echo (c/lit "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64") :>> (str install-dir "/etc/hadoop/hadoop-env.sh"))
        (start-name-node-formatted! test node)
        (start-data-node! test node)
        (start-resource-manager! test node)
        (start-node-manager! test node)))

    (teardown! [_ test node]
      (info "Teardown Hadoop")
      (c/su
        (cu/grepkill! "hadoop")
        (c/exec (c/lit (str "rm -rf /tmp/hadoop-* ||:")))))

    db/LogFiles
    (log-files [_ _ _]
      (c/su
        (concat (find-files! (str install-dir "/logs"))
                (if (cu/exists? yarn-log-dir)
                  (do
                    (c/exec :chmod :-R :777 yarn-log-dir)
                    (find-files! yarn-log-dir))
                  []))))))
