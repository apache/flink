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
             [db :as db]
             [util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.flink.utils :as fu]))

(def install-dir "/opt/hadoop")
(def hadoop-conf-dir (str install-dir "/etc/hadoop"))
(def log-dir (str install-dir "/logs"))
(def yarn-log-dir (str log-dir "/yarn"))

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
  {:yarn.log-aggregation-enable          "true"

   :yarn.nodemanager.log-dirs            yarn-log-dir
   :yarn.nodemanager.resource.cpu-vcores "8"
   :yarn.nodemanager.vmem-check-enabled  "false"

   :yarn.resourcemanager.am.max-attempts "99999"
   :yarn.resourcemanager.hostname        (resource-manager (:nodes test))})

(defn core-site-config
  [test]
  {:hadoop.tmp.dir (str install-dir "/tmp")
   :fs.defaultFS   (str "hdfs://" (name-node (:nodes test)) ":9000")})

(defn hdfs-site-config
  [_]
  {:dfs.replication "1"})

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
    (c/exec :echo config-xml :> config-file)))

(defn- write-hadoop-env!
  "Configures additional environment variables in hadoop-env.sh"
  []
  (let [env-vars ["export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"
                  "export HADOOP_NAMENODE_OPTS=\"-Xms2G -Xmx2G $HADOOP_NAMENODE_OPTS\""
                  "export HADOOP_DATANODE_OPTS=\"-Xms2G -Xmx2G $HADOOP_DATANODE_OPTS\""]]
    (doseq [env-var env-vars]
      (c/exec :echo env-var :>> (str install-dir "/etc/hadoop/hadoop-env.sh")))))

(defn- write-configuration!
  [test]
  (write-config! (str install-dir "/etc/hadoop/yarn-site.xml") (yarn-site-config test))
  (write-config! (str install-dir "/etc/hadoop/core-site.xml") (core-site-config test))
  (write-config! (str install-dir "/etc/hadoop/hdfs-site.xml") (hdfs-site-config test))
  (write-hadoop-env!))

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

(defn db
  [url]
  (reify db/DB
    (setup! [_ test node]
      (info "Install Hadoop from" url)
      (c/su
        (cu/install-archive! url install-dir)
        (write-configuration! test)
        (start-name-node-formatted! test node)
        (start-data-node! test node)
        (start-resource-manager! test node)
        (start-node-manager! test node)))

    (teardown! [_ _ _]
      (info "Teardown Hadoop")
      (c/su
        (cu/grepkill! "hadoop")
        (c/exec :rm :-rf install-dir)))

    db/LogFiles
    (log-files [_ _ _]
      (c/su
        (meh (c/exec :chmod :-R :755 log-dir))
        (fu/find-files! log-dir)))))
