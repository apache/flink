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

(ns jepsen.flink.db
  (:require [clj-http.client :as http]
            [clojure.java.io]
            [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [db :as db]
             [util :refer [meh]]
             [zookeeper :as zk]]
            [jepsen.control.util :as cu]
            [jepsen.flink.hadoop :as hadoop]
            [jepsen.flink.mesos :as mesos]
            [jepsen.flink.utils :as fu]
            [jepsen.flink.zookeeper :refer :all]))

(def install-dir "/opt/flink")
(def upload-dir "/tmp")
(def log-dir (str install-dir "/log"))
(def conf-file (str install-dir "/conf/flink-conf.yaml"))
(def masters-file (str install-dir "/conf/masters"))

(def default-flink-dist-url "https://archive.apache.org/dist/flink/flink-1.6.0/flink-1.6.0-bin-hadoop28-scala_2.11.tgz")
(def hadoop-dist-url "https://archive.apache.org/dist/hadoop/common/hadoop-2.8.3/hadoop-2.8.3.tar.gz")
(def deb-zookeeper-package "3.4.9-3+deb8u1")
(def deb-mesos-package "1.5.0-2.0.2")
(def deb-marathon-package "1.6.322")

(def taskmanager-slots 3)

(defn flink-configuration
  [test node]
  {:high-availability                  "zookeeper"
   :high-availability.zookeeper.quorum (zookeeper-quorum test)
   :high-availability.storageDir       (str (:ha-storage-dir test) "/ha")
   :jobmanager.rpc.address             node
   :state.savepoints.dir               (str (:ha-storage-dir test) "/savepoints")
   :rest.address                       node
   :rest.port                          8081
   :rest.bind-address                  "0.0.0.0"
   :taskmanager.numberOfTaskSlots      taskmanager-slots
   :yarn.application-attempts          99999
   :slotmanager.taskmanager-timeout    10000
   :state.backend.local-recovery       "true"
   :taskmanager.registration.timeout   "30 s"})

(defn write-configuration!
  "Writes the flink-conf.yaml to the flink conf directory"
  [test node]
  (let [c (clojure.string/join "\n" (map (fn [[k v]] (str (name k) ": " v))
                                         (seq (flink-configuration test node))))]
    (c/exec :echo c :> conf-file)
    ;; TODO: write log4j.properties properly
    (c/exec (c/lit (str "sed -i'.bak' -e '/log4j.rootLogger=/ s/=.*/=DEBUG, file/' " install-dir "/conf/log4j.properties")))))

(defn install-flink!
  [test node]
  (let [url (:tarball test)]
    (info "Installing Flink from" url)
    (cu/install-archive! url install-dir)
    (info "Enable S3 FS")
    (c/exec (c/lit (str "ls " install-dir "/opt/flink-s3-fs-hadoop* | xargs -I {} mv {} " install-dir "/lib")))
    (c/upload (:job-jar test) upload-dir)
    (c/exec :mv (str upload-dir "/" (.getName (clojure.java.io/file (:job-jar test)))) install-dir)
    (write-configuration! test node)))

(defn teardown-flink!
  []
  (info "Tearing down Flink")
  (meh (cu/grepkill! "flink"))
  (meh (c/exec :rm :-rf install-dir))
  (meh (c/exec :rm :-rf (c/lit "/tmp/.yarn-properties*"))))

(defn get-log-files!
  []
  (if (cu/exists? log-dir) (cu/ls-full log-dir) []))

(defn flink-db
  []
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (install-flink! test node)))

    (teardown! [_ test node]
      (c/su
        (teardown-flink!)))

    db/LogFiles
    (log-files [_ test node]
      (concat
        (get-log-files!)))))

(defn combined-db
  [dbs]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (doall (map #(db/setup! % test node) dbs))))
    (teardown! [_ test node]
      (c/su
        (try
          (doall (map #(db/teardown! % test node) dbs))
          (finally (fu/stop-all-supervised-services!)))))
    db/LogFiles
    (log-files [_ test node]
      (->>
        (filter (partial satisfies? db/LogFiles) dbs)
        (map #(db/log-files % test node))
        (flatten)))))

(defn- sorted-nodes
  [test]
  (-> test :nodes sort))

(defn- select-nodes
  [test selector]
  (-> (sorted-nodes test)
      selector))

(defn- first-node
  [test]
  (select-nodes test first))

(defn- create-env-vars
  "Expects a map containing environment variables, and returns a string that can be used to set
  environment variables for a child process using Bash's quick assignment and inheritance trick.
  For example, for a map {:FOO \"bar\"}, this function returns \"FOO=bar \"."
  [m]
  (->>
    (map #(str (name (first %)) "=" (second %)) m)
    (clojure.string/join " ")
    (#(str % " "))))

(defn- hadoop-env-vars
  []
  (create-env-vars {:HADOOP_CLASSPATH (str "`" hadoop/install-dir "/bin/hadoop classpath`")
                    :HADOOP_CONF_DIR  hadoop/hadoop-conf-dir}))

(defn exec-flink!
  [cmd args]
  (c/su
    (c/exec (c/lit (str
                     (hadoop-env-vars)
                     install-dir "/bin/flink " cmd " " args)))))

(defn flink-run-cli-args
  "Returns the CLI args that should be passed to 'flink run'"
  [test]
  (concat
    ["-d"]
    (if (:main-class test)
      [(str "-c " (:main-class test))]
      [])))

(defn submit-job!
  ([test] (submit-job! test []))
  ([test cli-args]
   (exec-flink! "run" (clojure.string/join
                        " "
                        (concat cli-args
                                (flink-run-cli-args test)
                                [(str install-dir "/" (last (str/split (:job-jar test) #"/")))
                                 (:job-args test)])))))

;;; Standalone

(def standalone-master-count 2)

(defn- standalone-master-nodes
  [test]
  (select-nodes test (partial take standalone-master-count)))

(defn- standalone-taskmanager-nodes
  [test]
  (select-nodes test (partial drop standalone-master-count)))

(defn- start-standalone-masters!
  [test node]
  (when (some #{node} (standalone-master-nodes test))
    (fu/create-supervised-service!
      "flink-master"
      (str "env " (hadoop-env-vars)
           install-dir "/bin/jobmanager.sh start-foreground "
           ">> " log-dir "/jobmanager.log"))))

(defn- start-standalone-taskmanagers!
  [test node]
  (when (some #{node} (standalone-taskmanager-nodes test))
    (fu/create-supervised-service!
      "flink-taskmanager"
      (str "env " (hadoop-env-vars)
           install-dir "/bin/taskmanager.sh start-foreground "
           ">> " log-dir "/taskmanager.log"))))

(defn- start-flink-db
  []
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (start-standalone-masters! test node)
        (start-standalone-taskmanagers! test node)))

    (teardown! [_ test node]
      (c/su
        (when (some #{node} (standalone-master-nodes test))
          (fu/stop-supervised-service! "flink-master"))
        (when (some #{node} (standalone-taskmanager-nodes test))
          (fu/stop-supervised-service! "flink-taskmanager"))))))

(defn flink-standalone-db
  []
  (let [zk (zk/db deb-zookeeper-package)
        hadoop (hadoop/db hadoop-dist-url)
        flink (flink-db)
        start-flink (start-flink-db)]
    (combined-db [hadoop zk flink start-flink])))

(defn submit-job-from-first-node!
  [test]
  (c/on (first-node test)
        (submit-job! test)))

;;; YARN

(defn flink-yarn-db
  []
  (let [zk (zk/db deb-zookeeper-package)
        hadoop (hadoop/db hadoop-dist-url)
        flink (flink-db)]
    (combined-db [hadoop zk flink])))

(defn start-yarn-session!
  [test]
  (let [node (first-node test)]
    (c/on node
          (info "Starting YARN session from" node)
          (c/su
            (c/exec (c/lit (str (hadoop-env-vars)
                                " " install-dir "/bin/yarn-session.sh -d -jm 2048m -tm 2048m")))
            (submit-job! test)))))

(defn start-yarn-job!
  [test]
  (c/on (first-node test)
        (c/su
          (submit-job! test ["-m yarn-cluster" "-yjm 2048m" "-ytm 2048m"]))))

;;; Mesos

(defn flink-mesos-db
  []
  (let [zk (zk/db deb-zookeeper-package)
        hadoop (hadoop/db hadoop-dist-url)
        mesos (mesos/db deb-mesos-package deb-marathon-package)
        flink (flink-db)]
    (combined-db [hadoop zk mesos flink])))

(defn submit-job-with-retry!
  [test]
  (fu/retry
    (partial submit-job! test)
    :fallback (fn [e] (do
                        (fatal e "Could not submit job.")
                        (System/exit 1)))))

(defn mesos-appmaster-cmd
  "Returns the command used by Marathon to start Flink's Mesos application master."
  [test]
  (str (hadoop-env-vars)
       install-dir "/bin/mesos-appmaster.sh "
       "-Dmesos.master=" (zookeeper-uri
                           test
                           mesos/zk-namespace) " "
       "-Djobmanager.rpc.address=$(hostname -f) "
       "-Djobmanager.heap.mb=2048 "
       "-Djobmanager.rpc.port=6123 "
       "-Dmesos.resourcemanager.tasks.mem=2048 "
       "-Dtaskmanager.heap.mb=2048 "
       "-Dmesos.resourcemanager.tasks.cpus=1 "
       "-Drest.bind-address=$(hostname -f) "))

(defn start-mesos-session!
  [test]
  (c/su
    (let [r (fu/retry (fn []
                        (http/post
                          (str (mesos/marathon-base-url test) "/v2/apps")
                          {:form-params  {:id                    "flink"
                                          :cmd                   (mesos-appmaster-cmd test)
                                          :cpus                  1.0
                                          :mem                   2048
                                          :maxLaunchDelaySeconds 3}
                           :content-type :json})))]
      (info "Submitted Flink Application via Marathon" r)
      (c/on (-> test :nodes sort first)
            (submit-job-with-retry! test)))))
