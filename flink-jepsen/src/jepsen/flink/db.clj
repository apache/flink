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
            [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [db :as db]
             [util :refer [meh]]]
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

(def taskmanager-slots 3)

(defn- default-flink-configuration
  [test node]
  {:high-availability                  "zookeeper"
   :high-availability.zookeeper.quorum (zookeeper-quorum test)
   :high-availability.storageDir       "hdfs:///flink/ha"
   :jobmanager.memory.process.size     "2048m"
   :jobmanager.rpc.address             node
   :state.savepoints.dir               "hdfs:///flink/savepoints"
   :rest.address                       node
   :rest.port                          8081
   :rest.bind-address                  "0.0.0.0"
   :taskmanager.numberOfTaskSlots      taskmanager-slots
   :yarn.application-attempts          99999
   :slotmanager.taskmanager-timeout    10000
   :state.backend.local-recovery       "true"
   :taskmanager.memory.process.size    "2048m"
   :taskmanager.registration.timeout   "30 s"})

(defn flink-configuration
  [test node]
  (let [additional-config (-> test :test-spec :flink-config)]
    (merge (default-flink-configuration test node)
           additional-config)))

(defn write-configuration!
  "Writes the flink-conf.yaml to the flink conf directory"
  [test node]
  (let [c (clojure.string/join "\n" (map (fn [[k v]] (str (name k) ": " v))
                                         (seq (flink-configuration test node))))]
    (c/exec :echo c :> conf-file)
    ;; TODO: write log4j.properties properly
    (c/exec (c/lit (str "sed -i'.bak' -e '/rootLogger\\.level/ s/=.*/= DEBUG/' " install-dir "/conf/log4j.properties")))))

(defn- file-name
  [path]
  (.getName (clojure.java.io/file path)))

(defn upload-job-jar!
  [job-jar]
  (c/upload job-jar upload-dir)
  (c/exec :mv (str upload-dir "/" (file-name job-jar)) install-dir))

(defn upload-job-jars!
  [job-jars]
  (doseq [job-jar job-jars]
    (upload-job-jar! job-jar)))

(defn install-flink!
  [test node]
  (let [url (:tarball test)]
    (info "Installing Flink from" url)
    (cu/install-archive! url install-dir)
    (info "Enable S3 FS")
    (c/exec (c/lit (str "mkdir " install-dir "/plugins/s3-fs-hadoop && ls " install-dir "/opt/flink-s3-fs-hadoop* | xargs -I {} mv {} " install-dir "/plugins/s3-fs-hadoop")))
    (upload-job-jars! (->> test :test-spec :jobs (map :job-jar)))
    (write-configuration! test node)))

(defn teardown-flink!
  []
  (info "Tearing down Flink")
  (meh (cu/grepkill! "flink"))
  (meh (c/exec :rm :-rf install-dir))
  (meh (c/exec :rm :-rf (c/lit "/tmp/.yarn-properties*"))))

(defn combined-db
  [dbs]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (doseq [db dbs] (db/setup! db test node))))
    (teardown! [_ test node]
      (c/su
        (try
          (doseq [db (reverse dbs)] (db/teardown! db test node))
          (finally (fu/stop-all-supervised-services!)))))
    db/LogFiles
    (log-files [_ test node]
      (->>
        (filter (partial satisfies? db/LogFiles) dbs)
        (map #(db/log-files % test node))
        (flatten)))))

(defn flink-db
  [db]
  (let [flink-base-db (reify db/DB
                        (setup! [_ test node]
                          (c/su
                            (install-flink! test node)))

                        (teardown! [_ _ _]
                          (c/su
                            (teardown-flink!)))

                        db/LogFiles
                        (log-files [_ _ _]
                          (c/su
                            (fu/dump-jstack-by-pattern! log-dir
                                                        "TaskExecutor"
                                                        "TaskManager"
                                                        "ClusterEntrypoint")
                            (fu/find-files! log-dir))))]
    (combined-db [flink-base-db db])))

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
    (apply fu/join-space)
    (#(str % " "))))

(defn- hadoop-env-vars
  []
  (create-env-vars {:HADOOP_CLASSPATH (str "`" hadoop/install-dir "/bin/hadoop classpath`")
                    :HADOOP_CONF_DIR  hadoop/hadoop-conf-dir}))

(defn exec-flink!
  [cmd args]
  (c/su
    (c/exec (c/lit (fu/join-space
                     (hadoop-env-vars)
                     (str install-dir "/bin/flink")
                     cmd
                     (apply fu/join-space args))))))

(defn flink-run-cli-args
  "Returns the CLI args that should be passed to 'flink run'"
  [job-spec]
  (concat
    ["-d"]
    (if (:main-class job-spec)
      [(str "-c " (:main-class job-spec))]
      [])))

(defn submit-job!
  ([test] (submit-job! test []))
  ([test cli-args]
   (doseq [{:keys [job-jar job-args] :as job-spec} (-> test :test-spec :jobs)]
     (exec-flink! "run" (concat cli-args
                                (flink-run-cli-args job-spec)
                                [(str install-dir "/" (file-name job-jar))
                                 job-args])))))

(defn- submit-job-with-retry!
  ([test] (submit-job-with-retry! test []))
  ([test cli-args] (fu/retry
                     (partial submit-job! test cli-args)
                     :fallback (fn [e] (do
                                         (fatal e "Could not submit job.")
                                         (System/exit 1))))))

;;; Standalone

(def standalone-master-count 2)

(defn- standalone-master-nodes
  [test]
  (select-nodes test (partial take standalone-master-count)))

(defn- standalone-taskmanager-nodes
  [test]
  (select-nodes test (partial drop standalone-master-count)))

(defn- start-standalone-masters!
  []
  (let [jobmanager-script (str install-dir "/bin/jobmanager.sh")
        jobmanager-log (str log-dir "/jobmanager.log")]
    (fu/create-supervised-service!
      "flink-master"
      (fu/join-space "env" (hadoop-env-vars) jobmanager-script "start-foreground" ">>" jobmanager-log))))

(defn- start-standalone-taskmanagers!
  []
  (let [taskmanager-script (str install-dir "/bin/taskmanager.sh")
        taskmanager-log (str log-dir "/taskmanager.log")]
    (fu/create-supervised-service!
      "flink-taskmanager"
      (fu/join-space "env" (hadoop-env-vars) taskmanager-script "start-foreground" ">>" taskmanager-log))))

(defn start-flink-db
  []
  (flink-db
    (reify db/DB
      (setup! [_ test node]
        (c/su
          (when (some #{node} (standalone-master-nodes test))
            (start-standalone-masters!))
          (when (some #{node} (standalone-taskmanager-nodes test))
            (start-standalone-taskmanagers!))
          (when (= (first-node test) node)
            (submit-job-with-retry! test))))

      (teardown! [_ test node]
        (c/su
          (when (some #{node} (standalone-master-nodes test))
            (fu/stop-supervised-service! "flink-master"))
          (when (some #{node} (standalone-taskmanager-nodes test))
            (fu/stop-supervised-service! "flink-taskmanager")))))))

;;; YARN

(defn- start-yarn-session-cmd
  []
  (fu/join-space (hadoop-env-vars)
                 (str install-dir "/bin/yarn-session.sh")
                 "-d"))

(defn- start-yarn-session!
  []
  (info "Starting YARN session")
  (let [exec-start-yarn-session! #(c/su (c/exec (c/lit (start-yarn-session-cmd))))
        log-failure! (fn [exception _] (info "Starting YARN session failed due to"
                                             (.getMessage exception)
                                             "Retrying..."))]
    (fu/retry exec-start-yarn-session!
              :delay 4000
              :on-error log-failure!)))

(defn yarn-session-db
  []
  (flink-db (reify db/DB
              (setup! [_ test node]
                (when (= node (first-node test))
                  (start-yarn-session!)
                  (submit-job! test)))
              (teardown! [_ _ _]))))

(defn- start-yarn-job!
  [test]
  (c/su
    (submit-job-with-retry! test ["-m yarn-cluster"])))

(defn yarn-job-db
  []
  (flink-db (reify db/DB
              (setup! [_ test node]
                (when (= node (first-node test))
                  (start-yarn-job! test)))
              (teardown! [_ _ _]))))

;;; Mesos

(defn- mesos-appmaster-cmd
  "Returns the command used by Marathon to start Flink's Mesos application master."
  [test]
  (fu/join-space
    (hadoop-env-vars)
    (str install-dir "/bin/mesos-appmaster.sh")
    (str "-Dmesos.master=" (zookeeper-uri test mesos/zk-namespace))
    "-Djobmanager.rpc.address=$(hostname -f)"
    "-Djobmanager.rpc.port=6123"
    "-Dmesos.resourcemanager.tasks.cpus=1"
    "-Dcontainerized.taskmanager.env.HADOOP_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)"
    "-Dtaskmanager.memory.process.size=2048m"
    "-Drest.bind-address=$(hostname -f)"))

(defn- start-mesos-session!
  [test]
  (c/su
    (let [log-submission-failure! (fn [exception _]
                                    (info "Submitting Flink Application via Marathon failed due to"
                                          (.getMessage exception)
                                          "Retrying..."))
          submit-flink! (fn []
                          (http/post
                            (str (mesos/marathon-base-url test) "/v2/apps")
                            {:form-params  {:id                    "flink"
                                            :cmd                   (mesos-appmaster-cmd test)
                                            :cpus                  1.0
                                            :mem                   2048
                                            :maxLaunchDelaySeconds 3}
                             :content-type :json}))
          marathon-response (fu/retry submit-flink!
                                      :on-retry log-submission-failure!
                                      :delay 4000)]
      (info "Submitted Flink Application via Marathon" marathon-response))))

(defn flink-mesos-app-master
  []
  (flink-db
    (reify
      db/DB
      (setup! [_ test node]
        (when (= (first-node test) node)
          (start-mesos-session! test)
          (submit-job-with-retry! test)))

      (teardown! [_ _ _]))))
