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

(ns jepsen.flink.mesos
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [db :as db]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.flink.utils :refer [create-supervised-service! stop-supervised-service!]]
            [jepsen.flink.zookeeper :refer [zookeeper-uri]]))

;;; Mesos

(def master-count 1)
(def master-dir "/var/lib/mesos/master")
(def slave-dir "/var/lib/mesos/slave")
(def log-dir "/var/log/mesos")
(def master-bin "/usr/sbin/mesos-master")
(def slave-bin "/usr/sbin/mesos-slave")
(def zk-namespace :mesos)

;;; Marathon

(def marathon-bin "/usr/bin/marathon")
(def zk-marathon-namespace "marathon")
(def marathon-rest-port 8080)

;;; Mesos functions

(defn mesos-master-cmd
  "Returns the command to run the mesos master."
  [test node]
  (clojure.string/join " "
                       ["env GLOG_v=1"
                        master-bin
                        (str "--hostname=" (name node))
                        (str "--log_dir=" log-dir)
                        (str "--offer_timeout=30secs")
                        (str "--quorum=" (util/majority master-count))
                        (str "--registry_fetch_timeout=120secs")
                        (str "--registry_store_timeout=5secs")
                        (str "--work_dir=" master-dir)
                        (str "--zk=" (zookeeper-uri test zk-namespace))]))

(defn mesos-slave-cmd
  "Returns the command to run the mesos agent."
  [test node]
  (clojure.string/join " "
                       ["env GLOG_v=1"
                        slave-bin
                        (str "--hostname=" (name node))
                        (str "--log_dir=" log-dir)
                        (str "--master=" (zookeeper-uri test zk-namespace))
                        (str "--recovery_timeout=30secs")
                        (str "--work_dir=" slave-dir)
                        (str "--resources='cpus:8'")]))

(defn create-mesos-master-supervised-service!
  [test node]
  (create-supervised-service! "mesos-master"
                              (mesos-master-cmd test node)))

(defn create-mesos-slave-supervised-service!
  [test node]
  (create-supervised-service! "mesos-slave"
                              (mesos-slave-cmd test node)))

(defn master-node?
  "Returns a truthy value if the node should run the mesos master."
  [test node]
  (some #{node} (take master-count (sort (:nodes test)))))

(defn start-master!
  [test node]
  (when (master-node? test node)
    (info node "Starting mesos master")
    (c/su
      (create-mesos-master-supervised-service! test node))))

(defn start-slave!
  [test node]
  (when-not (master-node? test node)
    (info node "Starting mesos slave")
    (c/su
      (create-mesos-slave-supervised-service! test node))))

(defn stop-master!
  [test node]
  (when (master-node? test node)
    (info node "Stopping mesos master")
    (stop-supervised-service! "mesos-master")
    (meh (c/exec :rm :-rf
                 (c/lit (str log-dir "/*"))
                 (c/lit (str master-dir "/*"))))))

(defn stop-slave!
  [test node]
  (when-not (master-node? test node)
    (info node "Stopping mesos slave")
    (stop-supervised-service! "mesos-slave")
    (meh (c/exec :rm :-rf
                 (c/lit (str log-dir "/*"))
                 (c/lit (str slave-dir "/*"))))))

;;; Marathon functions

(defn install!
  [test node mesos-version marathon-version]
  (c/su
    (debian/add-repo! :mesosphere
                      "deb http://repos.mesosphere.com/debian jessie main"
                      "keyserver.ubuntu.com"
                      "E56151BF")
    (debian/install {:mesos    mesos-version
                     :marathon marathon-version})
    (c/exec :mkdir :-p "/var/run/mesos")
    (c/exec :mkdir :-p master-dir)
    (c/exec :mkdir :-p slave-dir)))

(defn marathon-cmd
  "Returns the command to run the marathon."
  [test node]
  (clojure.string/join " "
                       [marathon-bin
                        (str "--hostname " node)
                        (str "--master " (zookeeper-uri test zk-namespace))
                        (str "--zk " (zookeeper-uri test zk-marathon-namespace))
                        (str ">> " log-dir "/marathon.out")]))

(defn create-marathon-supervised-service!
  [test node]
  (create-supervised-service! "marathon"
                              (marathon-cmd test node)))

(defn marathon-node?
  [test node]
  (= node (first (sort (:nodes test)))))

(defn start-marathon!
  [test node]
  (when (marathon-node? test node)
    (info "Start marathon")
    (c/su
      (create-marathon-supervised-service! test node))))

(defn stop-marathon!
  [test node]
  (when (marathon-node? test node)
    (stop-supervised-service! "marathon")))

(defn marathon-base-url
  [test]
  (str "http://" (first (sort (:nodes test))) ":" marathon-rest-port))

(defn db
  [mesos-version marathon-version]
  (reify db/DB
    (setup! [this test node]
      (install! test node mesos-version marathon-version)
      (start-master! test node)
      (start-slave! test node)
      (start-marathon! test node))
    (teardown! [this test node]
      (stop-slave! test node)
      (stop-master! test node)
      (stop-marathon! test node))
    db/LogFiles
    (log-files [_ test node]
      (if (cu/exists? log-dir) (cu/ls-full log-dir) []))))
