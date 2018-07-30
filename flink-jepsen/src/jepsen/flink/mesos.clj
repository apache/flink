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
            [jepsen.flink.zookeeper :refer [zookeeper-uri]]))

;;; Mesos

(def master-count 1)
(def master-pidfile "/var/run/mesos/master.pid")
(def slave-pidfile "/var/run/mesos/slave.pid")
(def master-dir "/var/lib/mesos/master")
(def slave-dir "/var/lib/mesos/slave")
(def log-dir "/var/log/mesos")
(def master-bin "/usr/sbin/mesos-master")
(def slave-bin "/usr/sbin/mesos-slave")
(def zk-namespace :mesos)

;;; Marathon

(def marathon-bin "/usr/bin/marathon")
(def zk-marathon-namespace "marathon")
(def marathon-pidfile "/var/run/mesos/marathon.pid")
(def marathon-rest-port 8080)

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

;;; Mesos functions

(defn start-master!
  [test node]
  (when (some #{node} (take master-count (sort (:nodes test))))
    (info node "Starting mesos master")
    (c/su
      (c/exec :start-stop-daemon
              :--background
              :--chdir master-dir
              :--exec "/usr/bin/env"
              :--make-pidfile
              :--no-close
              :--oknodo
              :--pidfile master-pidfile
              :--start
              :--
              "GLOG_v=1"
              master-bin
              (str "--hostname=" (name node))
              (str "--log_dir=" log-dir)
              (str "--offer_timeout=30secs")
              (str "--quorum=" (util/majority master-count))
              (str "--registry_fetch_timeout=120secs")
              (str "--registry_store_timeout=5secs")
              (str "--work_dir=" master-dir)
              (str "--zk=" (zookeeper-uri test zk-namespace))
              :>> (str log-dir "/master.stdout")
              (c/lit "2>&1")))))

(defn start-slave!
  [test node]
  (when-not (some #{node} (take master-count (sort (:nodes test))))
    (info node "Starting mesos slave")
    (c/su
      (c/exec :start-stop-daemon :--start
              :--background
              :--chdir slave-dir
              :--exec slave-bin
              :--make-pidfile
              :--no-close
              :--pidfile slave-pidfile
              :--oknodo
              :--
              (str "--hostname=" (name node))
              (str "--log_dir=" log-dir)
              (str "--master=" (zookeeper-uri test zk-namespace))
              (str "--recovery_timeout=30secs")
              (str "--work_dir=" slave-dir)
              :>> (str log-dir "/slave.stdout")
              (c/lit "2>&1")))))

(defn stop-master!
  [node]
  (info node "Stopping mesos master")
  (meh (cu/grepkill! :mesos-master))
  (meh (c/exec :rm :-rf master-pidfile))
  (meh (c/exec :rm :-rf
               (c/lit (str log-dir "/*"))
               (c/lit (str master-dir "/*")))))

(defn stop-slave!
  [node]
  (info node "Stopping mesos slave")
  (meh (cu/grepkill! :mesos-slave))
  (meh (c/exec :rm :-rf slave-pidfile))
  (meh (c/exec :rm :-rf
               (c/lit (str log-dir "/*"))
               (c/lit (str slave-dir "/*")))))

;;; Marathon functions

(defn start-marathon!
  [test node]
  (when (= node (first (sort (:nodes test))))
    (info "Start marathon")
    (c/su
      (c/exec :start-stop-daemon :--start
              :--background
              :--exec marathon-bin
              :--make-pidfile
              :--no-close
              :--pidfile marathon-pidfile
              :--
              (c/lit (str "--hostname " node))
              (c/lit (str "--master " (zookeeper-uri test zk-namespace)))
              (c/lit (str "--zk " (zookeeper-uri test zk-marathon-namespace)))
              :>> (str log-dir "/marathon.stdout")
              (c/lit "2>&1")))))

(defn stop-marathon!
  []
  (cu/grepkill! "marathon"))

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
      (stop-slave! node)
      (stop-master! node)
      (stop-marathon!))
    db/LogFiles
    (log-files [_ test node]
      (if (cu/exists? log-dir) (cu/ls-full log-dir) []))))
