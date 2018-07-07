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

(ns jepsen.flink.client
  (:require [clj-http.client :as http]
            [clojure.tools.logging :refer :all]
            [jepsen.client :as client]
            [jepsen.flink.zookeeper :as fz]
            [jepsen.flink.utils :as fu]
            [zookeeper :as zk])
  (:import (java.io ByteArrayInputStream ObjectInputStream)))

(defn connect-zk-client!
  [connection-string]
  (zk/connect connection-string :timeout-msec 60000))

(defn read-url
  [bytes]
  (with-open [object-input-stream (ObjectInputStream. (ByteArrayInputStream. bytes))]
    (.readUTF object-input-stream)))

(defn wait-for-zk-operation
  [zk-client operation path]
  (let [p (promise)]
    (letfn [(iter [_]
              (when-let [res (operation zk-client path :watcher iter)]
                (deliver p res)))
            ]
      (iter nil)
      p)))

(defn wait-for-path-to-exist
  [zk-client path]
  (info "Waiting for path" path "in ZK.")
  (wait-for-zk-operation zk-client zk/exists path))

(defn wait-for-children-to-exist
  [zk-client path]
  (wait-for-zk-operation zk-client zk/children path))

(defn find-application-id
  [zk-client]
  (do
    (->
      (wait-for-path-to-exist zk-client "/flink")
      (deref))
    (->
      (wait-for-children-to-exist zk-client "/flink")
      (deref)
      (first))))

(defn watch-node-bytes
  [zk-client path callback]
  (when (zk/exists zk-client path :watcher (fn [_] (watch-node-bytes zk-client path callback)))
    (->>
      (zk/data zk-client path :watcher (fn [_] (watch-node-bytes zk-client path callback)))
      :data
      (callback))))

(defn make-job-manager-url [test]
  (let [rest-url-atom (atom nil)
        zk-client (connect-zk-client! (fz/zookeeper-quorum test))
        init-future (future
                      (let [application-id (find-application-id zk-client)
                            path (str "/flink/" application-id "/leader/rest_server_lock")
                            _ (->
                                (wait-for-path-to-exist zk-client path)
                                (deref))]
                        (info "Determined application id to be" application-id)
                        (watch-node-bytes zk-client path
                                          (fn [bytes]
                                            (let [url (read-url bytes)]
                                              (info "Leading REST url changed to" url)
                                              (reset! rest-url-atom url))))))]
    {:rest-url-atom rest-url-atom
     :closer        (fn [] (zk/close zk-client))
     :init-future   init-future}))

(defn list-jobs!
  [base-url]
  (->>
    (http/get (str base-url "/jobs") {:as :json})
    :body
    :jobs
    (map :id)))

(defn get-job-details!
  [base-url job-id]
  (assert base-url)
  (assert job-id)
  (let [job-details (->
                      (http/get (str base-url "/jobs/" job-id) {:as :json})
                      :body)]
    (assert (:vertices job-details) "Job does not have vertices")
    job-details))

(defn job-running?
  [base-url job-id]
  (->>
    (get-job-details! base-url job-id)
    :vertices
    (map :status)
    (every? #(= "RUNNING" %))))

(defrecord Client
  [deploy-cluster! closer rest-url init-future job-id]
  client/Client
  (open! [this test node]
    (let [{:keys [rest-url-atom closer init-future]} (make-job-manager-url test)]
      (assoc this :closer closer :rest-url rest-url-atom :init-future init-future :job-id (atom nil))))

  (setup! [this test] this)

  (invoke! [this test op]
    (case (:f op)
      :submit (do
                (deploy-cluster! test)
                (deref init-future)
                (let [jobs (fu/retry (fn [] (list-jobs! @rest-url))
                                     :fallback (fn [e] (do
                                                         (fatal e "Could not get running jobs.")
                                                         (System/exit 1))))
                      num-jobs (count jobs)]
                  (assert (= 1 num-jobs) (str "Expected 1 job, was " num-jobs))
                  (reset! job-id (first jobs)))
                (assoc op :type :ok))
      :job-running? (let [base-url @rest-url]
                      (if base-url
                        (try
                          (assoc op :type :ok :value (job-running? base-url @job-id))
                          (catch Exception e (do
                                               (warn e "Get job details from" base-url "failed.")
                                               (assoc op :type :fail))))
                        (assoc op :type :fail :value "Cluster not deployed yet.")))))

  (teardown! [this test])
  (close! [this test] (closer)))
