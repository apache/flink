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

(ns jepsen.flink.zookeeper)

(defn zookeeper-quorum
  "Returns the zk quorum string, e.g., host1:2181,host2:2181"
  [test]
  (->> test
       :nodes
       (map #(str % ":2181"))
       (clojure.string/join ",")))

(defn zookeeper-uri
  ([test] (zookeeper-uri test ""))
  ([test namespace] (str "zk://" (zookeeper-quorum test) "/" (name namespace))))
