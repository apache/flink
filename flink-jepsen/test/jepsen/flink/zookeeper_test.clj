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

(ns jepsen.flink.zookeeper-test
  (:require [clojure.test :refer :all])
  (:require [jepsen.flink.zookeeper :refer :all]))

(deftest zookeeper-quorum-test
  (is (= (zookeeper-quorum {:nodes ["n1" "n2" "n3"]}) "n1:2181,n2:2181,n3:2181")))

(deftest zookeeper-uri-test
  (is (= (zookeeper-uri {:nodes ["n1" "n2" "n3"]}) "zk://n1:2181,n2:2181,n3:2181/"))
  (is (= (zookeeper-uri {:nodes ["n1"]}) "zk://n1:2181/")))
