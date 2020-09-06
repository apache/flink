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

(defproject jepsen.flink "0.1.0-SNAPSHOT"
  :license {:name "Apache License"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :main jepsen.flink.flink
  :aot [jepsen.flink.flink]
  :dependencies [[org.clojure/clojure "1.9.0"],
                 [cheshire "5.8.0"]
                 [clj-http "3.8.0"]
                 [jepsen "0.1.13"],
                 [jepsen.zookeeper "0.1.0"]
                 [org.clojure/data.xml "0.0.8"]
                 [zookeeper-clj "0.9.4" :exclusions [org.slf4j/slf4j-log4j12]]]
  :profiles {:test {:dependencies [[clj-http-fake "1.0.3"]]}})
