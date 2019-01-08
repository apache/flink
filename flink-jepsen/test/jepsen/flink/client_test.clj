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

(ns jepsen.flink.client-test
  (:require [clojure.test :refer :all]
            [clj-http.fake :as fake]
            [jepsen.flink.client :refer :all]))

(deftest read-url-test
  (is (= "https://www.asdf.de" (read-url (byte-array [0xAC 0xED 0x00 0x05 0x77 0x15 0x00 0x13 0x68 0x74 0x74 0x70 0x73 0x3A 0x2F 0x2F 0x77 0x77 0x77 0x2E 0x61 0x73 0x64 0x66 0x2E 0x64 0x65])))))

(deftest job-running?-test
  (fake/with-fake-routes
    {"http://localhost:8081/jobs/a718f168ec6be8eff1345a17bf64196c"
     (fn [request] {:status  200
                    :headers {}
                    :body    "{\"jid\":\"54ae4d8ec01d85053d7eb5d139492df2\",\"name\":\"Socket Window WordCount\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1522059578198,\"end-time\":-1,\"duration\":19505,\"now\":1522059597703,\"timestamps\":{\"RUNNING\":1522059578244,\"RESTARTING\":0,\"RECONCILING\":0,\"CREATED\":1522059578198,\"FAILING\":0,\"FINISHED\":0,\"CANCELLING\":0,\"SUSPENDING\":0,\"FAILED\":0,\"CANCELED\":0,\"SUSPENDED\":0},\"vertices\":[{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"name\":\"Source: Socket Stream -> Flat Map\",\"parallelism\":1,\"status\":\"RUNNING\",\"start-time\":1522059578369,\"end-time\":-1,\"duration\":19334,\"tasks\":{\"DEPLOYING\":0,\"SCHEDULED\":0,\"CANCELED\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FAILED\":0,\"RUNNING\":1,\"CREATED\":0,\"FINISHED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":false,\"write-bytes\":0,\"write-bytes-complete\":false,\"read-records\":0,\"read-records-complete\":false,\"write-records\":0,\"write-records-complete\":false}},{\"id\":\"90bea66de1c231edf33913ecd54406c1\",\"name\":\"Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) -> Sink: Print to Std. Out\",\"parallelism\":1,\"status\":\"RUNNING\",\"start-time\":1522059578381,\"end-time\":-1,\"duration\":19322,\"tasks\":{\"DEPLOYING\":0,\"SCHEDULED\":0,\"CANCELED\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FAILED\":0,\"RUNNING\":1,\"CREATED\":0,\"FINISHED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":false,\"write-bytes\":0,\"write-bytes-complete\":false,\"read-records\":0,\"read-records-complete\":false,\"write-records\":0,\"write-records-complete\":false}}],\"status-counts\":{\"DEPLOYING\":0,\"SCHEDULED\":0,\"CANCELED\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FAILED\":0,\"RUNNING\":2,\"CREATED\":0,\"FINISHED\":0},\"plan\":{\"jid\":\"54ae4d8ec01d85053d7eb5d139492df2\",\"name\":\"Socket Window WordCount\",\"nodes\":[{\"id\":\"90bea66de1c231edf33913ecd54406c1\",\"parallelism\":1,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out\",\"inputs\":[{\"num\":0,\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"ship_strategy\":\"HASH\",\"exchange\":\"pipelined_bounded\"}],\"optimizer_properties\":{}},{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"parallelism\":1,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Socket Stream -&gt; Flat Map\",\"optimizer_properties\":{}}]}}"})
     "http://localhost:8081/jobs/a718f168ec6be8eff1345a17bf64196d"
     (fn [request] {:status  200
                    :headers {}
                    :body    "{\"jid\":\"54ae4d8ec01d85053d7eb5d139492df2\",\"name\":\"Socket Window WordCount\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1522059578198,\"end-time\":-1,\"duration\":19505,\"now\":1522059597703,\"timestamps\":{\"RUNNING\":1522059578244,\"RESTARTING\":0,\"RECONCILING\":0,\"CREATED\":1522059578198,\"FAILING\":0,\"FINISHED\":0,\"CANCELLING\":0,\"SUSPENDING\":0,\"FAILED\":0,\"CANCELED\":0,\"SUSPENDED\":0},\"vertices\":[{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"name\":\"Source: Socket Stream -> Flat Map\",\"parallelism\":1,\"status\":\"CREATED\",\"start-time\":1522059578369,\"end-time\":-1,\"duration\":19334,\"tasks\":{\"DEPLOYING\":0,\"SCHEDULED\":0,\"CANCELED\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FAILED\":0,\"RUNNING\":1,\"CREATED\":0,\"FINISHED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":false,\"write-bytes\":0,\"write-bytes-complete\":false,\"read-records\":0,\"read-records-complete\":false,\"write-records\":0,\"write-records-complete\":false}},{\"id\":\"90bea66de1c231edf33913ecd54406c1\",\"name\":\"Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) -> Sink: Print to Std. Out\",\"parallelism\":1,\"status\":\"RUNNING\",\"start-time\":1522059578381,\"end-time\":-1,\"duration\":19322,\"tasks\":{\"DEPLOYING\":0,\"SCHEDULED\":0,\"CANCELED\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FAILED\":0,\"RUNNING\":1,\"CREATED\":0,\"FINISHED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":false,\"write-bytes\":0,\"write-bytes-complete\":false,\"read-records\":0,\"read-records-complete\":false,\"write-records\":0,\"write-records-complete\":false}}],\"status-counts\":{\"DEPLOYING\":0,\"SCHEDULED\":0,\"CANCELED\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FAILED\":0,\"RUNNING\":2,\"CREATED\":0,\"FINISHED\":0},\"plan\":{\"jid\":\"54ae4d8ec01d85053d7eb5d139492df2\",\"name\":\"Socket Window WordCount\",\"nodes\":[{\"id\":\"90bea66de1c231edf33913ecd54406c1\",\"parallelism\":1,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out\",\"inputs\":[{\"num\":0,\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"ship_strategy\":\"HASH\",\"exchange\":\"pipelined_bounded\"}],\"optimizer_properties\":{}},{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"parallelism\":1,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Socket Stream -&gt; Flat Map\",\"optimizer_properties\":{}}]}}"})}

    (is (= (job-running? "http://localhost:8081" "a718f168ec6be8eff1345a17bf64196c") true))
    (is (= (job-running? "http://localhost:8081" "a718f168ec6be8eff1345a17bf64196d") false))))
