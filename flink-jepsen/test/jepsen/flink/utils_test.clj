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

(ns jepsen.flink.utils-test
  (:require [clojure.test :refer :all])
  (:require [jepsen.flink.utils :refer [retry]]))

(deftest retry-test
  (testing "Single failure then result."
    (let [counter (atom 0)
          failing-once (fn [] (if (= @counter 0)
                                (do (swap! counter inc)
                                    (throw (Exception. "Expected")))
                                "result"))]
      (is (= "result" (retry failing-once :delay 0)))))

  (testing "Exhaust all attempts."
    (let [failing-always (fn [] (throw (Exception. "Expected")))]
      (is (nil? (retry failing-always :retries 1 :delay 0 :fallback :nil)))))

  (testing "Propagate exception."
    (let [failing-always (fn [] (throw (Exception. "Expected")))]
      (is (thrown-with-msg? Exception #"Expected" (retry failing-always
                                                         :retries 1
                                                         :delay 0
                                                         :fallback (fn [e] (throw e))))))))
