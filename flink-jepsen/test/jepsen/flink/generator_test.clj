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

(ns jepsen.flink.generator-test
  (:require [clojure.test :refer :all])
  (:require [jepsen.flink.generator :refer :all]))

(deftest inc-by-factor-test
  (testing "Should not increase if factor is 1."
    (is (= 10 (@#'jepsen.flink.generator/inc-by-factor 10 1))))

  (testing "Should increase by factor."
    (is (= 15 (@#'jepsen.flink.generator/inc-by-factor 10 1.5))))

  (testing "Should round down."
    (is (= 15 (@#'jepsen.flink.generator/inc-by-factor 10 1.52))))

  (testing "Should throw if factor < 1."
    (is (thrown? AssertionError (@#'jepsen.flink.generator/inc-by-factor 1 0)))))
