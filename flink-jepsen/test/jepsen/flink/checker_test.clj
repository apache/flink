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

(ns jepsen.flink.checker-test
  (:require [clojure.test :refer :all]
            [jepsen
             [checker :as checker]]
            [jepsen.flink.checker :refer :all]))

(deftest get-job-running-history-test
  (let [history [{:type :info, :f :kill-random-subset-task-managers, :process :nemesis, :time 121898381144, :value '("172.31.33.170")}
                 {:type :invoke, :f :job-running?, :value nil, :process 0, :time 127443701575}
                 {:type :ok, :f :job-running?, :value false, :process 0, :time 127453553462}
                 {:type :invoke, :f :job-running?, :value nil, :process 0, :time 127453553463}
                 {:type :ok, :f :job-running?, :value true, :process 0, :time 127453553464}
                 {:type :info, :f :job-running?, :value nil, :process 0, :time 127453553465}]]
    (is (= (get-job-running-history history) [false true false]))))

(deftest job-running-checker-test
  (let [checker (job-running-checker)
        test {}
        model (job-running-within-grace-period 3 60 10)
        opts {}
        check (fn [history] (checker/check checker test model history opts))]
    (testing "Model should be inconsistent if job is not running after grace period."
      (let [result (check
                     [{:type :info, :f :kill-task-managers, :process :nemesis, :time 0, :value ["172.31.32.48"]}
                      {:type :ok, :f :job-running?, :value false, :process 0, :time 60000000001}])]
        (is (= false (:valid? result)))
        (is (= "Job is not running." (-> result :final-model :msg)))))
    (testing "Model should be consistent if job is running after grace period."
      (is (= true (:valid? (check
                             [{:type :info, :f :kill-task-managers, :process :nemesis, :time 0, :value ["172.31.32.48"]}
                              {:type :ok, :f :job-running?, :value true, :process 0, :time 60000000001}
                              {:type :ok, :f :job-running?, :value true, :process 0, :time 60000000002}
                              {:type :ok, :f :job-running?, :value true, :process 0, :time 60000000003}])))))
    (testing "Should tolerate non-running job during failures."
      (is (= true (:valid? (check
                             [{:type :info, :f :partition-start, :process :nemesis, :time -1}
                              {:type :info, :f :partition-start, :process :nemesis, :time 0, :value "Cut off [...]"}
                              {:type :ok, :f :job-running?, :value false, :process 0, :time 60000000001}
                              {:type :info, :f :partition-stop, :process :nemesis, :time 60000000002}
                              {:type :info, :f :partition-stop, :process :nemesis, :time 60000000003, :value "fully connected"}
                              {:type :ok, :f :job-running?, :value true, :process 0, :time 60000000004}
                              {:type :ok, :f :job-running?, :value true, :process 0, :time 60000000005}
                              {:type :ok, :f :job-running?, :value true, :process 0, :time 60000000006}])))))
    (testing "Should not tolerate non-running job without a cause."
      (let [result (check
                     [{:type :ok, :f :job-running?, :value true, :process 0, :time 0}
                      {:type :ok, :f :job-running?, :value true, :process 0, :time 1}
                      {:type :ok, :f :job-running?, :value false, :process 0, :time 60000000001}
                      {:type :ok, :f :job-running?, :value true, :process 0, :time 60000000002}])]
        (is (= false (:valid? result)))
        (is (= "Job is not running." (-> result :final-model :msg)))))
    (testing "Model should be inconsistent if job submission was unsuccessful."
      (let [result (check [{:type :invoke, :f :job-running?, :value nil, :process 0, :time 239150413307}
                           {:type :info, :f :job-running?, :value nil, :process 0, :time 239150751938, :error "indeterminate: Assert failed: job-id"}])]
        (is (= false (:valid? result)))))
    (testing "Model should be inconsistent if the job status cannot be polled, i.e., if the cluster is unavailable."
      (let [result (check [{:type :fail, :f :job-running?, :value nil, :process 0, :time 0 :error "Error"}
                           {:type :fail, :f :job-running?, :value nil, :process 0, :time 60000000001 :error "Error"}
                           {:type :fail, :f :job-running?, :value nil, :process 0, :time 60000000002 :error "Error"}])]
        (is (= false (:valid? result)))
        (is (= "Cluster is not running." (-> result :final-model :msg)))))
    (testing "Should tolerate non-running job after cancellation."
      (is (= true (:valid? (check [{:type :invoke, :f :cancel-job, :value nil, :process 0, :time 0}
                                   {:type :ok, :f :cancel-job, :value true, :process 0, :time 1}
                                   {:type :ok, :f :job-running?, :value true, :process 0, :time 2}
                                   {:type :ok, :f :job-running?, :value false, :process 0, :time 3}])))))
    (testing "Model should be inconsistent if job is running after cancellation."
      (let [result (check [{:type :invoke, :f :cancel-job, :value nil, :process 0, :time 0}
                           {:type :ok, :f :cancel-job, :value true, :process 0, :time 1}
                           {:type :ok, :f :job-running?, :value true, :process 0, :time 10000000002}])]
        (is (= false (:valid? result)))
        (is (= "Job is running after cancellation." (-> result :final-model :msg)))))
    (testing "Model should be inconsistent if Flink cluster is not available at the end."
      (let [result (check [{:type :ok, :f :job-running?, :value true, :process 0, :time 0}
                           {:type :ok, :f :job-running?, :value true, :process 0, :time 1}
                           {:type :ok, :f :job-running?, :value true, :process 0, :time 2}
                           {:type :fail, :f :job-running?, :value nil, :process 0, :time 60000000003, :error "Error"}])]
        (is (= false (:valid? result)))
        (is (= "Cluster is not running." (-> result :final-model :msg)))))
    (testing "Model should be inconsistent if Flink cluster is not available after job cancellation."
      (let [result (check [{:type :ok, :f :job-running?, :value true, :process 0, :time 0}
                           {:type :invoke, :f :cancel-job, :value nil, :process 0, :time 1}
                           {:type :ok, :f :cancel-job, :value true, :process 0, :time 2}
                           {:type :fail, :f :job-running?, :value nil, :process 0, :time 60000000001, :error "Error"}])]
        (is (= false (:valid? result)))
        (is (= "Cluster is not running." (-> result :final-model :msg)))))
    (testing "Should throw AssertionError if job cancelling operation failed."
      (is (thrown-with-msg? AssertionError
                            #":cancel-job must not fail"
                            (check [{:type :fail, :f :cancel-job, :value nil, :process 0, :time 0}]))))
    (testing "Should tolerate non-running job if grace period has not passed."
      (is (= true (:valid? (check [{:type :invoke, :f :job-running?, :value nil, :process 0, :time 0}
                                   {:type :ok, :f :job-running?, :value false, :process 0, :time 1}
                                   {:type :ok, :f :job-running?, :value true, :process 0, :time 2}
                                   {:type :ok, :f :job-running?, :value true, :process 0, :time 3}
                                   {:type :ok, :f :job-running?, :value true, :process 0, :time 4}])))))))

(deftest safe-inc-test
  (is (= (safe-inc nil) 1))
  (is (= (safe-inc 1) 2)))

(deftest nemeses-active?-test
  (is (= (nemeses-active? {:partition-start 2 :fail-name-node-start 0}) true))
  (is (= (nemeses-active? {:partition-start 0}) false)))

(deftest dissoc-if-test
  (is (= (:a (dissoc-if #(-> (first %) (= :b)) {:a 1 :b 2})) 1)))

(deftest zero-value?-test
  (is (= (zero-value? [:test 0]) true))
  (is (= (zero-value? [:test 1]) false)))
