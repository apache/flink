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

(ns jepsen.flink.utils
  (:require [clojure.tools.logging :refer :all]))

(defn retry
  "Runs a function op and retries on exception.

  The following options are supported:

  :on-retry - A function called for every retry with an exception and the attempt number as arguments.
  :success - A function called with the result of op.
  :fallback – A function with an exception as the first argument that is called if all retries are exhausted.
  :retries - Number of total retries.
  :delay – The time between retries."
  ([op & {:keys [on-retry success fallback retries delay]
          :or   {on-retry (fn [exception attempt] (warn "Retryable operation failed:"
                                                        (.getMessage exception)))
                 success  identity
                 fallback :default
                 retries  10
                 delay    2000}
          :as   keys}]
   (let [r (try
             (op)
             (catch Exception e (if (< 0 retries)
                                  {:exception e}
                                  (fallback e))))]
     (if (:exception r)
       (do
         (on-retry (:exception r) retries)
         (Thread/sleep delay)
         (recur op (assoc keys :retries (dec retries))))
       (success r)))))
