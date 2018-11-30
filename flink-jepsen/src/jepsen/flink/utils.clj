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
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [util :refer [meh]]]
            [jepsen.os.debian :as debian]))

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

(defn find-files!
  "Lists files recursively given a directory. If the directory does not exist, an empty collection
  is returned."
  [dir]
  (let [files (try
                (c/exec :find dir :-type :f)
                (catch Exception e
                  (if (.contains (.getMessage e) "No such file or directory")
                    ""
                    (throw e))))]
    (->>
      (clojure.string/split files #"\n")
      (remove clojure.string/blank?))))

;;; runit process supervisor (http://smarden.org/runit/)

(def runit-version "2.1.2-3")

(defn- install-process-supervisor!
  "Installs the process supervisor."
  []
  (debian/install {:runit runit-version}))

(defn create-supervised-service!
  "Registers a service with the process supervisor and starts it."
  [service-name cmd]
  (let [service-dir (str "/etc/sv/" service-name)
        run-script (str service-dir "/run")]
    (info "Create supervised service" service-name)
    (c/su
      (install-process-supervisor!)
      (c/exec :mkdir :-p service-dir)
      (c/exec :echo (clojure.string/join "\n" ["#!/bin/sh"
                                               "exec 2>&1"
                                               (str "exec " cmd)]) :> run-script)
      (c/exec :chmod :+x run-script)
      (c/exec :ln :-sfT service-dir (str "/etc/service/" service-name)))))

(defn stop-supervised-service!
  "Stops a service and removes it from supervision."
  [service-name]
  (info "Stop supervised service" service-name)
  (c/su
    (c/exec :rm :-f (str "/etc/service/" service-name))))

(defn stop-all-supervised-services!
  "Stops and removes all services from supervision if any."
  []
  (info "Stop all supervised services.")
  (c/su
    ;; HACK:
    ;; Remove all symlinks in /etc/service except sshd.
    ;; This is only relevant when tests are run in Docker because there sshd is started using runit.
    (meh (c/exec :find (c/lit (str "/etc/service -maxdepth 1 -type l ! -name 'sshd' -delete"))))))
