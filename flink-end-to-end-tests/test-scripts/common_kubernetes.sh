#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh

DOCKER_MODULE_DIR=${END_TO_END_DIR}/../flink-container/docker
KUBERNETES_MODULE_DIR=${END_TO_END_DIR}/../flink-container/kubernetes
CONTAINER_SCRIPTS=${END_TO_END_DIR}/test-scripts/container-scripts
MINIKUBE_START_RETRIES=3
MINIKUBE_START_BACKOFF=5
RESULT_HASH="e682ec6622b5e83f2eb614617d5ab2cf"

function check_kubernetes_status {
    minikube status
    return $?
}

function start_kubernetes_if_not_running {
    if ! check_kubernetes_status; then
        minikube start
    fi

    check_kubernetes_status
    return $?
}

function start_kubernetes {
    if ! retry_times ${MINIKUBE_START_RETRIES} ${MINIKUBE_START_BACKOFF} start_kubernetes_if_not_running; then
        echo "Minikube not running. Could not start minikube. Aborting..."
        exit 1
    fi
    eval $(minikube docker-env)
}

on_exit cleanup
