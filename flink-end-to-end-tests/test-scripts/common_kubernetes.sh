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

# If running tests on non-linux os, the kubectl and minikube should be installed manually
function setup_kubernetes_for_linux {
    # Download kubectl, which is a requirement for using minikube.
    if ! [ -x "$(command -v kubectl)" ]; then
        local version=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
        curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/$version/bin/linux/amd64/kubectl && \
            chmod +x kubectl && sudo mv kubectl /usr/local/bin/
    fi
    # Download minikube.
    if ! [ -x "$(command -v minikube)" ]; then
        curl -Lo minikube https://storage.googleapis.com/minikube/releases/v1.8.2/minikube-linux-amd64 && \
            chmod +x minikube && sudo mv minikube /usr/local/bin/
    fi
}

function check_kubernetes_status {
    minikube status
    return $?
}

function start_kubernetes_if_not_running {
    if ! check_kubernetes_status; then
        start_command="minikube start"
        # We need sudo permission to set vm-driver to none in linux os.
        [[ "${OS_TYPE}" = "linux" ]] && start_command="sudo ${start_command} --vm-driver=none"
        ${start_command}
        # Fix the kubectl context, as it's often stale.
        minikube update-context
    fi

    check_kubernetes_status
    return $?
}

function start_kubernetes {
    [[ "${OS_TYPE}" = "linux" ]] && setup_kubernetes_for_linux
    if ! retry_times ${MINIKUBE_START_RETRIES} ${MINIKUBE_START_BACKOFF} start_kubernetes_if_not_running; then
        echo "Could not start minikube. Aborting..."
        exit 1
    fi
    eval $(minikube docker-env)
}

function stop_kubernetes {
    stop_command="minikube stop"
    [[ "${OS_TYPE}" = "linux" ]] && stop_command="sudo ${stop_command}"
    if ! retry_times ${MINIKUBE_START_RETRIES} ${MINIKUBE_START_BACKOFF} "${stop_command}"; then
        echo "Could not stop minikube. Aborting..."
        exit 1
    fi
}

on_exit cleanup
