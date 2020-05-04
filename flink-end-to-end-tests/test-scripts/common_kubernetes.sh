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
source "$(dirname "$0")"/common_docker.sh

DOCKER_MODULE_DIR=${END_TO_END_DIR}/../flink-container/docker
KUBERNETES_MODULE_DIR=${END_TO_END_DIR}/../flink-container/kubernetes
CONTAINER_SCRIPTS=${END_TO_END_DIR}/test-scripts/container-scripts
MINIKUBE_START_RETRIES=3
MINIKUBE_START_BACKOFF=5
RESULT_HASH="e682ec6622b5e83f2eb614617d5ab2cf"

NON_LINUX_ENV_NOTE="****** Please start/stop minikube manually in non-linux environment. ******"

# If running tests on non-linux os, the kubectl and minikube should be installed manually
function setup_kubernetes_for_linux {
    if [[ `uname -i` == 'aarch64' ]]; then
        local arch='arm64'
    else
        local arch='amd64'
    fi
    # Download kubectl, which is a requirement for using minikube.
    if ! [ -x "$(command -v kubectl)" ]; then
        echo "Installing kubectl ..."
        local version=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
        curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/$version/bin/linux/$arch/kubectl && \
            chmod +x kubectl && sudo mv kubectl /usr/local/bin/
    fi
    # Download minikube.
    if ! [ -x "$(command -v minikube)" ]; then
        echo "Installing minikube ..."
        curl -Lo minikube https://storage.googleapis.com/minikube/releases/v1.8.2/minikube-linux-$arch && \
            chmod +x minikube && sudo mv minikube /usr/local/bin/
    fi
}

function check_kubernetes_status {
    minikube status
    return $?
}

function start_kubernetes_if_not_running {
    if ! check_kubernetes_status; then
        echo "Starting minikube ..."
        # We need sudo permission to set vm-driver to none in linux os.
        # tl;dr: Configure minikube for a low disk space environment
        #
        # The VMs provided by azure have ~100GB of disk space, out of which
        # 85% are allocated, only 15GB are free. That's enough space
        # for our purposes. However, the kubernetes nodes running during
        # the k8s tests believe that 85% are not enough free disk space,
        # so they start garbage collecting their host. During GCing, they
        # are deleting all docker images currently not in use.
        # However, the k8s test is first building a flink image, then launching
        # stuff on k8s. Sometimes, k8s deletes the new Flink images,
        # thus it can not find them anymore, letting the test fail /
        # timeout. That's why we have set the GC threshold to 98% and 99%
        # here.
        # Similarly, the kubelets are marking themself as "low disk space",
        # causing Flink to avoid this node (again, failing the test)
        # Use fixed version v1.16.9 because fabric8 kubernetes-client could not work with higher version under jdk 8u252
        sudo CHANGE_MINIKUBE_NONE_USER=true minikube start --kubernetes-version v1.16.9 --vm-driver=none \
            --extra-config=kubelet.image-gc-high-threshold=99 \
            --extra-config=kubelet.image-gc-low-threshold=98 \
            --extra-config=kubelet.minimum-container-ttl-duration=120m \
            --extra-config=kubelet.eviction-hard="memory.available<5Mi,nodefs.available<1Mi,imagefs.available<1Mi" \
            --extra-config=kubelet.eviction-soft="memory.available<5Mi,nodefs.available<2Mi,imagefs.available<2Mi" \
            --extra-config=kubelet.eviction-soft-grace-period="memory.available=2h,nodefs.available=2h,imagefs.available=2h"
        # Fix the kubectl context, as it's often stale.
        minikube update-context
    fi

    check_kubernetes_status
    return $?
}

function start_kubernetes {
    if [[ "${OS_TYPE}" != "linux" ]]; then
        if ! check_kubernetes_status; then
            echo "$NON_LINUX_ENV_NOTE"
            exit 1
        fi
    else
        setup_kubernetes_for_linux
        if ! retry_times ${MINIKUBE_START_RETRIES} ${MINIKUBE_START_BACKOFF} start_kubernetes_if_not_running; then
            echo "Could not start minikube. Aborting..."
            exit 1
        fi
    fi
    eval $(minikube docker-env)
}

function stop_kubernetes {
    if [[ "${OS_TYPE}" != "linux" ]]; then
        echo "$NON_LINUX_ENV_NOTE"
    else
        echo "Stopping minikube ..."
        stop_command="sudo minikube stop"
        if ! retry_times ${MINIKUBE_START_RETRIES} ${MINIKUBE_START_BACKOFF} "${stop_command}"; then
            echo "Could not stop minikube. Aborting..."
            exit 1
        fi
    fi
}

function debug_and_show_logs {
    echo "Debugging failed Kubernetes test:"
    echo "Currently existing Kubernetes resources"
    kubectl get all
    kubectl describe all

    echo "Flink logs:"
    kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | while read pod;do
        kubectl logs $pod;
    done
}

function wait_rest_endpoint_up_k8s {
  local jm_pod_name=$1
  local successful_response_regex="Rest endpoint listening at"

  echo "Waiting for jobmanager pod ${jm_pod_name} ready."
  kubectl wait --for=condition=Ready --timeout=30s pod/$jm_pod_name || exit 1

  # wait at most 30 seconds until the endpoint is up
  local TIMEOUT=30
  for i in $(seq 1 ${TIMEOUT}); do
    QUERY_RESULT=$(kubectl logs $jm_pod_name 2> /dev/null)

    # ensure the response adapts with the successful regex
    if [[ ${QUERY_RESULT} =~ ${successful_response_regex} ]]; then
      echo "REST endpoint is up."
      return
    fi

    echo "Waiting for REST endpoint to come up..."
    sleep 1
  done
  echo "REST endpoint has not started within a timeout of ${TIMEOUT} sec"
  exit 1
}

function cleanup {
    if [ $TRAPPED_EXIT_CODE != 0 ];then
      debug_and_show_logs
    fi
    internal_cleanup
    kubectl wait --for=delete pod --all=true
    stop_kubernetes
}

function setConsoleLogging {
    cat >> $FLINK_DIR/conf/log4j.properties <<END
rootLogger.appenderRef.console.ref = ConsoleAppender

# Log all infos to the console
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%t] %-60c %x - %m%n
END
}

on_exit cleanup
