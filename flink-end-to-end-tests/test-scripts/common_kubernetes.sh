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

CONTAINER_SCRIPTS=${END_TO_END_DIR}/test-scripts/container-scripts
MINIKUBE_START_RETRIES=3
MINIKUBE_START_BACKOFF=5
RESULT_HASH="e682ec6622b5e83f2eb614617d5ab2cf"
MINIKUBE_VERSION="v1.8.2"

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
    # Download minikube when it is not installed beforehand.
    if ! [ -x "$(command -v minikube)" ]; then
        echo "Installing minikube $MINIKUBE_VERSION ..."
        curl -Lo minikube https://storage.googleapis.com/minikube/releases/$MINIKUBE_VERSION/minikube-linux-$arch && \
            chmod +x minikube && sudo mv minikube /usr/bin/minikube
    fi
    # conntrack is required for minikube 1.9 and later
    sudo apt-get install conntrack
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
        sudo CHANGE_MINIKUBE_NONE_USER=true minikube start --vm-driver=none \
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
        # Mount Flink dist into minikube virtual machine because we need to mount hostPath as usrlib
        minikube mount $FLINK_DIR:$FLINK_DIR &
        export minikube_mount_pid=$!
        echo "The mounting process is running with pid $minikube_mount_pid"
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
        echo "Killing mounting process $minikube_mount_pid"
        kill $minikube_mount_pid 2> /dev/null
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
  wait_for_logs $1 "Rest endpoint listening at"
}

function wait_num_checkpoints {
    POD_NAME=$1
    NUM_CHECKPOINTS=$2

    echo "Waiting for job ($POD_NAME) to have at least $NUM_CHECKPOINTS completed checkpoints ..."

   # wait at most 120 seconds
    local TIMEOUT=120
    for i in $(seq 1 ${TIMEOUT}); do
      N=$(kubectl logs $POD_NAME 2> /dev/null | grep -o "Completed checkpoint [1-9]* for job" | awk '{print $3}' | tail -1)

      if [ -z $N ]; then
        N=0
      fi

      if (( N < NUM_CHECKPOINTS )); then
        sleep 1
      else
        return
      fi
    done
    echo "Could not get $NUM_CHECKPOINTS completed checkpoints in $TIMEOUT sec"
    exit 1
}

function wait_for_logs {
  local jm_pod_name=$1
  local successful_response_regex=$2

  echo "Waiting for jobmanager pod ${jm_pod_name} ready."
  kubectl wait --for=condition=Ready --timeout=30s pod/$jm_pod_name || exit 1

  # wait at most 30 seconds until the log shows up
  local TIMEOUT=30
  echo "Waiting for log \"$2\"..."
  for i in $(seq 1 ${TIMEOUT}); do
    if check_logs_output $jm_pod_name $successful_response_regex; then
      echo "Log \"$2\" shows up."
      return
    fi

    sleep 1
  done
  echo "Log $2 does not show up within a timeout of ${TIMEOUT} sec"
  exit 1
}

function check_logs_output {
  local pod_name=$1
  local successful_response_regex=$2
  LOG_CONTENT=$(kubectl logs $pod_name 2> /dev/null)

  # ensure the log content adapts with the successful regex
  if [[ ${LOG_CONTENT} =~ ${successful_response_regex} ]]; then
    return 0
  fi
  return 1
}

function cleanup {
    if [ $TRAPPED_EXIT_CODE != 0 ];then
      debug_and_show_logs
    fi
    internal_cleanup
    kubectl wait --for=delete pod --all=true
    stop_kubernetes
}

function get_host_machine_address {
    if [[ "${OS_TYPE}" != "linux" ]]; then
        echo $(minikube ssh "route -n | grep ^0.0.0.0 | awk '{ print \$2 }' | tr -d '[:space:]'")
    else
        echo $(hostname --ip-address)
    fi
}

on_exit cleanup
