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

export FLINK_JOB=org.apache.flink.examples.java.wordcount.WordCount
export FLINK_IMAGE_NAME=test_kubernetes_embedded_job
export OUTPUT_VOLUME=${TEST_DATA_DIR}/out
export OUTPUT_FILE=kubernetes_wc_out
export FLINK_JOB_PARALLELISM=1
export FLINK_JOB_ARGUMENTS='"--output", "/cache/kubernetes_wc_out"'

function cleanup {
    kubectl delete job flink-job-cluster
    kubectl delete service flink-job-cluster
    kubectl delete deployment flink-task-manager
    rm -rf ${OUTPUT_VOLUME}
}

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

on_exit cleanup

mkdir -p $OUTPUT_VOLUME

if ! retry_times ${MINIKUBE_START_RETRIES} ${MINIKUBE_START_BACKOFF} start_kubernetes_if_not_running; then
    echo "Minikube not running. Could not start minikube. Aborting..."
    exit 1
fi

eval $(minikube docker-env)
cd "$DOCKER_MODULE_DIR"
./build.sh --from-local-dist --job-artifacts ${FLINK_DIR}/examples/batch/WordCount.jar --image-name ${FLINK_IMAGE_NAME}
cd "$END_TO_END_DIR"


kubectl create -f ${KUBERNETES_MODULE_DIR}/job-cluster-service.yaml
envsubst '${FLINK_IMAGE_NAME} ${FLINK_JOB} ${FLINK_JOB_PARALLELISM} ${FLINK_JOB_ARGUMENTS}' < ${CONTAINER_SCRIPTS}/job-cluster-job.yaml.template | kubectl create -f -
envsubst '${FLINK_IMAGE_NAME} ${FLINK_JOB_PARALLELISM}' < ${CONTAINER_SCRIPTS}/task-manager-deployment.yaml.template | kubectl create -f -
kubectl wait --for=condition=complete job/flink-job-cluster --timeout=1h
kubectl cp `kubectl get pods | awk '/task-manager/ {print $1}'`:/cache/${OUTPUT_FILE} ${OUTPUT_VOLUME}/${OUTPUT_FILE}

check_result_hash "WordCount" ${OUTPUT_VOLUME}/${OUTPUT_FILE} "e682ec6622b5e83f2eb614617d5ab2cf"
