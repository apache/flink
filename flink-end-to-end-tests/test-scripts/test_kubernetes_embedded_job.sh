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

source "$(dirname "$0")"/common_kubernetes.sh

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
    stop_kubernetes
}

start_kubernetes

mkdir -p $OUTPUT_VOLUME

cd "$DOCKER_MODULE_DIR"
./build.sh --from-local-dist --job-artifacts ${FLINK_DIR}/examples/batch/WordCount.jar --image-name ${FLINK_IMAGE_NAME}
cd "$END_TO_END_DIR"


kubectl create -f ${KUBERNETES_MODULE_DIR}/job-cluster-service.yaml
envsubst '${FLINK_IMAGE_NAME} ${FLINK_JOB} ${FLINK_JOB_PARALLELISM} ${FLINK_JOB_ARGUMENTS}' < ${CONTAINER_SCRIPTS}/job-cluster-job.yaml.template | kubectl create -f -
envsubst '${FLINK_IMAGE_NAME} ${FLINK_JOB_PARALLELISM}' < ${CONTAINER_SCRIPTS}/task-manager-deployment.yaml.template | kubectl create -f -
kubectl wait --for=condition=complete job/flink-job-cluster --timeout=1h
kubectl cp `kubectl get pods | awk '/task-manager/ {print $1}'`:/cache/${OUTPUT_FILE} ${OUTPUT_VOLUME}/${OUTPUT_FILE}

check_result_hash "WordCount" ${OUTPUT_VOLUME}/${OUTPUT_FILE} "${RESULT_HASH}"
