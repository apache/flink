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

CLUSTER_ROLE_BINDING="flink-role-binding-default"
CLUSTER_ID="flink-native-k8s-session-1"
FLINK_IMAGE_NAME="test_kubernetes_session"
LOCAL_OUTPUT_PATH="${TEST_DATA_DIR}/out/wc_out"
OUTPUT_PATH="/tmp/wc_out"
OUTPUT_ARGS="--output ${OUTPUT_PATH}"
IMAGE_BUILD_RETRIES=3
IMAGE_BUILD_BACKOFF=2

INPUT_TYPE=${1:-embedded}
case $INPUT_TYPE in
    (embedded)
        INPUT_ARGS=""
    ;;
    (dummy-fs)
        source "$(dirname "$0")"/common_dummy_fs.sh
        cp_dummy_fs_to_opt
        INPUT_ARGS="--input dummy://localhost/words --input anotherDummy://localhost/words"
        RESULT_HASH="0e5bd0a3dd7d5a7110aa85ff70adb54b"
        ENABLE_DUMMPY_FS_ARGS="-Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-dummy-fs.jar;flink-another-dummy-fs.jar \
        -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-dummy-fs.jar;flink-another-dummy-fs.jar"
    ;;
    (*)
        echo "Unknown input type $INPUT_TYPE"
        exit 1
    ;;
esac

function internal_cleanup {
    kubectl delete deployment ${CLUSTER_ID}
    kubectl delete clusterrolebinding ${CLUSTER_ROLE_BINDING}
}

start_kubernetes

if ! retry_times $IMAGE_BUILD_RETRIES $IMAGE_BUILD_BACKOFF "build_image ${FLINK_IMAGE_NAME} $(get_host_machine_address)"; then
    echo "ERROR: Could not build image. Aborting..."
    exit 1
fi

kubectl create clusterrolebinding ${CLUSTER_ROLE_BINDING} --clusterrole=edit --serviceaccount=default:default --namespace=default

mkdir -p "$(dirname $LOCAL_OUTPUT_PATH)"

# Enable dummy fs for Flink client
[[ $INPUT_TYPE = 'dummy-fs' ]] && dummy_fs_setup

# Set the memory and cpu smaller than default, so that the jobmanager and taskmanager pods could be allocated in minikube.
"$FLINK_DIR"/bin/kubernetes-session.sh -Dkubernetes.cluster-id=${CLUSTER_ID} \
    -Dkubernetes.container.image=${FLINK_IMAGE_NAME} \
    -Djobmanager.memory.process.size=1088m \
    -Dkubernetes.jobmanager.cpu=0.5 \
    -Dkubernetes.taskmanager.cpu=0.5 \
    -Dkubernetes.rest-service.exposed.type=NodePort \
    $ENABLE_DUMMPY_FS_ARGS

kubectl wait --for=condition=Available --timeout=30s deploy/${CLUSTER_ID} || exit 1
jm_pod_name=$(kubectl get pods --selector="app=${CLUSTER_ID},component=jobmanager" -o jsonpath='{..metadata.name}')
wait_rest_endpoint_up_k8s $jm_pod_name

"$FLINK_DIR"/bin/flink run -e kubernetes-session \
    -Dkubernetes.cluster-id=${CLUSTER_ID} \
    ${FLINK_DIR}/examples/batch/WordCount.jar ${INPUT_ARGS} ${OUTPUT_ARGS}

if ! check_logs_output $jm_pod_name 'Starting KubernetesSessionClusterEntrypoint'; then
  echo "JobManager logs are not accessible via kubectl logs."
  exit 1
fi

tm_pod_name=$(kubectl get pods | awk '/taskmanager/ {print $1}')
if ! check_logs_output $tm_pod_name 'Starting Kubernetes TaskExecutor runner'; then
  echo "TaskManager logs are not accessible via kubectl logs."
  exit 1
fi

kubectl cp `kubectl get pods | awk '/taskmanager/ {print $1}'`:${OUTPUT_PATH} ${LOCAL_OUTPUT_PATH}

check_result_hash "WordCount" "${LOCAL_OUTPUT_PATH}" "${RESULT_HASH}"
