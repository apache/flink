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
CLUSTER_ID="flink-native-k8s-application-ha-1"
FLINK_IMAGE_NAME="test_kubernetes_application_ha"
LOCAL_LOGS_PATH="${TEST_DATA_DIR}/log"
IMAGE_BUILD_RETRIES=3
IMAGE_BUILD_BACKOFF=2

function internal_cleanup {
    if [[ "${OS_TYPE}" != "linux" ]]; then
      minikube ssh "sudo rm -rf /tmp/${CLUSTER_ID}"
    else
      sudo rm -rf /tmp/${CLUSTER_ID}
    fi
    kubectl delete deployment ${CLUSTER_ID}
    kubectl delete cm --selector="app=${CLUSTER_ID}"
    kubectl delete clusterrolebinding ${CLUSTER_ROLE_BINDING}
}

start_kubernetes

if ! retry_times $IMAGE_BUILD_RETRIES $IMAGE_BUILD_BACKOFF "build_image ${FLINK_IMAGE_NAME} $(get_host_machine_address)"; then
	echo "ERROR: Could not build image. Aborting..."
	exit 1
fi

kubectl create clusterrolebinding ${CLUSTER_ROLE_BINDING} --clusterrole=edit --serviceaccount=default:default --namespace=default

mkdir -p "$LOCAL_LOGS_PATH"

# Set the memory and cpu smaller than default, so that the jobmanager and taskmanager pods could be allocated in minikube.
"$FLINK_DIR"/bin/flink run-application -t kubernetes-application \
    -Dkubernetes.cluster-id=${CLUSTER_ID} \
    -Dkubernetes.container.image=${FLINK_IMAGE_NAME} \
    -Djobmanager.memory.process.size=1088m \
    -Dkubernetes.jobmanager.cpu=0.5 \
    -Dkubernetes.taskmanager.cpu=0.5 \
    -Dkubernetes.rest-service.exposed.type=NodePort \
    -Dkubernetes.pod-template-file=${CONTAINER_SCRIPTS}/kubernetes-pod-template.yaml \
    -Dhigh-availability=org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory \
    -Dhigh-availability.storageDir=file:///flink-ha \
    -Drestart-strategy=fixed-delay \
    -Drestart-strategy.fixed-delay.attempts=10 \
    local:///opt/flink/examples/streaming/StateMachineExample.jar

kubectl wait --for=condition=Available --timeout=30s deploy/${CLUSTER_ID} || exit 1
jm_pod_name=$(kubectl get pods --selector="app=${CLUSTER_ID},component=jobmanager" -o jsonpath='{..metadata.name}')

wait_num_checkpoints $jm_pod_name 3

job_id=$(kubectl logs $jm_pod_name | grep -E -o 'Job [a-z0-9]+ is submitted' | awk '{print $2}')

# Kill the JobManager
kubectl exec $jm_pod_name -- /bin/sh -c "kill 1"

# Check the new JobManager recovering from latest successful checkpoint
wait_for_logs $jm_pod_name "Restoring job $job_id from Checkpoint" 120
wait_num_checkpoints $jm_pod_name 1

"$FLINK_DIR"/bin/flink cancel -t kubernetes-application -Dkubernetes.cluster-id=${CLUSTER_ID} $job_id

kubectl wait --for=delete configmap --timeout=60s --selector="app=${CLUSTER_ID}"
kubectl wait --for=delete pod --timeout=60s --selector="app=${CLUSTER_ID}"
