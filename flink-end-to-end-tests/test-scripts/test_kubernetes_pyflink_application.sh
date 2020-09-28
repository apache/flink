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

CURRENT_DIR=`cd "$(dirname "$0")" && pwd -P`
CLUSTER_ROLE_BINDING="flink-role-binding-default"
CLUSTER_ID="flink-native-k8s-pyflink-application-1"
PURE_FLINK_IMAGE_NAME="test_kubernetes_application-1"
PYFLINK_IMAGE_NAME="test_kubernetes_pyflink_application"
LOCAL_LOGS_PATH="${TEST_DATA_DIR}/log"

function internal_cleanup {
    kubectl delete deployment ${CLUSTER_ID}
    kubectl delete clusterrolebinding ${CLUSTER_ROLE_BINDING}
}

start_kubernetes

build_image ${PURE_FLINK_IMAGE_NAME}

FLINK_PYTHON_DIR=`cd "${CURRENT_DIR}/../../flink-python" && pwd -P`

CONDA_HOME="${FLINK_PYTHON_DIR}/dev/.conda"

"${FLINK_PYTHON_DIR}/dev/lint-python.sh" -s miniconda

PYTHON_EXEC="${CONDA_HOME}/bin/python"

source "${CONDA_HOME}/bin/activate"

cd "${FLINK_PYTHON_DIR}"

if [[ -d "dist" ]]; then rm -Rf dist; fi

python setup.py sdist

cd dev

rm -rf .conda/pkgs

deactivate

PYFLINK_PACKAGE_FILE=$(basename "${FLINK_PYTHON_DIR}"/dist/apache-flink-*.tar.gz)
echo ${PYFLINK_PACKAGE_FILE}
# Create a new docker image that has python and PyFlink installed.
PYFLINK_DOCKER_DIR="$TEST_DATA_DIR/pyflink_docker"
mkdir -p "$PYFLINK_DOCKER_DIR"
cp "${FLINK_PYTHON_DIR}/dist/${PYFLINK_PACKAGE_FILE}" $PYFLINK_DOCKER_DIR/
if [[ -d "dist" ]]; then rm -Rf dist; fi
cd ${PYFLINK_DOCKER_DIR}
echo "FROM ${PURE_FLINK_IMAGE_NAME}" >> Dockerfile
echo "RUN apt-get update -y && apt-get install -y python3.7 python3-pip python3.7-dev && rm -rf /var/lib/apt/lists/*" >> Dockerfile
echo "RUN ln -s /usr/bin/python3 /usr/bin/python" >> Dockerfile
echo "COPY ${PYFLINK_PACKAGE_FILE} ${PYFLINK_PACKAGE_FILE}" >> Dockerfile
echo "RUN pip3 install ${PYFLINK_PACKAGE_FILE}" >> Dockerfile
echo "RUN rm ${PYFLINK_PACKAGE_FILE}" >> Dockerfile
docker build --no-cache --network="host" -t ${PYFLINK_IMAGE_NAME} .

kubectl create clusterrolebinding ${CLUSTER_ROLE_BINDING} --clusterrole=edit --serviceaccount=default:default --namespace=default

mkdir -p "$LOCAL_LOGS_PATH"

# Set the memory and cpu smaller than default, so that the jobmanager and taskmanager pods could be allocated in minikube.
"$FLINK_DIR"/bin/flink run-application -t kubernetes-application \
    -Dkubernetes.cluster-id=${CLUSTER_ID} \
    -Dkubernetes.container.image=${PYFLINK_IMAGE_NAME} \
    -Djobmanager.memory.process.size=1088m \
    -Dkubernetes.jobmanager.cpu=0.5 \
    -Dkubernetes.taskmanager.cpu=0.5 \
    -Dkubernetes.rest-service.exposed.type=NodePort \
    -pym word_count -pyfs /opt/flink/examples/python/table/batch

kubectl wait --for=condition=Available --timeout=30s deploy/${CLUSTER_ID} || exit 1
jm_pod_name=$(kubectl get pods --selector="app=${CLUSTER_ID},component=jobmanager" -o jsonpath='{..metadata.name}')
wait_rest_endpoint_up_k8s $jm_pod_name

# The Flink cluster will be destroyed immediately once the job finished or failed. So we check jobmanager logs
# instead of checking the result
kubectl logs -f $jm_pod_name >$LOCAL_LOGS_PATH/jobmanager.log
grep -E "Job [A-Za-z0-9]+ reached globally terminal state FINISHED" $LOCAL_LOGS_PATH/jobmanager.log
