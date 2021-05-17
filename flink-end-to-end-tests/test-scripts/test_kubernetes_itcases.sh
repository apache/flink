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

start_kubernetes

# Set the ITCASE_KUBECONFIG environment since it is required to run the ITCases
export ITCASE_KUBECONFIG=~/.kube/config

if [ -z "$DEBUG_FILES_OUTPUT_DIR"] ; then
    export DEBUG_FILES_OUTPUT_DIR="${TEST_DATA_DIR}/log"
fi
LOG4J_PROPERTIES=${END_TO_END_DIR}/../tools/ci/log4j.properties
MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES_OUTPUT_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"

function run_mvn_test {
  local test_class=$1
  run_mvn test $MVN_LOGGING_OPTIONS -Dtest=${test_class}

  EXIT_CODE=$?
  if [ $EXIT_CODE != 0 ]; then
    echo "Failed to run Kubernetes ITCase $test_class"
    exit $EXIT_CODE
  fi
}

cd $END_TO_END_DIR/../flink-kubernetes

# Run the ITCases
run_mvn_test org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClientITCase
run_mvn_test org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElectorITCase
run_mvn_test org.apache.flink.kubernetes.highavailability.KubernetesLeaderElectionAndRetrievalITCase
run_mvn_test org.apache.flink.kubernetes.highavailability.KubernetesStateHandleStoreITCase
run_mvn_test org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityRecoverFromSavepointITCase
