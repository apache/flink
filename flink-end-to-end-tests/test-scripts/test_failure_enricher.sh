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

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-failure-enricher-test/target/FailureEnricherTest.jar

echo "Moving the custom failure enricher to plugins/failure-enricher package."

mkdir ${FLINK_DIR}/plugins/failure-enricher
cp $TEST_PROGRAM_JAR ${FLINK_DIR}/plugins/failure-enricher/

set_config_key "jobmanager.failure-enrichers" "org.apache.flink.runtime.enricher.CustomTestFailureEnricher"

echo "Testing FailureEnricher function."

start_cluster

echo "Submitting job."

CLIENT_OUTPUT=$($FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --output $TEST_DATA_DIR/out/cl_out_pf)

# first need get the jobid
JOB_ID=$(echo "${CLIENT_OUTPUT}" | grep "Job has been submitted with JobID" | sed 's/.* //g')

if [[ -z $JOB_ID ]]; then
  echo "ERROR: Job could not be submitted."
  echo "${CLIENT_OUTPUT}"
  exit 1
fi
wait_job_terminal_state "${JOB_ID}" "FAILED"

# then call the restful api get the exceptions
exceptions_json=$(get_job_exceptions ${JOB_ID})
if [[ -z ${exceptions_json} ]]; then
  echo "ERROR: Could not get exceptions of ${jobid}."
  echo "${CLIENT_OUTPUT}"
  exit 1
fi

failure_labels=$(echo $exceptions_json | grep -o '"failureLabels":{[^}]*}' | sed 's/"failureLabels":{\([^}]*\)}/\1/')
if [[ -z ${failure_labels} ]]; then
  echo "ERROR: Could not get the failure labels from the exceptions(${exceptions_json})."
  exit 1
fi

# verify the exception label info
except_labels='"type":"user"'

if [[ $failure_labels == *"$except_labels"* ]]; then
  echo "The test is as expected, failure_labels(${failure_labels}) contains the expected label information."
else
  echo "ERROR: The failure_labels(${failure_labels}) does not contain the expected label information(${except_labels})."
  exit 1
fi

# stop the cluster
function cleanup {
    stop_cluster
}
on_exit cleanup
