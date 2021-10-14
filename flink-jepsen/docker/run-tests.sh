#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -euo pipefail

dockerdir=$(dirname $0)
dockerdir=$(cd ${dockerdir}; pwd)

cat <<EOF > ${dockerdir}/nodes
n1
n2
n3
EOF

common_jepsen_args+=(
--tarball ${2}
--ssh-private-key ~/.ssh/id_rsa
--nodes-file ${dockerdir}/nodes)

for i in $(seq 1 ${1})
do
  echo "Executing run #${i} of ${1}"

  # YARN session cluster
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-task-managers --test-spec "${dockerdir}/test-specs/yarn-session.edn"
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --test-spec "${dockerdir}/test-specs/yarn-session.edn"
  lein run test "${common_jepsen_args[@]}" --nemesis-gen fail-name-node-during-recovery --test-spec "${dockerdir}/test-specs/yarn-session.edn"

  # YARN per-job cluster
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-task-managers --test-spec "${dockerdir}/test-specs/yarn-job.edn"
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --test-spec "${dockerdir}/test-specs/yarn-job.edn"
  lein run test "${common_jepsen_args[@]}" --nemesis-gen fail-name-node-during-recovery --test-spec "${dockerdir}/test-specs/yarn-job.edn"

  # Standalone
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --test-spec "${dockerdir}/test-specs/standalone-session.edn"
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --client-gen cancel-jobs --test-spec "${dockerdir}/test-specs/standalone-session.edn"

  # Below is a test that uses Flink's exactly-once Kafka producer/consumer.
  # The test submits two jobs:
  #
  #   (1) DataGeneratorJob - Publishes data to a Kafka topic
  #   (2) StateMachineJob  - Consumes data from the same Kafka topic, and validates exactly-once semantics
  #
  # To enable the test, you first need to build the flink-state-machine-kafka job jar,
  # and copy the artifact to flink-jepsen/bin:
  #
  #   git clone https://github.com/igalshilman/flink-state-machine-example
  #   cd flink-state-machine-example
  #   mvn clean package -pl flink-state-machine-kafka/flink-state-machine-kafka -am
  #   cp flink-state-machine-kafka/flink-state-machine-kafka/target/flink-state-machine-kafka-1.0-SNAPSHOT.jar /path/to/flink-jepsen/bin
  #
  # lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-task-managers-bursts --time-limit 60 --test-spec "${dockerdir}/test-specs/standalone-session-kafka.edn" --job-running-healthy-threshold 15

done
