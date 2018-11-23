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

common_jepsen_args+=(--ha-storage-dir hdfs:///flink
--test-spec "${dockerdir}/data-stream-test-program-parallelism-1.edn"
--tarball ${2}
--ssh-private-key ~/.ssh/id_rsa
--nodes-file ${dockerdir}/nodes)

for i in $(seq 1 ${1})
do
  echo "Executing run #${i} of ${1}"
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-task-managers --deployment-mode yarn-session
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --deployment-mode yarn-session
  lein run test "${common_jepsen_args[@]}" --nemesis-gen fail-name-node-during-recovery --deployment-mode yarn-session

  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-task-managers --deployment-mode yarn-job
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --deployment-mode yarn-job
  lein run test "${common_jepsen_args[@]}" --nemesis-gen fail-name-node-during-recovery --deployment-mode yarn-job

  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-task-managers --deployment-mode mesos-session
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --deployment-mode mesos-session

  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --deployment-mode standalone-session
  lein run test "${common_jepsen_args[@]}" --nemesis-gen kill-job-managers --client-gen cancel-job --deployment-mode standalone-session
  echo
done
