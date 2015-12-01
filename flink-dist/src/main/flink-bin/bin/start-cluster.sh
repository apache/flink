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

# Start a Flink cluster in batch or streaming mode
USAGE="Usage: start-cluster.sh [batch|streaming]"

STREAMING_MODE=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Start the JobManager instance(s)
shopt -s nocasematch
if [[ $RECOVERY_MODE == "zookeeper" ]]; then
    # HA Mode
    readMasters

    echo "Starting HA cluster with ${#MASTERS[@]} masters and ${#ZK_QUORUM[@]} peers in ZooKeeper quorum."

    for ((i=0;i<${#MASTERS[@]};++i)); do
        master=${MASTERS[i]}
        webuiport=${WEBUIPORTS[i]}
        ssh -n $FLINK_SSH_OPTS $master -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/jobmanager.sh\" start cluster ${master} ${webuiport} &"
    done

else
    echo "Starting cluster."

    # Start single JobManager on this machine
    "$FLINK_BIN_DIR"/jobmanager.sh start cluster
fi
shopt -u nocasematch

# Start TaskManager instance(s)
readSlaves

for slave in ${SLAVES[@]}; do
    ssh -n $FLINK_SSH_OPTS $slave -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/taskmanager.sh\" start &"
done
