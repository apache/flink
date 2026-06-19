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

# Start/stop a ZooKeeper quorum peer.
USAGE="Usage: zookeeper.sh ((start|start-foreground) peer-id)|stop|stop-all"

STARTSTOP=$1
PEER_ID=$2

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

ZK_CONF="$FLINK_CONF_DIR/zoo.cfg"
if [ ! -f "$ZK_CONF" ]; then
    echo "[ERROR] No ZooKeeper configuration file found in '$ZK_CONF'."
    exit 1
fi

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    if [ -z $PEER_ID ]; then
        echo "[ERROR] Missing peer id argument. $USAGE."
        exit 1
    fi

    if [[ ! ${ZK_HEAP} =~ ${IS_NUMBER} ]]; then
        echo "[ERROR] Configured ZooKeeper JVM heap size is not a number. Please set '$KEY_ZK_HEAP_MB' in $FLINK_CONF_FILE."
        exit 1
    fi

    if [ "$ZK_HEAP" -gt 0 ]; then
        export JVM_ARGS="$JVM_ARGS -Xms"$ZK_HEAP"m -Xmx"$ZK_HEAP"m"
    fi

    # Startup parameters
    args=("--zkConfigFile" "${ZK_CONF}" "--peerId" "${PEER_ID}")
fi

# the JMX log4j integration in ZK 3.4 does not work log4j 2
export JVM_ARGS="$JVM_ARGS -Dzookeeper.jmx.log4j.disable=true"

if [[ $STARTSTOP == "start-foreground" ]]; then
    "${FLINK_BIN_DIR}"/flink-console.sh zookeeper "${args[@]}"
else
    "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP zookeeper "${args[@]}"
fi
