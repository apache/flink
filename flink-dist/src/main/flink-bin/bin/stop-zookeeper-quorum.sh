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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Stops a ZooKeeper quorum as configured in $FLINK_CONF/zoo.cfg

ZK_CONF="$FLINK_CONF_DIR/zoo.cfg"
if [ ! -f "$ZK_CONF" ]; then
    echo "[ERROR] No ZooKeeper configuration file found in '$ZK_CONF'."
    exit 1
fi

# Extract server.X from ZooKeeper config and stop instances
while read server ; do
    server=$(echo -e "${server}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//') # trim

    # match server.id=address[:port[:port]]
    if [[ $server =~ ^server\.([0-9]+)[[:space:]]*\=[[:space:]]*([^: \#]+) ]]; then
        id=${BASH_REMATCH[1]}
        server=${BASH_REMATCH[2]}

        ssh -n $FLINK_SSH_OPTS $server -- "nohup /bin/bash -l $FLINK_BIN_DIR/zookeeper.sh stop &"
    else
        echo "[WARN] Parse error. Skipping config entry '$server'."
    fi
done < <(grep "^server\." "$ZK_CONF")
