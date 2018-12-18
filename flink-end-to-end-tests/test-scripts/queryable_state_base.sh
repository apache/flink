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

function link_queryable_state_lib {
    echo "Moving flink-queryable-state-runtime from opt/ to lib/"
    mv ${FLINK_DIR}/opt/flink-queryable-state-runtime* ${FLINK_DIR}/lib/
    if [ $? != 0 ]; then
        echo "Failed to move flink-queryable-state-runtime from opt/ to lib/. Exiting"
        exit 1
    fi
}

function unlink_queryable_state_lib {
    echo "Moving flink-queryable-state-runtime from lib/ to opt/"
    mv ${FLINK_DIR}/lib/flink-queryable-state-runtime* ${FLINK_DIR}/opt/
    if [ $? != 0 ]; then
        echo "Failed to move flink-queryable-state-runtime from lib/ to opt/. Exiting"
        exit 1
    fi
}

# Returns the ip address of the queryable state server
function get_queryable_state_server_ip {
    local ip=$(cat ${FLINK_DIR}/log/flink*taskexecutor*log \
        | grep "Started Queryable State Server" \
        | head -1 \
        | awk '{split($11, a, "/"); split(a[2], b, ":"); print b[1]}')

    printf "${ip} \n"
}

# Returns the ip address of the queryable state server
function get_queryable_state_proxy_port {
    local port=$(cat ${FLINK_DIR}/log/flink*taskexecutor*log \
        | grep "Started Queryable State Proxy Server" \
        | head -1 \
        | awk '{split($12, a, "/"); split(a[2], b, ":"); split(b[2], c, "."); print c[1]}')

    printf "${port} \n"
}
