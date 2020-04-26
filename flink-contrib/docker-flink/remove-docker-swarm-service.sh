#!/bin/sh

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

usage() {
  cat <<HERE
Usage:
  remove-docker-swarm-service.sh <service-name> 
HERE
  exit 1
}

[[ $# -ne 1 ]] && usage

SERVICE_BASE_NAME="$1"
JOB_MANAGER_NAME=${SERVICE_BASE_NAME}-jobmanager
TASK_MANAGER_NAME=${SERVICE_BASE_NAME}-taskmanager
OVERLAY_NETWORK_NAME=${SERVICE_BASE_NAME}

# Remove taskmanager service
docker service rm ${TASK_MANAGER_NAME}

# Remove jobmanager service
docker service rm ${JOB_MANAGER_NAME}

# Remove overlay network
docker network rm ${OVERLAY_NETWORK_NAME}
