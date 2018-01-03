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
  create-docker-swarm-service.sh [--image-name <image>] <service-name> <service-port>

  If the --image-name flag is not used the service will use the 'flink' image.
HERE
  exit 1
}

if [ "$1" == "--image-name" ]; then
  IMAGE_NAME="$2"
  shift; shift
else
  IMAGE_NAME=flink
fi

[[ $# -ne 2 ]] && usage

SERVICE_BASE_NAME="$1"
SERVICE_PORT="${2}"
JOB_MANAGER_NAME=${SERVICE_BASE_NAME}-jobmanager
TASK_MANAGER_NAME=${SERVICE_BASE_NAME}-taskmanager
JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_NAME}
OVERLAY_NETWORK_NAME=${SERVICE_BASE_NAME}

# Create overlay network
docker network create -d overlay ${OVERLAY_NETWORK_NAME}

# Create the jobmanager service
docker service create --name ${JOB_MANAGER_NAME} --env JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_RPC_ADDRESS} -p ${SERVICE_PORT}:8081 --network ${OVERLAY_NETWORK_NAME} ${IMAGE_NAME} jobmanager

# Create the taskmanager service (scale this out as needed)
docker service create --name ${TASK_MANAGER_NAME} --env JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_RPC_ADDRESS} --network ${OVERLAY_NETWORK_NAME} ${IMAGE_NAME} taskmanager
