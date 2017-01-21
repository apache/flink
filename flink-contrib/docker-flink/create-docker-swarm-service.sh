#!/bin/sh

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

# Create the taskmanger service (scale this out as needed)
docker service create --name ${TASK_MANAGER_NAME} --env JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_RPC_ADDRESS} --network ${OVERLAY_NETWORK_NAME} ${IMAGE_NAME} taskmanager
