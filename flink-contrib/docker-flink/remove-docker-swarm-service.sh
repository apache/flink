#!/bin/sh

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
