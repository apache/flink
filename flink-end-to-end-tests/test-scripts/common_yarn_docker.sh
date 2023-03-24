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
set -o pipefail

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/common_docker.sh
source "$(dirname "$0")"/common_artifact_download_cacher.sh

FLINK_TARBALL_DIR=${TEST_DATA_DIR}
FLINK_TARBALL=flink.tar.gz
FLINK_DIRNAME=$(basename "${FLINK_DIR}")

MAX_RETRY_SECONDS=120
CLUSTER_SETUP_RETRIES=3
IMAGE_BUILD_RETRIES=5

echo "Flink Tarball directory ${FLINK_TARBALL_DIR}"
echo "Flink tarball filename ${FLINK_TARBALL}"
echo "Flink distribution directory name ${FLINK_DIRNAME}"
echo "End-to-end directory ${END_TO_END_DIR}"

start_time=$(date +%s)

# Make sure we stop our cluster at the end
function cluster_shutdown {
  if [ ${TRAPPED_EXIT_CODE} != 0 ];then
      debug_copy_and_show_logs
  fi
  docker-compose -f "${END_TO_END_DIR}/test-scripts/docker-hadoop-secure-cluster/docker-compose.yml" down
  rm "${FLINK_TARBALL_DIR}/${FLINK_TARBALL}"
}
on_exit cluster_shutdown

function start_hadoop_cluster() {
    echo "Starting Hadoop cluster"
    docker-compose -f "${END_TO_END_DIR}/test-scripts/docker-hadoop-secure-cluster/docker-compose.yml" up -d

    # Wait for kerberos to be set up
    local start_time
    start_time=$(date +%s)
    until docker logs master 2>&1 | grep -q "Finished master initialization"; do
        local current_time
        current_time=$(date +%s)
        local time_diff=$((current_time - start_time))

        if [ ${time_diff} -ge ${MAX_RETRY_SECONDS} ]; then
            return 1
        else
            echo "Waiting for hadoop cluster to come up. We have been trying for ${time_diff} seconds, retrying ..."
            sleep 5
        fi
    done

    # Perform health checks
    containers_health_check "master" "worker1" "worker2" "kdc"

    # Try and see if NodeManagers are up, otherwise the Flink job will not have enough resources to run
    local nm_running="0"
    local start_time
    start_time=$(date +%s)
    while [ "${nm_running}" -lt "2" ]; do
        local current_time
        current_time=$(date +%s)
        local time_diff=$((current_time - start_time))

        if [ ${time_diff} -ge ${MAX_RETRY_SECONDS} ]; then
            return 1
        else
            echo "We only have ${nm_running} NodeManagers up. We have been trying for ${time_diff} seconds, retrying ..."
            sleep 1
        fi

        docker_kinit master
        nm_running=$(docker exec master bash -c "yarn node -list" | grep -c RUNNING)
        docker_kdestroy master
    done

    echo "We now have ${nm_running} NodeManagers up."

    return 0
}

function build_image() {
    echo "Pre-downloading Hadoop tarball"
    local cache_path
    cache_path=$(get_artifact "http://archive.apache.org/dist/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz")
    ln "${cache_path}" "${END_TO_END_DIR}/test-scripts/docker-hadoop-secure-cluster/hadoop/hadoop.tar.gz"

    echo "Building Hadoop Docker container"
    docker-compose -f "${END_TO_END_DIR}/test-scripts/docker-hadoop-secure-cluster/docker-compose.yml" build
}

function start_hadoop_cluster_and_prepare_flink() {
    if ! retry_times ${IMAGE_BUILD_RETRIES} 2 build_image; then
        echo "ERROR: Could not build hadoop image. Aborting..."
        exit 1
    fi
    if ! retry_times $CLUSTER_SETUP_RETRIES 0 start_hadoop_cluster; then
        echo "ERROR: Could not start hadoop cluster. Aborting..."
        exit 1
    fi

    mkdir -p "${FLINK_TARBALL_DIR}"
    tar czf "${FLINK_TARBALL_DIR}/${FLINK_TARBALL}" -C "$(dirname "${FLINK_DIR}")" .

    docker cp "${FLINK_TARBALL_DIR}/${FLINK_TARBALL}" master:/home/hadoop-user/

    # Now, at least the container is ready
    docker exec master bash -c "tar xzf /home/hadoop-user/${FLINK_TARBALL} --directory /home/hadoop-user/"

    # Minimal Flink config, bebe
    FLINK_CONFIG=$(cat << END
security.kerberos.login.keytab: /home/hadoop-user/hadoop-user.keytab
security.kerberos.login.principal: hadoop-user
slot.request.timeout: 120000
END
)
    docker exec master bash -c "echo \"${FLINK_CONFIG}\" > /home/hadoop-user/${FLINK_DIRNAME}/conf/flink-conf.yaml"

    echo "Flink config:"
    docker exec master bash -c "cat /home/hadoop-user/${FLINK_DIRNAME}/conf/flink-conf.yaml"
}

function debug_copy_and_show_logs {
    echo "Debugging failed YARN Docker test:"
    echo -e "\nCurrently running containers"
    docker ps

    echo -e "\nCurrently running JVMs"
    jps -v

    local log_directory="${TEST_DATA_DIR}/logs"
    local yarn_docker_containers
    yarn_docker_containers="master $(docker ps --format '{{.Names}}' | grep worker)"

    extract_hadoop_logs "${log_directory}" "${yarn_docker_containers}"
    print_logs "${log_directory}"

    local yarn_application_logs
    yarn_application_logs=$(get_yarn_application_logs)
    echo -e "\n==== YARN application logs begin ===="
    echo "${yarn_application_logs}"
    echo -e "\n==== YARN application logs end ====\n"
}

function extract_hadoop_logs() {
    local parent_folder="$1"
    shift
    docker_container_aliases="$@"

    for docker_container_alias in $(echo ${docker_container_aliases}); do
        local target_container_log_folder="${parent_folder}/${docker_container_alias}"
        echo "Extracting ${docker_container_alias} Hadoop logs into ${target_container_log_folder}"
        mkdir -p "${target_container_log_folder}"
        docker cp "${docker_container_alias}:/var/log/hadoop/" "${target_container_log_folder}"

        local target_container_docker_log_file="${target_container_log_folder}/docker-${docker_container_alias}.log"
        echo "Extracting ${docker_container_alias} Docker logs into ${target_container_docker_log_file}"
        docker logs "${docker_container_alias}" > "${target_container_docker_log_file}"
    done
}

function print_logs() {
    local parent_folder="$1"

    ls -lisahR "${parent_folder}"
    find "${parent_folder}" -type f -exec echo -e "\nContent of {}:" \; -exec cat {} \;
}

# Expects only one application to be running and waits until this one is in
# final state SUCCEEDED
function wait_for_single_yarn_application {

    application_id=$(get_yarn_application_id)

    docker_kinit master

    # Wait for the application to finish successfully
    start_time=$(date +%s)
    application_state="UNDEFINED"
    while [[ ${application_state} != "FINISHED" ]]; do
        current_time=$(date +%s)
        time_diff=$((current_time - start_time))

        if [[ $time_diff -ge $MAX_RETRY_SECONDS ]]; then
            echo "Application ${application_id} is in state ${application_state} and we have waited too long, quitting..."
            exit 1
        else
            echo "Application ${application_id} is in state ${application_state}. We have been waiting for ${time_diff} seconds, looping ..."
            sleep 1
        fi

        application_state=$(docker exec master bash -c "yarn application -status ${application_id}" | grep "\sState" | sed 's/.*State : \(\w*\)/\1/')
    done

    final_application_state=$(docker exec master bash -c "yarn application -status ${application_id}" | grep "\sFinal-State" | sed 's/.*Final-State : \(\w*\)/\1/')

    echo "Final Application State: ${final_application_state}"

    if [[ ${final_application_state} != "SUCCEEDED" ]]; then
        echo "Running the Flink Application failed. 😞"
        exit 1
    fi

    docker_kdestroy master
}

function get_output {
    echo "Getting output" >&2

    docker_kinit master

    docker exec master bash -c "hdfs dfs -ls -R $1"
    local output
    output=$(docker exec master bash -c "hdfs dfs -cat $1")

    docker_kdestroy master

    echo "${output}"
}

function get_yarn_application_id {
    echo "Getting YARN application id" >&2

    docker_kinit master

    local application_id
    application_id=$(docker exec master bash -c "yarn application -list -appStates ALL" | grep "Flink" | awk '{print $1}')
    echo "YARN application ID: $application_id" >&2

    docker_kdestroy master

    echo "${application_id}"
}

function get_yarn_application_logs {
    echo -e "Getting YARN application logs" >&2

    local application_id
    application_id=$(get_yarn_application_id)

    local logs
    docker_kinit master
    logs=$(docker exec master bash -c "yarn logs -applicationId ${application_id}")
    docker_kdestroy master

    echo "${logs}"
}

function docker_kinit {
    docker exec "$1" bash -c "kinit -kt /home/hadoop-user/hadoop-user.keytab hadoop-user"
}

function docker_kdestroy {
    docker exec "$1" bash -c "kdestroy"
}
