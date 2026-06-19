#!/usr/bin/env bash
#
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
#

# This script aims to test Materialized Table and Kubernetes integration.

source "$(dirname "$0")"/common_kubernetes.sh

CURRENT_DIR=`cd "$(dirname "$0")" && pwd -P`
CLUSTER_ROLE_BINDING="flink-role-binding-default"
APPLICATION_CLUSTER_ID="flink-native-k8s-sql-mt-application-1"
FLINK_IMAGE_NAME="test_kubernetes_mt_application-1"
LOCAL_LOGS_PATH="${TEST_DATA_DIR}/log"
IMAGE_BUILD_RETRIES=3
IMAGE_BUILD_BACKOFF=2

# copy test-filesystem jar & hadoop plugin
TEST_FILE_SYSTEM_JAR=`ls ${END_TO_END_DIR}/../flink-test-utils-parent/flink-table-filesystem-test-utils/target/flink-table-filesystem-test-utils-*.jar`
cp $TEST_FILE_SYSTEM_JAR ${FLINK_DIR}/lib/
add_optional_plugin "s3-fs-hadoop"

# start kubernetes
start_kubernetes
kubectl create clusterrolebinding ${CLUSTER_ROLE_BINDING} --clusterrole=edit --serviceaccount=default:default --namespace=default

 # build image
if ! retry_times $IMAGE_BUILD_RETRIES $IMAGE_BUILD_BACKOFF "build_image ${FLINK_IMAGE_NAME} $(get_host_machine_address)"; then
 	echo "ERROR: Could not build image. Aborting..."
 	exit 1
fi

# setup materialized table data dir
echo "[INFO] Start S3 env"
source "$(dirname "$0")"/common_s3_minio.sh
s3_setup hadoop
S3_TEST_DATA_WORDS_URI="s3://$IT_CASE_S3_BUCKET/"
MATERIALIZED_TABLE_DATA_DIR="${S3_TEST_DATA_WORDS_URI}/test_materialized_table-$(uuidgen)"

echo "[INFO] Start SQL Gateway"
set_config_key "sql-gateway.endpoint.rest.address" "localhost"
start_sql_gateway

SQL_GATEWAY_REST_PORT=8083

function internal_cleanup {
    kubectl delete deployment ${APPLICATION_CLUSTER_ID}
    kubectl delete clusterrolebinding ${CLUSTER_ROLE_BINDING}
}

function open_session() {
  local session_options=$1

  if [ -z "$session_options" ]; then
    session_options="{}"
  fi

  curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"properties\": $session_options}" \
    "http://localhost:$SQL_GATEWAY_REST_PORT/sessions" | jq -r '.sessionHandle'
}

function configure_session() {
  local session_handle=$1
  local statement=$2

  response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"$statement\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/configure-session)

  if [ "$response" != "{}" ]; then
    echo "Configure session $session_handle $statement failed: $response"
    exit 1
  fi
  echo "Configured session $session_handle $statement successfully"
}

function execute_statement() {
  local session_handle=$1
  local statement=$2

  local response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"$statement\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/v3/sessions/$session_handle/statements)

  local operation_handle=$(echo $response | jq -r '.operationHandle')
  if [ -z "$operation_handle" ] || [ "$operation_handle" == "null" ]; then
    echo "Failed to execute statement: $statement, response: $response"
    exit 1
  fi
  get_operation_result $session_handle $operation_handle
}

function get_operation_result() {
  local session_handle=$1
  local operation_handle=$2

  local fields_array=()
  local next_uri="v3/sessions/$session_handle/operations/$operation_handle/result/0"
  while [ ! -z "$next_uri" ] && [ "$next_uri" != "null" ];
  do
    response=$(curl -s -X GET \
      -H "Content-Type:  \
       application/json" \
      http://localhost:$SQL_GATEWAY_REST_PORT/$next_uri)
    result_type=$(echo $response | jq -r '.resultType')
    result_kind=$(echo $response | jq -r '.resultKind')
    next_uri=$(echo $response | jq -r '.nextResultUri')
    errors=$(echo $response | jq -r '.errors')
    if [ "$errors" != "null" ]; then
      echo "fetch operation $operation_handle failed: $errors"
      exit 1
    fi
    if [ result_kind == "SUCCESS" ]; then
      fields_array+="SUCCESS"
      break;
    fi
    if [ "$result_type" != "NOT_READY" ] && [ "$result_kind" == "SUCCESS_WITH_CONTENT" ]; then
      new_fields=$(echo $response | jq -r '.results.data[].fields')
      fields_array+=$new_fields
    else
      sleep 1
    fi
  done
  echo $fields_array
}

function create_materialized_table_in_continous_mode() {
  local session_handle=$1
  local table_name=$2
  local operation_handle=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"CREATE MATERIALIZED TABLE $table_name \
        PARTITIONED BY (ds) \
        with (\
          'format' = 'json',\
          'sink.rolling-policy.rollover-interval' = '10s',\
          'sink.rolling-policy.check-interval' = '10s'\
        )\
        FRESHNESS = INTERVAL '10' SECOND \
        AS SELECT \
          DATE_FORMAT(\`timestamp\`, 'yyyy-MM-dd') AS ds, \
          \`user\`, \
          \`type\` \
        FROM filesystem_source \/*+ options('source.monitor-interval' = '10s') *\/ \"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/statements | jq -r '.operationHandle')
  get_operation_result $session_handle $operation_handle
}

function create_filesystem_source() {
  local session_handle=$1
  local table_name=$2
  local path=$3

  create_source_result=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"CREATE TABLE $table_name (\
      \`timestamp\` TIMESTAMP_LTZ(3),\
      \`user\` STRING,\
      \`type\` STRING\
    ) WITH (\
      'format' = 'csv'\
    )\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/statements/)

    echo $create_source_result
}

S3_ENDPOINT=${S3_ENDPOINT//localhost/$(get_host_machine_address)}
echo "[INFO] Create a new session"
session_options="{\"table.catalog-store.kind\": \"file\",
                 \"table.catalog-store.file.path\": \"$MATERIALIZED_TABLE_DATA_DIR/\",
                 \"execution.target\": \"kubernetes-application\",
                 \"kubernetes.cluster-id\": \"${APPLICATION_CLUSTER_ID}\",
                 \"kubernetes.container.image.ref\": \"${FLINK_IMAGE_NAME}\",
                 \"jobmanager.memory.process.size\": \"1088m\",
                 \"taskmanager.memory.process.size\": \"1000m\",
                 \"kubernetes.jobmanager.cpu\": \"0.5\",
                 \"kubernetes.taskmanager.cpu\": \"0.5\",
                 \"kubernetes.rest-service.exposed.type\": \"NodePort\",
                 \"s3.endpoint\": \"$S3_ENDPOINT\",
                 \"workflow-scheduler.type\": \"embedded\"}"

session_handle=$(open_session "$session_options")
echo "[INFO] Session Handle $session_handle"

# prepare catalog & database
echo "[INFO] Create catalog & database"
configure_session "$session_handle" "create catalog if not exists test_catalog with (\
                                                'type' = 'test-filesystem',\
                                                'default-database' = 'test_db',\
                                                'path' = '$MATERIALIZED_TABLE_DATA_DIR/'\
                                    )"
configure_session $session_handle "use catalog test_catalog"
configure_session $session_handle "create database if not exists test_db"
create_filesystem_source $session_handle "filesystem_source"

# 1. create materialized table in continuous mode
echo "[INFO] Create Materialized table in continuous mode"
create_materialized_table_in_continous_mode $session_handle "my_materialized_table_in_continuous_mode"
echo "[INFO] Wait deployment ${APPLICATION_CLUSTER_ID} Available"
kubectl wait --for=condition=Available --timeout=30s deploy/${APPLICATION_CLUSTER_ID} || exit 1
jm_pod_name=$(kubectl get pods --selector="app=${APPLICATION_CLUSTER_ID},component=jobmanager" -o jsonpath='{..metadata.name}')
echo "[INFO] Wait first checkpoint finished"
wait_num_checkpoints $jm_pod_name 1

# 2. suspend & resume materialized table in continuous mode
echo "[INFO] Suspend materialized table"
configure_session $session_handle "set 'execution.checkpointing.savepoint-dir' = '$MATERIALIZED_TABLE_DATA_DIR/savepoint'"
execute_statement $session_handle "alter materialized table my_materialized_table_in_continuous_mode suspend"

kubectl wait --for=delete deployment/$APPLICATION_CLUSTER_ID

echo "[INFO] Resume materialized table"
execute_statement $session_handle "alter materialized table my_materialized_table_in_continuous_mode resume"
kubectl wait --for=condition=Available --timeout=30s deploy/${APPLICATION_CLUSTER_ID} || exit 1
jm_pod_name=$(kubectl get pods --selector="app=${APPLICATION_CLUSTER_ID},component=jobmanager" -o jsonpath='{..metadata.name}')
echo "[INFO] Wait resumed job finished the first checkpoint"
wait_num_checkpoints $jm_pod_name 1

# 3. verify resumed continuous job is restore from savepoint
mkdir -p "$LOCAL_LOGS_PATH"
kubectl logs $jm_pod_name > $LOCAL_LOGS_PATH/jobmanager.log
grep -E "Starting job [A-Za-z0-9]+ from savepoint" $LOCAL_LOGS_PATH/jobmanager.log
