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

# Tests for Azure file system.

# To run single test, export IT_CASE_AZURE_ACCOUNT, IT_CASE_AZURE_ACCESS_KEY, IT_CASE_AZURE_CONTAINER to
# the appropriate values and run:
# flink-end-to-end-tests/run-single-test.sh skip flink-end-to-end-tests/test-scripts/test_azure_fs.sh

source "$(dirname "$0")"/common.sh

if [[ -z "${IT_CASE_AZURE_ACCOUNT:-}" ]]; then
    echo "Did not find Azure storage account environment variable, NOT running the e2e test."
    exit 0
else
    echo "Found Azure storage account $IT_CASE_AZURE_ACCOUNT, running the e2e test."
fi

if [[ -z "${IT_CASE_AZURE_ACCESS_KEY:-}" ]]; then
    echo "Did not find Azure storage access key environment variable, NOT running the e2e test."
    exit 0
else
    echo "Found Azure storage access key $IT_CASE_AZURE_ACCESS_KEY, running the e2e test."
fi

if [[ -z "${IT_CASE_AZURE_CONTAINER:-}" ]]; then
    echo "Did not find Azure storage container environment variable, NOT running the e2e test."
    exit 0
else
    echo "Found Azure storage container $IT_CASE_AZURE_CONTAINER, running the e2e test."
fi

AZURE_TEST_DATA_WORDS_URI="wasbs://$IT_CASE_AZURE_CONTAINER@$IT_CASE_AZURE_ACCOUNT.blob.core.windows.net/words"

###################################
# Setup Flink Azure access.
#
# Globals:
#   FLINK_DIR
#   IT_CASE_AZURE_ACCOUNT
#   IT_CASE_AZURE_ACCESS_KEY
# Returns:
#   None
###################################
function azure_setup {

  echo "Copying flink azure jars and writing out configs"
  add_optional_plugin "azure-fs-hadoop"
  set_config_key "fs.azure.account.key.$IT_CASE_AZURE_ACCOUNT.blob.core.windows.net" "$IT_CASE_AZURE_ACCESS_KEY"
}

azure_setup

echo "Starting Flink cluster.."
start_cluster

$FLINK_DIR/bin/flink run -p 1 $FLINK_DIR/examples/batch/WordCount.jar --input $AZURE_TEST_DATA_WORDS_URI --output $TEST_DATA_DIR/out/wc_out

check_result_hash "WordCountWithAzureFS" $TEST_DATA_DIR/out/wc_out "72a690412be8928ba239c2da967328a5"
