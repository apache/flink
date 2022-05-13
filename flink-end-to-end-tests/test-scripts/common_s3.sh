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

if [[ $S3_SOURCED ]]; then
  echo "Only source common_s3.sh or common_s3_minio.sh in the same test, previously sourced $S3_SOURCED" && exit 1
fi
export S3_SOURCED="common_s3.sh"

if [[ -z "${IT_CASE_S3_BUCKET:-}" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS bucket $IT_CASE_S3_BUCKET, running the e2e test."
fi

if [[ -z "${IT_CASE_S3_ACCESS_KEY:-}" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS access key, running the e2e test."
fi

if [[ -z "${IT_CASE_S3_SECRET_KEY:-}" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS secret key, running the e2e test."
fi

# export credentials into environment variables for AWS client
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="$IT_CASE_S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$IT_CASE_S3_SECRET_KEY"

AWS_ACCESS_KEY=$IT_CASE_S3_ACCESS_KEY
AWS_SECRET_KEY=$IT_CASE_S3_SECRET_KEY

S3_TEST_DATA_WORDS_URI="s3://$IT_CASE_S3_BUCKET/static/words"

###################################
# Setup Flink s3 access.
#
# Globals:
#   FLINK_DIR
#   IT_CASE_S3_ACCESS_KEY
#   IT_CASE_S3_SECRET_KEY
# Arguments:
#   $1 - s3 filesystem type (hadoop/presto)
# Returns:
#   None
###################################
function s3_setup {
  add_optional_plugin "s3-fs-$1"
  set_config_key "s3.access-key" "$IT_CASE_S3_ACCESS_KEY"
  set_config_key "s3.secret-key" "$IT_CASE_S3_SECRET_KEY"
}

function s3_setup_with_provider {
  add_optional_plugin "s3-fs-$1"
  # reads (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
  set_config_key "$2" "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
}

source "$(dirname "$0")"/common_s3_operations.sh
