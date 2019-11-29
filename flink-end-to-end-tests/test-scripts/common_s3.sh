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

if [[ -z "$IT_CASE_S3_BUCKET" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS bucket $IT_CASE_S3_BUCKET, running the e2e test."
fi

if [[ -z "$IT_CASE_S3_ACCESS_KEY" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS access key, running the e2e test."
fi

if [[ -z "$IT_CASE_S3_SECRET_KEY" ]]; then
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

s3util="java -jar ${END_TO_END_DIR}/flink-e2e-test-utils/target/S3UtilProgram.jar"

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

###################################
# Download s3 objects to folder by full path prefix.
#
# Globals:
#   IT_CASE_S3_BUCKET
# Arguments:
#   $1 - local path to save folder with files
#   $2 - s3 key full path prefix
#   $3 - s3 file name prefix w/o directory to filter files by name (optional)
# Returns:
#   None
###################################
function s3_get_by_full_path_and_filename_prefix {
  local file_prefix="${3-}"
  AWS_REGION=$AWS_REGION \
  ${s3util} --action downloadByFullPathAndFileNamePrefix \
    --localFolder "$1" --s3prefix "$2" --s3filePrefix "${file_prefix}" --bucket $IT_CASE_S3_BUCKET
}

###################################
# Delete s3 objects by full path prefix.
#
# Globals:
#   IT_CASE_S3_BUCKET
# Arguments:
#   $1 - s3 key full path prefix
# Returns:
#   None
###################################
function s3_delete_by_full_path_prefix {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action deleteByFullPathPrefix --s3prefix "$1" --bucket $IT_CASE_S3_BUCKET
}

###################################
# Count number of lines in files of s3 objects filtered by prefix.
# The lines has to be simple to comply with CSV format
# because SQL is used to query the s3 objects.
#
# Globals:
#   IT_CASE_S3_BUCKET
# Arguments:
#   $1 - s3 key prefix
#   $2 - s3 bucket
#   $3 - s3 file name prefix w/o directory to filter files by name (optional)
# Returns:
#   None
###################################
function s3_get_number_of_lines_by_prefix {
  local file_prefix="${3-}"
  AWS_REGION=$AWS_REGION \
  ${s3util} --action numberOfLinesInFilesWithFullAndNamePrefix \
    --s3prefix "$1" --s3filePrefix "${file_prefix}" --bucket $IT_CASE_S3_BUCKET
}
