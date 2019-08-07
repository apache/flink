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
# List s3 objects by full path prefix.
#
# Globals:
#   IT_CASE_S3_BUCKET
# Arguments:
#   $1 - s3 full path key prefix
# Returns:
#   List of s3 object keys, separated by newline
###################################
function s3_list {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action listByFullPathPrefix --s3prefix "$1" --bucket $IT_CASE_S3_BUCKET
}

###################################
# Download s3 object.
#
# Globals:
#   IT_CASE_S3_BUCKET
# Arguments:
#   $1 - local path to save file
#   $2 - s3 object key
# Returns:
#   None
###################################
function s3_get {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action downloadFile --localFile "$1" --s3file "$2" --bucket $IT_CASE_S3_BUCKET
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
# Upload file to s3 object.
#
# Globals:
#   IT_CASE_S3_BUCKET
# Arguments:
#   $1 - local file to upload
#   $2 - s3 bucket
#   $3 - s3 object key
# Returns:
#   None
###################################
function s3_put {
  local_file=$1
  bucket=$2
  s3_file=$3
  resource="/${bucket}/${s3_file}"
  contentType="application/octet-stream"
  dateValue=`date -R`
  stringToSign="PUT\n\n${contentType}\n${dateValue}\n${resource}"
  s3Key=$IT_CASE_S3_ACCESS_KEY
  s3Secret=$IT_CASE_S3_SECRET_KEY
  signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`
  curl -X PUT -T "${local_file}" \
    -H "Host: ${bucket}.s3.amazonaws.com" \
    -H "Date: ${dateValue}" \
    -H "Content-Type: ${contentType}" \
    -H "Authorization: AWS ${s3Key}:${signature}" \
    https://${bucket}.s3.amazonaws.com/${s3_file}
}

###################################
# Delete s3 object.
#
# Globals:
#   None
# Arguments:
#   $1 - s3 bucket
#   $2 - s3 object key
#   $3 - (optional) s3 host suffix
# Returns:
#   None
###################################
function s3_delete {
  bucket=$1
  s3_file=$2
  resource="/${bucket}/${s3_file}"
  contentType="application/octet-stream"
  dateValue=`date -R`
  stringToSign="DELETE\n\n${contentType}\n${dateValue}\n${resource}"
  s3Key=$IT_CASE_S3_ACCESS_KEY
  s3Secret=$IT_CASE_S3_SECRET_KEY
  signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`
  curl -X DELETE \
    -H "Host: ${bucket}.s3.amazonaws.com" \
    -H "Date: ${dateValue}" \
    -H "Content-Type: ${contentType}" \
    -H "Authorization: AWS ${s3Key}:${signature}" \
    https://${bucket}.s3.amazonaws.com/${s3_file}
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
# Count number of lines in a file of s3 object.
# The lines has to be simple to comply with CSV format
# because SQL is used to query the s3 object.
#
# Globals:
#   IT_CASE_S3_BUCKET
# Arguments:
#   $1 - s3 file object key
#   $2 - s3 bucket
# Returns:
#   None
###################################
function s3_get_number_of_lines_in_file {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action numberOfLinesInFile --s3file "$1" --bucket $IT_CASE_S3_BUCKET
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
