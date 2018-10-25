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

if [[ -z "$ARTIFACTS_AWS_BUCKET" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS bucket $ARTIFACTS_AWS_BUCKET, running the e2e test."
fi

if [[ -z "$ARTIFACTS_AWS_ACCESS_KEY" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS access key $ARTIFACTS_AWS_ACCESS_KEY, running the e2e test."
fi

if [[ -z "$ARTIFACTS_AWS_SECRET_KEY" ]]; then
    echo "Did not find AWS environment variables, NOT running the e2e test."
    exit 0
else
    echo "Found AWS secret key $ARTIFACTS_AWS_SECRET_KEY, running the e2e test."
fi

AWS_REGION="${AWS_REGION:-eu-west-1}"
AWS_ACCESS_KEY=$ARTIFACTS_AWS_ACCESS_KEY
AWS_SECRET_KEY=$ARTIFACTS_AWS_SECRET_KEY

s3util="java -jar ${END_TO_END_DIR}/flink-e2e-test-utils/target/S3UtilProgram.jar"

###################################
# Setup Flink s3 access.
#
# Globals:
#   FLINK_DIR
#   ARTIFACTS_AWS_ACCESS_KEY
#   ARTIFACTS_AWS_SECRET_KEY
# Arguments:
#   None
# Returns:
#   None
###################################
function s3_setup {
  # make sure we delete the file at the end
  function s3_cleanup {
    rm $FLINK_DIR/lib/flink-s3-fs*.jar

    # remove any leftover settings
    sed -i -e 's/s3.access-key: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
    sed -i -e 's/s3.secret-key: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
  }
  trap s3_cleanup EXIT

  cp $FLINK_DIR/opt/flink-s3-fs-hadoop-*.jar $FLINK_DIR/lib/
  echo "s3.access-key: $ARTIFACTS_AWS_ACCESS_KEY" >> "$FLINK_DIR/conf/flink-conf.yaml"
  echo "s3.secret-key: $ARTIFACTS_AWS_SECRET_KEY" >> "$FLINK_DIR/conf/flink-conf.yaml"
}

s3_setup

###################################
# List s3 objects by full path prefix.
#
# Globals:
#   ARTIFACTS_AWS_BUCKET
# Arguments:
#   $1 - s3 full path key prefix
# Returns:
#   List of s3 object keys, separated by newline
###################################
function s3_list {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action listByFullPathPrefix --s3prefix "$1" --bucket $ARTIFACTS_AWS_BUCKET
}

###################################
# Download s3 object.
#
# Globals:
#   ARTIFACTS_AWS_BUCKET
# Arguments:
#   $1 - local path to save file
#   $2 - s3 object key
# Returns:
#   None
###################################
function s3_get {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action downloadFile --localFile "$1" --s3file "$2" --bucket $ARTIFACTS_AWS_BUCKET
}

###################################
# Download s3 objects to folder by full path prefix.
#
# Globals:
#   ARTIFACTS_AWS_BUCKET
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
    --localFolder "$1" --s3prefix "$2" --s3filePrefix "${file_prefix}" --bucket $ARTIFACTS_AWS_BUCKET
}

###################################
# Upload file to s3 object.
#
# Globals:
#   ARTIFACTS_AWS_BUCKET
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
  s3Key=$ARTIFACTS_AWS_ACCESS_KEY
  s3Secret=$ARTIFACTS_AWS_SECRET_KEY
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
  s3Key=$ARTIFACTS_AWS_ACCESS_KEY
  s3Secret=$ARTIFACTS_AWS_SECRET_KEY
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
#   ARTIFACTS_AWS_BUCKET
# Arguments:
#   $1 - s3 key full path prefix
# Returns:
#   None
###################################
function s3_delete_by_full_path_prefix {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action deleteByFullPathPrefix --s3prefix "$1" --bucket $ARTIFACTS_AWS_BUCKET
}

###################################
# Count number of lines in a file of s3 object.
# The lines has to be simple to comply with CSV format
# because SQL is used to query the s3 object.
#
# Globals:
#   ARTIFACTS_AWS_BUCKET
# Arguments:
#   $1 - s3 file object key
#   $2 - s3 bucket
# Returns:
#   None
###################################
function s3_get_number_of_lines_in_file {
  AWS_REGION=$AWS_REGION \
  ${s3util} --action numberOfLinesInFile --s3file "$1" --bucket $ARTIFACTS_AWS_BUCKET
}

###################################
# Count number of lines in files of s3 objects filtered by prefix.
# The lines has to be simple to comply with CSV format
# because SQL is used to query the s3 objects.
#
# Globals:
#   ARTIFACTS_AWS_BUCKET
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
    --s3prefix "$1" --s3filePrefix "${file_prefix}" --bucket $ARTIFACTS_AWS_BUCKET
}
