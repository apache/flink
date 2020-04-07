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

###################################
# Starts a docker container of the aws.
#
# To improve performance of s3_get_number_of_lines_by_prefix, one docker container will be reused for several aws
# commands. An interactive python shell keeps the container busy such that it can be reused to issue several commands.
#
# Globals:
#   TEST_INFRA_DIR
# Exports:
#   AWSCLI_CONTAINER_ID
###################################
function aws_cli_start() {
  export AWSCLI_CONTAINER_ID=$(docker run -d \
    --network host \
    --mount type=bind,source="$TEST_INFRA_DIR",target=/hostdir \
    -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    --entrypoint python \
    -it banst/awscli)

  while [[ "$(docker inspect -f {{.State.Running}} "$AWSCLI_CONTAINER_ID")" -ne "true" ]]; do
    sleep 0.1
  done
  on_exit aws_cli_stop
}

###################################
# Stops the docker container of the aws cli.
#
# Globals:
#   AWSCLI_CONTAINER_ID
###################################
function aws_cli_stop() {
  docker kill "$AWSCLI_CONTAINER_ID"
  docker rm "$AWSCLI_CONTAINER_ID"
  export AWSCLI_CONTAINER_ID=
}

# always start it while sourcing, so that AWSCLI_CONTAINER_ID is available from parent script
if [[ $AWSCLI_CONTAINER_ID ]]; then
  aws_cli_stop
fi
aws_cli_start

###################################
# Runs an aws command on the previously started container.
#
# Globals:
#   AWSCLI_CONTAINER_ID
###################################
function aws_cli() {
  local endpoint=""
  if [[ $S3_ENDPOINT ]]; then
    endpoint="--endpoint-url $S3_ENDPOINT"
  fi
  if ! docker exec "$AWSCLI_CONTAINER_ID" aws $endpoint "$@"; then
    echo "Error executing aws command: $@";
    return 1
  fi
}

###################################
# Download s3 objects to folder by full path prefix.
#
# Globals:
#   IT_CASE_S3_BUCKET
#   TEST_INFRA_DIR
# Arguments:
#   $1 - local path to save folder with files
#   $2 - s3 key full path prefix
#   $3 - s3 file name prefix w/o directory to filter files by name (optional)
#   $4 - recursive?
# Returns:
#   None
###################################
function s3_get_by_full_path_and_filename_prefix() {
  local args=
  if [[ $3 ]]; then
    args=" --exclude '*' --include '*/${3}[!/]*'"
  fi
  if [[ "$4" == true ]]; then
    args="$args --recursive"
  fi
  local relative_dir=${1#$TEST_INFRA_DIR}
  aws_cli s3 cp --quiet "s3://$IT_CASE_S3_BUCKET/$2" "/hostdir/${relative_dir}" $args
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
function s3_delete_by_full_path_prefix() {
  aws_cli s3 rm --quiet "s3://$IT_CASE_S3_BUCKET/$1" --recursive
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
#   $2 - s3 file name prefix w/o directory to filter files by name (optional)
# Returns:
#   line number in part files
###################################
function s3_get_number_of_lines_by_prefix() {
  local file_prefix="${2-}"

  # find all files that have the given prefix
  parts=$(aws_cli s3api list-objects --bucket "$IT_CASE_S3_BUCKET" --prefix "$1" |
    docker run -i stedolan/jq -r '[.Contents[].Key] | join(" ")')

  # in parallel (N tasks), query the number of lines, store result in a file named lines
  N=10
  echo "0" >lines
  # turn off job control, so that there is noise when starting/finishing bg tasks
  old_state=$(set +o)
  set +m
  for part in $parts; do
    if [[ $(basename "${part}") == $file_prefix* ]]; then
      ((i = i % N))
      ((i++ == 0)) && wait
      aws_cli s3api select-object-content --bucket "$IT_CASE_S3_BUCKET" --key "$part" \
        --expression "select count(*) from s3object" --expression-type "SQL" \
        --input-serialization='{"CSV": {}}' --output-serialization='{"CSV": {}}' /dev/stdout >>lines &
    fi
  done
  wait
  # restore old settings
  eval "$old_state"
  # add number of lines of each part
  paste -sd+ lines | bc
}
