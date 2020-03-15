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

source "$(dirname "$0")"/common.sh

INPUT_TYPE=${1:-file}
RESULT_HASH="72a690412be8928ba239c2da967328a5"
S3_PREFIX=temp/test_batch_wordcount-$(uuidgen)
OUTPUT_PATH="${TEST_DATA_DIR}/out/wc_out"

fetch_complete_result=()

case $INPUT_TYPE in
    (file)
        ARGS="--input ${TEST_INFRA_DIR}/test-data/words --output ${OUTPUT_PATH}"
    ;;
    (hadoop)
        source "$(dirname "$0")"/common_s3.sh
        s3_setup hadoop
        ARGS="--input ${S3_TEST_DATA_WORDS_URI} --output s3://$IT_CASE_S3_BUCKET/$S3_PREFIX"
        OUTPUT_PATH="$TEST_DATA_DIR/$S3_PREFIX"
        on_exit "s3_delete_by_full_path_prefix '$S3_PREFIX'"
        fetch_complete_result=(s3_get_by_full_path_and_filename_prefix "$OUTPUT_PATH" "${S3_PREFIX}" "" false)
    ;;
    (hadoop_minio)
        source "$(dirname "$0")"/common_s3_minio.sh
        s3_setup hadoop
        ARGS="--input ${S3_TEST_DATA_WORDS_URI} --output s3://$IT_CASE_S3_BUCKET/$S3_PREFIX"
        OUTPUT_PATH="$TEST_INFRA_DIR/$IT_CASE_S3_BUCKET/$S3_PREFIX"
    ;;
    (hadoop_with_provider)
        source "$(dirname "$0")"/common_s3.sh
        s3_setup_with_provider hadoop "fs.s3a.aws.credentials.provider"
        ARGS="--input ${S3_TEST_DATA_WORDS_URI} --output s3://$IT_CASE_S3_BUCKET/$S3_PREFIX"
        OUTPUT_PATH="$TEST_DATA_DIR/$S3_PREFIX"
        on_exit "s3_delete_by_full_path_prefix '$S3_PREFIX'"
        fetch_complete_result=(s3_get_by_full_path_and_filename_prefix "$OUTPUT_PATH" "${S3_PREFIX}" "" false)
    ;;
    (presto)
        source "$(dirname "$0")"/common_s3.sh
        s3_setup presto
        ARGS="--input ${S3_TEST_DATA_WORDS_URI} --output s3://$IT_CASE_S3_BUCKET/$S3_PREFIX"
        OUTPUT_PATH="$TEST_DATA_DIR/$S3_PREFIX"
        on_exit "s3_delete_by_full_path_prefix '$S3_PREFIX'"
        fetch_complete_result=(s3_get_by_full_path_and_filename_prefix "$OUTPUT_PATH" "${S3_PREFIX}" "" false)
    ;;
    (presto_minio)
        source "$(dirname "$0")"/common_s3_minio.sh
        s3_setup presto
        ARGS="--input ${S3_TEST_DATA_WORDS_URI} --output s3://$IT_CASE_S3_BUCKET/$S3_PREFIX"
        OUTPUT_PATH="$TEST_INFRA_DIR/$IT_CASE_S3_BUCKET/$S3_PREFIX"
    ;;
    (dummy-fs)
        source "$(dirname "$0")"/common_dummy_fs.sh
        dummy_fs_setup
        ARGS="--input dummy://localhost/words --input anotherDummy://localhost/words --output ${OUTPUT_PATH}"
        RESULT_HASH="0e5bd0a3dd7d5a7110aa85ff70adb54b"
    ;;
    (*)
        echo "Unknown input type $INPUT_TYPE"
        exit 1
    ;;
esac

mkdir -p "$(dirname $OUTPUT_PATH)"
start_cluster

# The test may run against different source types.
# But the sources should provide the same test data, so the checksum stays the same for all tests.
${FLINK_DIR}/bin/flink run -p 1 ${FLINK_DIR}/examples/batch/WordCount.jar ${ARGS}
# Fetches result from AWS s3 to the OUTPUT_PATH, no-op for other filesystems and minio-based tests
${fetch_complete_result[@]}
check_result_hash "WordCount (${INPUT_TYPE})" "${OUTPUT_PATH}" "${RESULT_HASH}"
