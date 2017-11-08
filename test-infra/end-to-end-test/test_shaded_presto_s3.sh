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
    echo "Did not find AWS environment variables, NOT running Shaded Presto S3 e2e tests."
    exit 0
else
    echo "Found AWS bucket $ARTIFACTS_AWS_BUCKET, running Shaded Presto S3 e2e tests."
fi

# Tests for our shaded/bundled Hadoop S3A file system.

set -e
set -o pipefail

# Convert relative path to absolute path
TEST_ROOT=`pwd`
TEST_INFRA_DIR="$0"
TEST_INFRA_DIR=`dirname "$TEST_INFRA_DIR"`
cd $TEST_INFRA_DIR
TEST_INFRA_DIR=`pwd`
cd $TEST_ROOT

. "$TEST_INFRA_DIR"/common.sh

s3_put $TEST_INFRA_DIR/test-data/words $ARTIFACTS_AWS_BUCKET flink-end-to-end-test-shaded-presto-s3

cp $FLINK_DIR/opt/flink-s3-fs-presto-*.jar $FLINK_DIR/lib/
echo "s3.access-key: $ARTIFACTS_AWS_ACCESS_KEY" >> "$FLINK_DIR/conf/flink-conf.yaml"
echo "s3.secret-key: $ARTIFACTS_AWS_SECRET_KEY" >> "$FLINK_DIR/conf/flink-conf.yaml"

start_cluster

$FLINK_DIR/bin/flink run -p 1 $FLINK_DIR/examples/batch/WordCount.jar --input s3:/$resource --output $TEST_DATA_DIR/out/wc_out

check_result_hash "WordCountWithShadedPrestoS3" $TEST_DATA_DIR/out/wc_out "72a690412be8928ba239c2da967328a5"

# remove any leftover settings
sed -i -e 's/s3.access-key: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
sed -i -e 's/s3.secret-key: .*//' "$FLINK_DIR/conf/flink-conf.yaml"

rm $FLINK_DIR/lib/flink-s3-fs*.jar

s3_delete $ARTIFACTS_AWS_BUCKET flink-end-to-end-test-shaded-presto-s3

stop_cluster
clean_data_dir
check_all_pass
