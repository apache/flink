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

# Tests for our shaded/bundled Hadoop S3A file system.

if [[ -z "$ARTIFACTS_AWS_BUCKET" ]]; then
    echo "Did not find AWS environment variables, NOT running Shaded Hadoop S3A e2e tests."
    exit 0
else
    echo "Found AWS bucket $ARTIFACTS_AWS_BUCKET, running Shaded Hadoop S3A e2e tests."
fi

source "$(dirname "$0")"/common.sh

s3_put $TEST_INFRA_DIR/test-data/words $ARTIFACTS_AWS_BUCKET flink-end-to-end-test-shaded-s3a
# make sure we delete the file at the end
function s3_cleanup {
  s3_delete $ARTIFACTS_AWS_BUCKET flink-end-to-end-test-shaded-s3a
  rm $FLINK_DIR/lib/flink-s3-fs*.jar

  # remove any leftover settings
  sed -i -e 's/s3.access-key: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
  sed -i -e 's/s3.secret-key: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
}
trap s3_cleanup EXIT

cp $FLINK_DIR/opt/flink-s3-fs-hadoop-*.jar $FLINK_DIR/lib/
echo "s3.access-key: $ARTIFACTS_AWS_ACCESS_KEY" >> "$FLINK_DIR/conf/flink-conf.yaml"
echo "s3.secret-key: $ARTIFACTS_AWS_SECRET_KEY" >> "$FLINK_DIR/conf/flink-conf.yaml"

start_cluster

$FLINK_DIR/bin/flink run -p 1 $FLINK_DIR/examples/batch/WordCount.jar --input s3:/$resource --output $TEST_DATA_DIR/out/wc_out

check_result_hash "WordCountWithShadedS3A" $TEST_DATA_DIR/out/wc_out "72a690412be8928ba239c2da967328a5"