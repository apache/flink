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

start_cluster

$FLINK_DIR/bin/pyflink.sh $FLINK_DIR/examples/python/batch/WordCount.py - $TEST_INFRA_DIR/test-data/words $TEST_DATA_DIR/out/py_wc_out
check_result_hash "BatchPythonWordCount" $TEST_DATA_DIR/out/py_wc_out "dd9d7a7bbc8b52747c7d4e15c9d2b069"
