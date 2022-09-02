#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

function test_module() {
    module="$FLINK_PYTHON_DIR/pyflink/$1"
    echo "test module $module"
    pytest --durations=20 ${module} $2
    if [[ $? -ne 0 ]]; then
        echo "test module $module failed"
        exit 1
    fi
}

function test_all_modules() {
    # test common module
    test_module "common"

    # test datastream module
    test_module "datastream"

    # test fn_execution module
    test_module "fn_execution"

    # test table module
    test_module "table"
}

# CURRENT_DIR is "flink/flink-python/dev/"
CURRENT_DIR="$(cd "$( dirname "$0" )" && pwd)"

# FLINK_PYTHON_DIR is "flink/flink-python"
FLINK_PYTHON_DIR=$(dirname "$CURRENT_DIR")

# FLINK_SOURCE_DIR is "flink"
FLINK_SOURCE_DIR=$(dirname "$FLINK_PYTHON_DIR")

# python test
test_all_modules

# cython test
source "${FLINK_SOURCE_DIR}/tools/azure-pipelines/build_properties.sh"
pr_contains_cython_changes
if [[ "$?" == 0 ]] ; then
    echo "This pull request doesn't contain cython files change. Skipping the cython check."
else
    # compile cython files
    python setup.py build_ext --inplace --force
    test_all_modules
fi
