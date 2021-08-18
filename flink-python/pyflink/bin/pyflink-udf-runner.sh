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
python=${python:-python}

if [[ "$FLINK_TESTING" = "1" ]]; then
    ACTUAL_FLINK_HOME=`cd $FLINK_HOME; pwd -P`
    FLINK_SOURCE_ROOT_DIR=`cd $ACTUAL_FLINK_HOME/../../../../; pwd`
    FLINK_PYTHON="${FLINK_SOURCE_ROOT_DIR}/flink-python"
    if [[ -f "${FLINK_PYTHON}/pyflink/fn_execution/boot.py" ]]; then
        # use pyflink source code to override the pyflink.zip in PYTHONPATH
        # to ensure loading latest code
        export PYTHONPATH="$FLINK_PYTHON:$PYTHONPATH"
    fi
fi

if [[ "$_PYTHON_WORKING_DIR" != "" ]]; then
    # set current working directory to $_PYTHON_WORKING_DIR
    cd "$_PYTHON_WORKING_DIR"
    if [[ "$python" == ${_PYTHON_WORKING_DIR}* ]]; then
        # The file extracted from archives may not preserve its original permission.
        # Set minimum execution permission to prevent from permission denied error.
        chmod +x "$python"
    fi
fi

log="$BOOT_LOG_DIR/flink-python-udf-boot.log"
${python} -m pyflink.fn_execution.beam.beam_boot $@ 2>&1 | tee ${log}
