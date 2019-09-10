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
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/find-flink-home.sh

_FLINK_HOME_DETERMINED=1

. "$FLINK_HOME"/bin/config.sh

if [[ "$FLINK_IDENT_STRING" = "" ]]; then
    FLINK_IDENT_STRING="$USER"
fi

if [[ "$python" = "" ]]; then
    python="python"
fi

# Add pyflink & py4j to PYTHONPATH
PYFLINK_ZIP="$FLINK_OPT_DIR/python/pyflink.zip"
if [[ ! ${PYTHONPATH} =~ ${PYFLINK_ZIP} ]]; then
    export PYTHONPATH="$PYFLINK_ZIP:$PYTHONPATH"
fi
PY4J_ZIP=`echo "$FLINK_OPT_DIR"/python/py4j-*-src.zip`
if [[ ! ${PYTHONPATH} =~ ${PY4J_ZIP} ]]; then
    export PYTHONPATH="$PY4J_ZIP:$PYTHONPATH"
fi

log="$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-python-udf-boot-$HOSTNAME.log"

${python} -m pyflink.fn_execution.boot $@ 2>&1 | tee -a ${log}
