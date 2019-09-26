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

CURRENT_DIR=`pwd -P`
FLINK_SOURCE_ROOT_DIR=`cd $bin/../../../../../; pwd -P`
cd $CURRENT_DIR

# Add pyflink to PYTHONPATH
FLINK_PYTHON="${FLINK_SOURCE_ROOT_DIR}/flink-python"
if [[ ! -f "${FLINK_PYTHON}/pyflink/fn_execution/boot.py" ]]; then
  # Add pyflink.zip to PYTHONPATH if directory pyflink does not exist
  PYFLINK_ZIP="$FLINK_OPT_DIR/python/pyflink.zip"
  if [[ ! ${PYTHONPATH} =~ ${PYFLINK_ZIP} ]]; then
    export PYTHONPATH="$PYFLINK_ZIP:$PYTHONPATH"
  fi
else
  # Add directory flink-python/pyflink to PYTHONPATH if it exists, this is helpful during
  # development as this script is used to start up the Python worker and putting the
  # directory of flink-python/pyflink to PYTHONPATH makes sure the Python source code will
  # take effect immediately after changed.
  export PYTHONPATH="$FLINK_PYTHON:$PYTHONPATH"
fi

# Add py4j to PYTHONPATH
PY4J_ZIP=`echo "$FLINK_OPT_DIR"/python/py4j-*-src.zip`
if [[ ! ${PYTHONPATH} =~ ${PY4J_ZIP} ]]; then
    export PYTHONPATH="$PY4J_ZIP:$PYTHONPATH"
fi

# Add cloudpickle to PYTHONPATH
CLOUDPICKLE_ZIP=`echo "$FLINK_OPT_DIR"/python/cloudpickle-*-src.zip`
if [[ ! ${PYTHONPATH} =~ ${CLOUDPICKLE_ZIP} ]]; then
    export PYTHONPATH="$CLOUDPICKLE_ZIP:$PYTHONPATH"
fi

log="$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-python-udf-boot-$HOSTNAME.log"

${python} -m pyflink.fn_execution.boot $@ 2>&1 | tee -a ${log}
