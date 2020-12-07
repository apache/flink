#!/bin/bash
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

FLINK_CLASSPATH=`constructFlinkClassPath`
PYTHON_JAR_PATH=`echo "$FLINK_OPT_DIR"/flink-python*.jar`


PYFLINK_PYTHON="${PYFLINK_PYTHON:-"python"}"

# So that python can find out Flink's Jars
export FLINK_BIN_DIR=$FLINK_BIN_DIR
export FLINK_HOME

# Add pyflink & py4j & cloudpickle to PYTHONPATH
export PYTHONPATH="$FLINK_OPT_DIR/python/pyflink.zip:$PYTHONPATH"
PY4J_ZIP=`echo "$FLINK_OPT_DIR"/python/py4j-*-src.zip`
CLOUDPICKLE_ZIP=`echo "$FLINK_OPT_DIR"/python/cloudpickle-*-src.zip`
export PYTHONPATH="$PY4J_ZIP:$CLOUDPICKLE_ZIP:$PYTHONPATH"

PARSER="org.apache.flink.client.python.PythonShellParser"
function parse_options() {
    ${JAVA_RUN} ${JVM_ARGS} -cp ${FLINK_CLASSPATH}:${PYTHON_JAR_PATH} ${PARSER} "$@"
    printf "%d\0" $?
}

# Turn off posix mode since it does not allow process substitution
set +o posix
# If the command has option --help | -h, the script will directly
# run the PythonShellParser program to stdout the help message.
if [[ "$@" =~ '--help' ]] || [[ "$@" =~ '-h' ]]; then
    ${JAVA_RUN} ${JVM_ARGS} -cp ${FLINK_CLASSPATH}:${PYTHON_JAR_PATH} ${PARSER} "$@"
    exit 0
fi
OPTIONS=()
while IFS= read -d '' -r ARG; do
  OPTIONS+=("$ARG")
done < <(parse_options "$@")

COUNT=${#OPTIONS[@]}
LAST=$((COUNT - 1))
LAUNCHER_EXIT_CODE=${OPTIONS[$LAST]}

# Certain JVM failures result in errors being printed to stdout (instead of stderr), which causes
# the code that parses the output of the launcher to get confused. In those cases, check if the
# exit code is an integer, and if it's not, handle it as a special error case.
if ! [[ ${LAUNCHER_EXIT_CODE} =~ ^[0-9]+$ ]]; then
  echo "${OPTIONS[@]}" | head -n-1 1>&2
  exit 1
fi

if [[ ${LAUNCHER_EXIT_CODE} != 0 ]]; then
  exit ${LAUNCHER_EXIT_CODE}
fi

OPTIONS=("${OPTIONS[@]:0:$LAST}")

export SUBMIT_ARGS=${OPTIONS[@]}

# -i: interactive
# -m: execute shell.py in the zip package
${PYFLINK_PYTHON} -i -m pyflink.shell
