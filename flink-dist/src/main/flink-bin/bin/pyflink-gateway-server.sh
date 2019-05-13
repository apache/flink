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

# =====================================================================
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

FLINK_CLASSPATH=`constructFlinkClassPath`

ARGS=()

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -c|--class)
            DRIVER=$2
            shift
            shift
            ;;
        *)
           ARGS+=("$1")
           shift
           ;;
    esac
done

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-python-$HOSTNAME.log
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j-cli.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml)

TABLE_JAR_PATH=`echo "$FLINK_ROOT_DIR"/opt/flink-table*.jar`

FLINK_TEST_CLASSPATH=""
if [[ -n "$FLINK_TESTING" ]]; then
  bin=`dirname "$0"`
  FLINK_SOURCE_ROOT_DIR=`cd "$bin/../../"; pwd -P`

  while read -d '' -r testJarFile ; do
    if [[ "$FLINK_TEST_CLASSPATH" == "" ]]; then
      FLINK_TEST_CLASSPATH="$testJarFile";
    else
      FLINK_TEST_CLASSPATH="$FLINK_TEST_CLASSPATH":"$testJarFile"
    fi
  done < <(find "$FLINK_SOURCE_ROOT_DIR" ! -type d -name 'flink-*-tests.jar' -print0 | sort -z)
fi

exec $JAVA_RUN $JVM_ARGS "${log_setting[@]}" -cp ${FLINK_CLASSPATH}:${TABLE_JAR_PATH}:${FLINK_TEST_CLASSPATH} ${DRIVER} ${ARGS[@]}
