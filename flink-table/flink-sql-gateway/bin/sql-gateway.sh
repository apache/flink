#!/usr/bin/env bash
#
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
#

function usage() {
  echo "Usage: sql-gateway.sh [start|start-foreground|stop|stop-all] [args]"
  echo "  commands:"
  echo "    start               - Run a SQL Gateway as a daemon"
  echo "    start-foreground    - Run a SQL Gateway as a console application"
  echo "    stop                - Stop the SQL Gateway daemon"
  echo "    stop-all            - Stop all the SQL Gateway daemons"
  echo "    -h | --help         - Show this help message"
}

################################################################################
# Adopted from "flink" bash script
################################################################################

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path
bin=`dirname "$target"`

# get flink config
. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

################################################################################
# SQL gateway specific logic
################################################################################

ENTRYPOINT=sql-gateway

if [[ "$1" = *--help ]] || [[ "$1" = *-h ]]; then
  usage
  exit 0
fi

STARTSTOP=$1

if [ -z "$STARTSTOP" ]; then
  STARTSTOP="start"
fi

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  usage
  exit 1
fi

# ./sql-gateway.sh start --help, print the message to the console
if [[ "$STARTSTOP" = start* ]] && ( [[ "$*" = *--help* ]] || [[ "$*" = *-h* ]] ); then
  FLINK_TM_CLASSPATH=`constructFlinkClassPath`
  SQL_GATEWAY_CLASSPATH=`findSqlGatewayJar`
  "$JAVA_RUN"  -classpath "`manglePathList "$FLINK_TM_CLASSPATH:$SQL_GATEWAY_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.table.gateway.SqlGateway "${@:2}"
  exit 0
fi

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_SQL_GATEWAY}"
fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${@:2}"
else
    "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${@:2}"
fi
