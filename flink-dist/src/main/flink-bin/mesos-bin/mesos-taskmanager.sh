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

# get Flink config
. "$bin"/config.sh

CC_CLASSPATH=`manglePathList $(constructFlinkClassPath):$INTERNAL_HADOOP_CLASSPATHS`

local filename="flink-taskmanager"
log="${filename}.log"
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml"
gclog="${filename}-gc.log"

local heap_dump_file_path="${FLINK_JVM_HEAPDUMP_DIRECTORY}/${filename}.hprof"
local gc_logging_opts=$(getGCLoggingOpts ${gclog})
local crash_on_oom_opts=$(getCrashOnOOMOpts ${heap_dump_file_path})
JVM_ARGS=("${gc_logging_opts[@]}" "${crash_on_oom_opts[@]}" "${JVM_ARGS[@]}")s

# Add precomputed memory JVM options
if [ -z "${FLINK_ENV_JAVA_OPTS_MEM}" ]; then
    FLINK_ENV_JAVA_OPTS_MEM=""
fi
export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_MEM}"

# Add TaskManager-specific JVM options
export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_TM}"

ENTRY_POINT=org.apache.flink.mesos.entrypoint.MesosTaskExecutorRunner

exec $JAVA_RUN $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} -classpath "$CC_CLASSPATH" $log_setting ${ENTRY_POINT} "$@"

