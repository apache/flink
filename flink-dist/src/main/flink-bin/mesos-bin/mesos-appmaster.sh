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

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

CC_CLASSPATH=`manglePathList $(constructFlinkClassPath):$INTERNAL_HADOOP_CLASSPATHS`

log="${FLINK_LOG_DIR}/flink-${FLINK_IDENT_STRING}-mesos-appmaster-${HOSTNAME}.log"
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml"

export FLINK_CONF_DIR
export FLINK_BIN_DIR
export FLINK_LIB_DIR

ENTRY_POINT=org.apache.flink.mesos.runtime.clusterframework.MesosApplicationMasterRunner

if [[ "${FLINK_MODE}" == "new" ]]; then
    ENTRY_POINT=org.apache.flink.mesos.entrypoint.MesosSessionClusterEntrypoint
fi

exec $JAVA_RUN $JVM_ARGS -classpath "$CC_CLASSPATH" $log_setting ${ENTRY_POINT} "$@"

rc=$?

if [[ $rc -ne 0 ]]; then
    echo "Error while starting the mesos application master. Please check ${log} for more details."
fi

exit $rc
