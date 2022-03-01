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

# Start a Flink service as a console application. Must be stopped with Ctrl-C
# or with SIGTERM by kill or the controlling process.
USAGE="Usage: flink-console.sh (taskexecutor|zookeeper|historyserver|standalonesession|standalonejob|kubernetes-session|kubernetes-application|kubernetes-taskmanager) [args]"

SERVICE=$1
ARGS=("${@:2}") # get remaining arguments as array

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

case $SERVICE in
    (taskexecutor)
        CLASS_TO_RUN=org.apache.flink.runtime.taskexecutor.TaskManagerRunner
    ;;

    (historyserver)
        CLASS_TO_RUN=org.apache.flink.runtime.webmonitor.history.HistoryServer
    ;;

    (zookeeper)
        CLASS_TO_RUN=org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer
    ;;

    (standalonesession)
        CLASS_TO_RUN=org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
    ;;

    (standalonejob)
        CLASS_TO_RUN=org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint
    ;;

    (kubernetes-session)
        CLASS_TO_RUN=org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint
    ;;

    (kubernetes-application)
        CLASS_TO_RUN=org.apache.flink.kubernetes.entrypoint.KubernetesApplicationClusterEntrypoint
    ;;

    (kubernetes-taskmanager)
        CLASS_TO_RUN=org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner
    ;;

    (*)
        echo "Unknown service '${SERVICE}'. $USAGE."
        exit 1
    ;;
esac

FLINK_TM_CLASSPATH=`constructFlinkClassPath`

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-$SERVICE.pid
mkdir -p "$FLINK_PID_DIR"
# The lock needs to be released after use because this script is started foreground
command -v flock >/dev/null 2>&1
flock_exist=$?
if [[ ${flock_exist} -eq 0 ]]; then
    exec 200<"$FLINK_PID_DIR"
    flock 200
fi
# Remove the pid file when all the processes are dead
if [ -f "$pid" ]; then
    all_dead=0
    while read each_pid; do
        # Check whether the process is still running
        kill -0 $each_pid > /dev/null 2>&1
        [[ $? -eq 0 ]] && all_dead=1
    done < "$pid"
    [ ${all_dead} -eq 0 ] && rm $pid
fi
id=$([ -f "$pid" ] && echo $(wc -l < "$pid") || echo "0")

FLINK_LOG_PREFIX="${FLINK_LOG_DIR}/flink-${FLINK_IDENT_STRING}-${SERVICE}-${id}-${HOSTNAME}"
log="${FLINK_LOG_PREFIX}.log"

log_setting=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${FLINK_CONF_DIR}/log4j-console.properties" "-Dlog4j.configurationFile=file:${FLINK_CONF_DIR}/log4j-console.properties" "-Dlogback.configurationFile=file:${FLINK_CONF_DIR}/logback-console.xml")

echo "Starting $SERVICE as a console application on host $HOSTNAME."

# Add the current process id to pid file
echo $$ >> "$pid" 2>/dev/null

# Release the lock because the java process runs in the foreground and would block other processes from modifying the pid file
[[ ${flock_exist} -eq 0 ]] &&  flock -u 200

# Evaluate user options for local variable expansion
FLINK_ENV_JAVA_OPTS=$(eval echo ${FLINK_ENV_JAVA_OPTS})

exec "$JAVA_RUN" $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLINK_TM_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" ${CLASS_TO_RUN} "${ARGS[@]}"
