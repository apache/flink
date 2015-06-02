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


STARTSTOP=$1
STREAMINGMODE=$2

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

FLINK_TM_CLASSPATH=`constructFlinkClassPath`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-taskmanager-$HOSTNAME.log
out=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-taskmanager-$HOSTNAME.out
pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-taskmanager.pid
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml)

JAVA_VERSION=$($JAVA_RUN -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

# Only set JVM 8 arguments if we have correctly extracted the version
if [[ ${JAVA_VERSION} =~ ${IS_NUMBER} ]]; then
    if [ "$JAVA_VERSION" -lt 18 ]; then
        JVM_ARGS="$JVM_ARGS -XX:MaxPermSize=256m"
    fi
fi

case $STARTSTOP in

    (start)

        # Use batch mode as default
        if [ -z $STREAMINGMODE ]; then
            echo "Did not specify [batch|streaming] mode. Falling back to batch mode as default."
            STREAMINGMODE="batch"
        fi

        if [[ ! ${FLINK_TM_HEAP} =~ ${IS_NUMBER} ]]; then
            echo "ERROR: Configured task manager heap size is not a number. Cancelling task manager startup."
            exit 1
        fi

        if [ "$FLINK_TM_HEAP" -gt 0 ]; then
            JVM_ARGS="$JVM_ARGS -Xms"$FLINK_TM_HEAP"m -Xmx"$FLINK_TM_HEAP"m"
        fi

        mkdir -p "$FLINK_PID_DIR"
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Task manager running as process `cat $pid` on host $HOSTNAME.  Stop it first.
                exit 1
            fi
        fi

        # Rotate log files
        rotateLogFile $log
        rotateLogFile $out

        echo Starting task manager on host $HOSTNAME
        $JAVA_RUN $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLINK_TM_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.runtime.taskmanager.TaskManager --configDir "$FLINK_CONF_DIR" --streamingMode "$STREAMINGMODE" > "$out" 2>&1 < /dev/null &
        echo $! > $pid

    ;;

    (stop)
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Stopping task manager on host $HOSTNAME
                kill `cat $pid`
            else
                echo No task manager to stop on host $HOSTNAME
            fi
        else
            echo No task manager to stop on host $HOSTNAME
        fi
    ;;

    (*)
        echo "Please specify 'start [batch|streaming]' or 'stop'"
    ;;

esac
