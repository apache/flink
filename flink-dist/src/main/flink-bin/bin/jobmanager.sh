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
EXECUTIONMODE=$2
STREAMINGMODE=$3

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

JAVA_VERSION=$($JAVA_RUN -version 2>&1 | sed 's/java version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

if [ "$JAVA_VERSION" -lt 18 ]; then
    JVM_ARGS="$JVM_ARGS -XX:MaxPermSize=256m"
fi

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

FLINK_JM_CLASSPATH=`constructFlinkClassPath`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-jobmanager-$HOSTNAME.log
out=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-jobmanager-$HOSTNAME.out
pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-jobmanager.pid
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml)

case $STARTSTOP in

    (start)

        if [ -z $EXECUTIONMODE ]; then
            echo "Please specify 'start (cluster|local) [batch|streaming]' or 'stop'"
            exit 1
        fi

        # Use batch mode as default
        if [ -z $STREAMINGMODE ]; then
            echo "Did not specify [batch|streaming] mode. Falling back to batch mode as default."
            STREAMINGMODE="batch"
        fi

        if [[ ! ${FLINK_JM_HEAP} =~ $IS_NUMBER ]]; then
            echo "ERROR: Configured job manager heap size is not a number. Cancelling job manager startup."

            exit 1
        fi

        if [ "$EXECUTIONMODE" = "local" ]; then
            if [[ ! ${FLINK_TM_HEAP} =~ $IS_NUMBER ]]; then
                echo "ERROR: Configured task manager heap size is not a number. Cancelling (local) job manager startup."

                exit 1
            fi

            FLINK_JM_HEAP=`expr $FLINK_JM_HEAP + $FLINK_TM_HEAP`
        fi

        if [ "$FLINK_JM_HEAP" -gt 0 ]; then
            JVM_ARGS="$JVM_ARGS -Xms"$FLINK_JM_HEAP"m -Xmx"$FLINK_JM_HEAP"m"
        fi

        mkdir -p "$FLINK_PID_DIR"
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Job manager running as process `cat $pid`.  Stop it first.
                exit 1
            fi
        fi

        # Rotate log files
        rotateLogFile $log
        rotateLogFile $out

        echo "Starting Job Manager"
        $JAVA_RUN $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLINK_JM_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.runtime.jobmanager.JobManager --configDir "$FLINK_CONF_DIR" --executionMode $EXECUTIONMODE --streamingMode "$STREAMINGMODE" > "$out" 2>&1 < /dev/null &
        echo $! > $pid

    ;;

    (stop)
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo "Stopping job manager"
                kill `cat $pid`
            else
                echo "No job manager to stop"
            fi
        else
            echo "No job manager to stop"
        fi
    ;;

    (*)
        echo "Please specify 'start (cluster|local) [batch|streaming]' or 'stop'"
    ;;

esac
