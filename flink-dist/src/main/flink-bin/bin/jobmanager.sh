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


STARTSTOP=$1
EXECUTIONMODE=$2

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [ "$EXECUTIONMODE" = "local" ]; then
    FLINK_JM_HEAP=`expr $FLINK_JM_HEAP + $FLINK_TM_HEAP`
fi

JVM_ARGS="$JVM_ARGS -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m"

if [ "$FLINK_JM_HEAP" -gt 0 ]; then
    JVM_ARGS="$JVM_ARGS -Xms"$FLINK_JM_HEAP"m -Xmx"$FLINK_JM_HEAP"m"
fi

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

# auxilliary function to construct a the classpath for the jobmanager
constructJobManagerClassPath() {
    for jarfile in "$FLINK_LIB_DIR"/*.jar ; do
        if [[ $FLINK_JM_CLASSPATH = "" ]]; then
            FLINK_JM_CLASSPATH=$jarfile;
        else
            FLINK_JM_CLASSPATH=$FLINK_JM_CLASSPATH:$jarfile
        fi
    done

    echo $FLINK_JM_CLASSPATH
}

FLINK_JM_CLASSPATH=`manglePathList "$(constructJobManagerClassPath)"`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-jobmanager-$HOSTNAME.log
out=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-jobmanager-$HOSTNAME.out
pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-jobmanager.pid
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml)

case $STARTSTOP in

    (start)
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

        echo Starting job manager
        $JAVA_RUN $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "$FLINK_JM_CLASSPATH" org.apache.flink.runtime.jobmanager.JobManager -executionMode $EXECUTIONMODE -configDir "$FLINK_CONF_DIR"  > "$out" 2>&1 < /dev/null &
        echo $! > $pid
    ;;

    (stop)
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Stopping job manager
                kill `cat $pid`
            else
                echo No job manager to stop
            fi
        else
            echo No job manager to stop
        fi
    ;;

    (*)
        echo Please specify start or stop
    ;;

esac
