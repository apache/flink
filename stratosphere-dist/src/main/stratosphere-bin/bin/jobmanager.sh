#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

STARTSTOP=$1
EXECUTIONMODE=$2
JOBMANAGER_ADDRESS=$3

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [ "$EXECUTIONMODE" = "local" ]; then
    STRATOSPHERE_JM_HEAP=`expr $STRATOSPHERE_JM_HEAP + $STRATOSPHERE_TM_HEAP`
fi

JVM_ARGS="$JVM_ARGS -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m"

if [ "$STRATOSPHERE_JM_HEAP" -gt 0 ]; then
    JVM_ARGS="$JVM_ARGS -Xms"$STRATOSPHERE_JM_HEAP"m -Xmx"$STRATOSPHERE_JM_HEAP"m"
fi

if [ "$STRATOSPHERE_IDENT_STRING" = "" ]; then
    STRATOSPHERE_IDENT_STRING="$USER"
fi

# auxilliary function to construct a the classpath for the jobmanager
constructJobManagerClassPath() {
    for jarfile in "$STRATOSPHERE_LIB_DIR"/*.jar ; do
        if [[ $STRATOSPHERE_JM_CLASSPATH = "" ]]; then
            STRATOSPHERE_JM_CLASSPATH=$jarfile;
        else
            STRATOSPHERE_JM_CLASSPATH=$STRATOSPHERE_JM_CLASSPATH:$jarfile
        fi
    done

    echo $STRATOSPHERE_JM_CLASSPATH
}

STRATOSPHERE_JM_CLASSPATH=`manglePathList "$(constructJobManagerClassPath)"`

log=$STRATOSPHERE_LOG_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-jobmanager-$HOSTNAME.log
out=$STRATOSPHERE_LOG_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-jobmanager-$HOSTNAME.out
pid=$STRATOSPHERE_PID_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-jobmanager.pid
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$STRATOSPHERE_CONF_DIR"/log4j.properties)

case $STARTSTOP in

    (start)
        mkdir -p "$STRATOSPHERE_PID_DIR"
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
        if [[ -z "$JOBMANAGER_ADDRESS" ]]; then
            $JAVA_RUN $JVM_ARGS ${STRATOSPHERE_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "$STRATOSPHERE_JM_CLASSPATH" eu.stratosphere.nephele.jobmanager.JobManager -executionMode $EXECUTIONMODE -configDir "$STRATOSPHERE_CONF_DIR"  > "$out" 2>&1 < /dev/null &
        else
            $JAVA_RUN $JVM_ARGS ${STRATOSPHERE_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "$STRATOSPHERE_JM_CLASSPATH" eu.stratosphere.nephele.jobmanager.JobManager -jobmanagerAdd $JOBMANAGER_ADDRESS -executionMode $EXECUTIONMODE -configDir "$STRATOSPHERE_CONF_DIR"  > "$out" 2>&1 < /dev/null &
        fi
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
