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
JOBMANAGER_ADDRESS=$2

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [ "$STRATOSPHERE_IDENT_STRING" = "" ]; then
    STRATOSPHERE_IDENT_STRING="$USER"
fi

# auxilliary function to construct a lightweight classpath for the
# Stratosphere TaskManager
constructTaskManagerClassPath() {

    for jarfile in "$STRATOSPHERE_LIB_DIR"/*.jar ; do
        if [[ $STRATOSPHERE_TM_CLASSPATH = "" ]]; then
            STRATOSPHERE_TM_CLASSPATH=$jarfile;
        else
            STRATOSPHERE_TM_CLASSPATH=$STRATOSPHERE_TM_CLASSPATH:$jarfile
        fi
    done

    echo $STRATOSPHERE_TM_CLASSPATH
}

STRATOSPHERE_TM_CLASSPATH=`manglePathList "$(constructTaskManagerClassPath)"`

log=$STRATOSPHERE_LOG_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-taskmanager-$HOSTNAME.log
out=$STRATOSPHERE_LOG_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-taskmanager-$HOSTNAME.out
pid=$STRATOSPHERE_PID_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-taskmanager.pid
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$STRATOSPHERE_CONF_DIR"/log4j.properties)

JVM_ARGS="$JVM_ARGS -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -XX:NewRatio=6"

if [ "$STRATOSPHERE_TM_HEAP" -gt 0 ]; then
    JVM_ARGS="$JVM_ARGS -Xms"$STRATOSPHERE_TM_HEAP"m -Xmx"$STRATOSPHERE_TM_HEAP"m"
fi

case $STARTSTOP in

    (start)
        mkdir -p "$STRATOSPHERE_PID_DIR"
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
        if [[ -z "$JOBMANAGER_ADDRESS" ]]; then
            $JAVA_RUN $JVM_ARGS ${STRATOSPHERE_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "$STRATOSPHERE_TM_CLASSPATH" eu.stratosphere.nephele.taskmanager.TaskManager -configDir "$STRATOSPHERE_CONF_DIR" > "$out" 2>&1 < /dev/null &
        else
            $JAVA_RUN $JVM_ARGS ${STRATOSPHERE_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "$STRATOSPHERE_TM_CLASSPATH" eu.stratosphere.nephele.taskmanager.TaskManager -jobmanagerAdd "$JOBMANAGER_ADDRESS" -configDir "$STRATOSPHERE_CONF_DIR" > "$out" 2>&1 < /dev/null &
        fi
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
        echo Please specify start or stop
    ;;

esac
