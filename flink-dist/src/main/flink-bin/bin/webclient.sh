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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get nephele config
. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

JVM_ARGS="$JVM_ARGS -Xmx512m"

# auxilliary function to construct the classpath for the webclient
constructWebclientClassPath() {

	for jarfile in "$FLINK_LIB_DIR"/*.jar ; do
		if [[ $FLINK_WEBCLIENT_CLASSPATH = "" ]]; then
			FLINK_WEBCLIENT_CLASSPATH=$jarfile;
		else
			FLINK_WEBCLIENT_CLASSPATH=$FLINK_WEBCLIENT_CLASSPATH:$jarfile
		fi
	done

	echo $FLINK_WEBCLIENT_CLASSPATH
}

FLINK_WEBCLIENT_CLASSPATH=`manglePathList "$(constructWebclientClassPath)"`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-webclient-$HOSTNAME.log
out=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-webclient-$HOSTNAME.out
pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-webclient.pid
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml)

case $STARTSTOP in

        (start)
                mkdir -p "$FLINK_PID_DIR"
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Flink webclient running as process `cat $pid`.  Stop it first.
                                exit 1
                        fi
                fi
                echo Starting Flink webclient
		$JAVA_RUN $JVM_ARGS "${log_setting[@]}" -classpath "$FLINK_WEBCLIENT_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS" org.apache.flink.client.WebFrontend --configDir "$FLINK_CONF_DIR" > "$out" 2>&1 < /dev/null &
		echo $! > $pid
	;;

        (stop)
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Stopping Flink webclient
                                kill `cat $pid`
                        else
                                echo No Flink webclient to stop
                        fi
                else
                        echo No Flink webclient to stop
                fi
        ;;

        (*)
                echo Please specify start or stop
        ;;

esac

