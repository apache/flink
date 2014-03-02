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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get nephele config
. "$bin"/config.sh

if [ "$STRATOSPHERE_IDENT_STRING" = "" ]; then
        STRATOSPHERE_IDENT_STRING="$USER"
fi

STRATOSPHERE_LIB_CLIENTS_DIR=$STRATOSPHERE_ROOT_DIR/lib_clients

JVM_ARGS="$JVM_ARGS -Xmx512m"

# auxilliary function to construct the classpath for the webclient
constructWebclientClassPath() {

	for jarfile in "$STRATOSPHERE_LIB_DIR"/*.jar ; do
		if [[ $STRATOSPHERE_WEBCLIENT_CLASSPATH = "" ]]; then
			STRATOSPHERE_WEBCLIENT_CLASSPATH=$jarfile;
		else
			STRATOSPHERE_WEBCLIENT_CLASSPATH=$STRATOSPHERE_WEBCLIENT_CLASSPATH:$jarfile
		fi
	done
	
	for jarfile in "$STRATOSPHERE_LIB_CLIENTS_DIR"/*.jar ; do
		STRATOSPHERE_WEBCLIENT_CLASSPATH=$STRATOSPHERE_WEBCLIENT_CLASSPATH:$jarfile
	done

	echo $STRATOSPHERE_WEBCLIENT_CLASSPATH
}

STRATOSPHERE_WEBCLIENT_CLASSPATH=`manglePathList "$(constructWebclientClassPath)"`

log=$STRATOSPHERE_LOG_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-webclient-$HOSTNAME.log
out=$STRATOSPHERE_LOG_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-webclient-$HOSTNAME.out
pid=$STRATOSPHERE_PID_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-webclient.pid
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$STRATOSPHERE_CONF_DIR"/log4j.properties)

case $STARTSTOP in

        (start)
                mkdir -p "$STRATOSPHERE_PID_DIR"
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Stratosphere webclient running as process `cat $pid`.  Stop it first.
                                exit 1
                        fi
                fi
                echo Starting Stratosphere webclient
		$JAVA_RUN $JVM_ARGS "${log_setting[@]}" -classpath "$STRATOSPHERE_WEBCLIENT_CLASSPATH" eu.stratosphere.client.WebFrontend -configDir "$STRATOSPHERE_CONF_DIR" > "$out" 2>&1 < /dev/null &
		echo $! > $pid
	;;

        (stop)
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Stopping Stratosphere webclient
                                kill `cat $pid`
                        else
                                echo No Stratosphere webclient to stop
                        fi
                else
                        echo No Stratosphere webclient to stop
                fi
        ;;

        (*)
                echo Please specify start or stop
        ;;

esac

