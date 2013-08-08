#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
. "$bin"/nephele-config.sh

if [ "$NEPHELE_PID_DIR" = "" ]; then
        NEPHELE_PID_DIR=/tmp
fi

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
        NEPHELE_IDENT_STRING="$USER"
fi

NEPHELE_LIB_CLIENTS_DIR=$NEPHELE_ROOT_DIR/lib_clients

JVM_ARGS="$JVM_ARGS -Xmx512m"

# auxilliary function to construct a lightweight classpath for the
# PACT Webfrontend
constructPactWebFrontendClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do
		if [[ $PACT_WF_CLASSPATH = "" ]]; then
			PACT_WF_CLASSPATH=$jarfile;
		else
			PACT_WF_CLASSPATH=$PACT_WF_CLASSPATH:$jarfile
		fi
	done

	for jarfile in $NEPHELE_LIB_DIR/dropin/*.jar ; do
		PACT_WF_CLASSPATH=$PACT_WF_CLASSPATH:$jarfile
	done
	PACT_WF_CLASSPATH=$PACT_WF_CLASSPATH:$NEPHELE_LIB_DIR/dropin/
	
	for jarfile in $NEPHELE_LIB_CLIENTS_DIR/*.jar ; do
		PACT_WF_CLASSPATH=$PACT_WF_CLASSPATH:$jarfile
	done

	echo $PACT_WF_CLASSPATH
}

PACT_WF_CLASSPATH=`manglePathList $(constructPactWebFrontendClassPath)`

log=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-pact-web-$HOSTNAME.log
pid=$NEPHELE_PID_DIR/nephele-$NEPHELE_IDENT_STRING-pact-web.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$NEPHELE_CONF_DIR"/log4j.properties"

case $STARTSTOP in

        (start)
                mkdir -p "$NEPHELE_PID_DIR"
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo PACT Webfrontend running as process `cat $pid`.  Stop it first.
                                exit 1
                        fi
                fi
                echo Starting PACT Webfrontend
		$JAVA_RUN $JVM_ARGS $log_setting -classpath $PACT_WF_CLASSPATH eu.stratosphere.pact.client.WebFrontend -configDir $NEPHELE_CONF_DIR &
		echo $! > $pid
	;;

        (stop)
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Stopping PACT Webfrontend
                                kill `cat $pid`
                        else
                                echo No PACT Webfrontend to stop
                        fi
                else
                        echo No PACT Webfrontend to stop
                fi
        ;;

        (*)
                echo Please specify start or stop
        ;;

esac


