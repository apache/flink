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

JVM_ARGS="$JVM_ARGS -Xmx512m"

# auxilliary function to construct a lightweight classpath for the
# PACT Webfrontend
constructPactWebFrontendClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do

		add=0

		if [[ "$jarfile" =~ 'nephele-server' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-management' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-hdfs' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-clients' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-runtime' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-compiler' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-logging' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'log4j' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-core' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jackson-core-asl' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jackson-mapper-asl' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-fileupload' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-io' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jetty-continuation' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jetty-http' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jetty-io' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jetty-security' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jetty-server' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jetty-servlet' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jetty-util' ]]; then
			add=1			
		elif [[ "$jarfile" =~ 'servlet-api' ]]; then
			add=1
		fi

		if [[ "$add" = "1" ]]; then
			if [[ $PACT_WF_CLASSPATH = "" ]]; then
				PACT_WF_CLASSPATH=$jarfile;
			else
				PACT_WF_CLASSPATH=$PACT_WF_CLASSPATH:$jarfile
			fi
		fi
	done

	echo $PACT_WF_CLASSPATH
}

PACT_WF_CLASSPATH=$(constructPactWebFrontendClassPath)

log=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-pact-web-$HOSTNAME.log
pid=$NEPHELE_PID_DIR/nephele-$NEPHELE_IDENT_STRING-pact-web.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

case $STARTSTOP in

        (start)
                mkdir -p "$NEPHELE_PID_DIR"
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo PACT Webfrontend running as process `cat $pid`.  Stop it first.
                                exit 1
                        fi
                fi
                echo starting PACT Webfrontend
		$JAVA_HOME/bin/java $JVM_ARGS $log_setting -classpath $PACT_WF_CLASSPATH eu.stratosphere.pact.client.WebFrontend -configDir $NEPHELE_CONF_DIR &
		echo $! > $pid
	;;

        (stop)
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo stopping PACT Webfrontend
                                kill `cat $pid`
                        else
                                echo no PACT Webfrontend to stop
                        fi
                else
                        echo no PACT Webfrontend to stop
                fi
        ;;

        (*)
                echo Please specify start or stop
        ;;

esac


