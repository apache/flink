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
EXECUTIONMODE=$2

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/nephele-config.sh

if [ "$EXECUTIONMODE" = "local" ]; then
	NEPHELE_JM_HEAP=`echo $NEPHELE_JM_HEAP+$NEPHELE_TM_HEAP | bc`
fi

JVM_ARGS="$JVM_ARGS -Xms"$NEPHELE_JM_HEAP"m -Xmx"$NEPHELE_JM_HEAP"m"

if [ "$NEPHELE_PID_DIR" = "" ]; then
	NEPHELE_PID_DIR=/tmp
fi

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
	NEPHELE_IDENT_STRING="$USER"
fi

# auxilliary function to construct a lightweight classpath for the
# Nephele JobManager
constructJobManagerClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do
		if [[ $NEPHELE_JM_CLASSPATH = "" ]]; then
			NEPHELE_JM_CLASSPATH=$jarfile;
		else
			NEPHELE_JM_CLASSPATH=$NEPHELE_JM_CLASSPATH:$jarfile
		fi
	done

	for jarfile in $NEPHELE_LIB_DIR/dropins/*.jar ; do
		NEPHELE_JM_CLASSPATH=$NEPHELE_JM_CLASSPATH:$jarfile
	done

	echo $NEPHELE_JM_CLASSPATH
}

NEPHELE_JM_CLASSPATH=$(constructJobManagerClassPath)

log=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-jobmanager-$HOSTNAME.log
out=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-jobmanager-$HOSTNAME.out
pid=$NEPHELE_PID_DIR/nephele-$NEPHELE_IDENT_STRING-jobmanager.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

case $STARTSTOP in

	(start)
		mkdir -p "$NEPHELE_PID_DIR"
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo Nephele job manager running as process `cat $pid`.  Stop it first.
				exit 1
     			fi
		fi

                # Rotate log files
                rotateLogFile $log
                rotateLogFile $out

		echo Starting Nephele job manager
		$JAVA_HOME/bin/java $JVM_ARGS $NEPHELE_OPTS $log_setting -classpath $NEPHELE_JM_CLASSPATH eu.stratosphere.nephele.jobmanager.JobManager -executionMode $EXECUTIONMODE -configDir $NEPHELE_CONF_DIR  > "$out" 2>&1 < /dev/null &
		echo $! > $pid
	;;

	(stop)
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo Stopping Nephele job manager
				kill `cat $pid`
			else
				echo No Nephele job manager to stop
			fi
		else
			echo No Nephele job manager to stop
		fi
	;;

	(*)
		echo Please specify start or stop
	;;

esac
