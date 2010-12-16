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

#!/bin/bash

STARTSTOP=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/nephele-config.sh

if [ "$NEPHELE_PID_DIR" = "" ]; then
	NEPHELE_PID_DIR=/tmp
fi

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
	NEPHELE_IDENT_STRING="$USER"
fi

log=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-taskmanager-$HOSTNAME.log
pid=$NEPHELE_PID_DIR/nephele-$NEPHELE_IDENT_STRING-taskmanager.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

JVM_ARGS="$JVM_ARGS -XX:+UseParNewGC -XX:NewRatio=8 -XX:PretenureSizeThreshold=64m -Xms"$TM_JHEAP"m -Xmx"$TM_JHEAP"m"

case $STARTSTOP in

	(start)
		mkdir -p "$NEPHELE_PID_DIR"
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo Nephele task manager running as process `cat $pid` on host $HOSTNAME.  Stop it first.
				exit 1
     			fi
		fi
		echo starting Nephele task manager on host $HOSTNAME
		$JAVA_HOME/bin/java $JVM_ARGS $NEPHELE_OPTS $log_setting -classpath $CLASSPATH eu.stratosphere.nephele.taskmanager.TaskManager -configDir $NEPHELE_CONF_DIR < /dev/null &
		echo $! > $pid
	;;

	(stop)
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo stopping Nephele task manager on host $HOSTNAME
				kill `cat $pid`
			else
				echo no Nephele task manager to stop on host $HOSTNAME
			fi
		else
			echo no Nephele task manager to stop on host $HOSTNAME
		fi
	;;

	(*)
		echo Please specify start or stop
	;;

esac
