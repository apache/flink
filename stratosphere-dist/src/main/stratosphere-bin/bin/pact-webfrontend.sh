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

webDir="$bin/../resources/web-docs/"

# get nephele config
. "$bin"/nephele-config.sh

if [ "$NEPHELE_PID_DIR" = "" ]; then
        NEPHELE_PID_DIR=/tmp
fi

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
        NEPHELE_IDENT_STRING="$USER"
fi

JVM_ARGS="$JVM_ARGS -Xmx512m"

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
		$JAVA_HOME/bin/java $JVM_ARGS $log_setting -classpath $CLASSPATH eu.stratosphere.pact.client.WebFrontend -configDir $NEPHELE_CONF_DIR -webdir $webDir &
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


