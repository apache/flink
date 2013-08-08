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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get nephele config
. "$bin"/nephele-config.sh

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
        NEPHELE_IDENT_STRING="$USER"
fi

NEPHELE_LIB_CLIENTS_DIR=$NEPHELE_ROOT_DIR/lib_clients

JVM_ARGS="$JVM_ARGS -Xmx512m"

# auxilliary function to construct a lightweight classpath for the
# PACT CLI client
constructPactCLIClientClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do
		if [[ $PACT_CC_CLASSPATH = "" ]]; then
			PACT_CC_CLASSPATH=$jarfile;
		else
			PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$jarfile
		fi
	done

	for jarfile in $NEPHELE_LIB_DIR/dropin/*.jar ; do
		PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$jarfile
	done
	PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$NEPHELE_LIB_DIR/dropin/
	
	for jarfile in $NEPHELE_LIB_CLIENTS_DIR/*.jar ; do
		PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$jarfile
	done

	echo $PACT_CC_CLASSPATH
}

PACT_CC_CLASSPATH=`manglePathList $(constructPactCLIClientClassPath)`

log=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-pact-run-$HOSTNAME.log
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$NEPHELE_CONF_DIR"/log4j.properties"

export NEPHELE_CONF_DIR

$JAVA_RUN $JVM_ARGS $log_setting -classpath $PACT_CC_CLASSPATH eu.stratosphere.pact.client.CliFrontend $*
