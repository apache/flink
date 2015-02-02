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



bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get Flink config
. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

JVM_ARGS="$JVM_ARGS -Xmx512m"

# auxilliary function to construct a lightweight classpath for the
# Flink CLI client
constructCLIClientClassPath() {

	for jarfile in $FLINK_LIB_DIR/*.jar ; do
		if [[ $CC_CLASSPATH = "" ]]; then
			CC_CLASSPATH=$jarfile;
		else
			CC_CLASSPATH=$CC_CLASSPATH:$jarfile
		fi
	done
	echo $CC_CLASSPATH
}

CC_CLASSPATH=`manglePathList $(constructCLIClientClassPath)`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-yarn-session-$HOSTNAME.log
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j-yarn-session.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback-yarn.xml"

export FLINK_CONF_DIR

$JAVA_RUN $JVM_ARGS -classpath $CC_CLASSPATH:$HADOOP_CLASSPATH:$HADOOP_CONF_DIR:$YARN_CONF_DIR  $log_setting org.apache.flink.client.FlinkYarnSessionCli -ship $bin/../ship/ -j $FLINK_LIB_DIR/*yarn-uberjar.jar $*

