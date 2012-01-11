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

. "$bin"/nephele-config.sh

# auxilliary function to construct a lightweight classpath for the
# Nephele visualization component
constructVisualizationClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do

		add=0

		if [[ "$jarfile" =~ 'nephele-visualization' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-management' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-logging' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'log4j' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'x86_64' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jfreechart' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jcommon' ]]; then
			add=1
		fi

		if [[ "$add" = "1" ]]; then
			if [[ $NEPHELE_VS_CLASSPATH = "" ]]; then
				NEPHELE_VS_CLASSPATH=$jarfile;
			else
				NEPHELE_VS_CLASSPATH=$NEPHELE_VS_CLASSPATH:$jarfile
			fi
		fi
	done

	echo $NEPHELE_VS_CLASSPATH
}

NEPHELE_VS_CLASSPATH=$(constructVisualizationClassPath)


$JAVA_HOME/bin/java $JVM_ARGS $NEPHELE_OPTS -classpath $NEPHELE_VS_CLASSPATH eu.stratosphere.nephele.visualization.swt.SWTVisualization -configDir $NEPHELE_CONF_DIR
