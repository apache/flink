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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [ "$STRATOSPHERE_IDENT_STRING" = "" ]; then
    STRATOSPHERE_IDENT_STRING="$USER"
fi

# auxilliary function to construct the classpath for the swt visualization component
constructVisualizationClassPath() {

    for jarfile in $STRATOSPHERE_LIB_DIR/*.jar ; do
        if [[ $STRATOSPHERE_VS_CLASSPATH = "" ]]; then
            STRATOSPHERE_VS_CLASSPATH=$jarfile;
        else
            STRATOSPHERE_VS_CLASSPATH=$STRATOSPHERE_VS_CLASSPATH:$jarfile
        fi
    done

    echo $STRATOSPHERE_VS_CLASSPATH
}

log=$STRATOSPHERE_LOG_DIR/stratosphere-$STRATOSPHERE_IDENT_STRING-visualization-$HOSTNAME.log
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$STRATOSPHERE_CONF_DIR"/log4j.properties"

STRATOSPHERE_VS_CLASSPATH=$(constructVisualizationClassPath)

$JAVA_RUN $JVM_ARGS $STRATOSPHERE_OPTS $log_setting -classpath $STRATOSPHERE_VS_CLASSPATH eu.stratosphere.addons.visualization.swt.SWTVisualization -configDir $STRATOSPHERE_CONF_DIR
