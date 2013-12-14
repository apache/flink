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

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
    NEPHELE_IDENT_STRING="$USER"
fi

# auxilliary function to construct a lightweight classpath for the
# Nephele visualization component
constructVisualizationClassPath() {

    for jarfile in $NEPHELE_LIB_DIR/*.jar ; do
        if [[ $NEPHELE_VS_CLASSPATH = "" ]]; then
            NEPHELE_VS_CLASSPATH=$jarfile;
        else
            NEPHELE_VS_CLASSPATH=$NEPHELE_VS_CLASSPATH:$jarfile
        fi
    done

    echo $NEPHELE_VS_CLASSPATH
}

log=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-visualization-$HOSTNAME.log
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

NEPHELE_VS_CLASSPATH=$(constructVisualizationClassPath)

$JAVA_RUN $JVM_ARGS $NEPHELE_OPTS $log_setting -classpath $NEPHELE_VS_CLASSPATH eu.stratosphere.addons.visualization.swt.SWTVisualization -configDir $NEPHELE_CONF_DIR
