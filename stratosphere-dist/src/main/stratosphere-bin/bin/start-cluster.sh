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

HOSTLIST=$STRATOSPHERE_SLAVES

if [ "$HOSTLIST" = "" ]; then
    HOSTLIST="${STRATOSPHERE_CONF_DIR}/slaves"
fi

if [ ! -f "$HOSTLIST" ]; then
    echo $HOSTLIST is not a valid slave list
    exit 1
fi

if [ "${UNAME:0:6}" == "CYGWIN" ] || [ "$UNAME" == "Linux" ]; then
    sed -i 's/'$KEY_JOBM_ADD':.*/'$KEY_JOBM_ADD': '$HOSTNAME'/g' $YAML_CONF
elif [ "$UNAME" == "Darwin" ]; then
    sed -i "" 's/'$KEY_JOBM_ADD':.*/'$KEY_JOBM_ADD': '$HOSTNAME'/g' $YAML_CONF
else
    echo "System $UNAME is not supported to rewrite the jobmanager address in config file."
fi

# cluster mode, bring up job manager locally and a task manager on every slave host
"$STRATOSPHERE_BIN_DIR"/jobmanager.sh start cluster $HOSTNAME

GOON=true
while $GOON
do
    read line || GOON=false
    if [ -n "$line" ]; then
        HOST=$( extractHostName $line)
        ssh -n $STRATOSPHERE_SSH_OPTS $HOST -- "nohup /bin/bash $STRATOSPHERE_BIN_DIR/taskmanager.sh start $HOSTNAME"
    fi
done < "$HOSTLIST"
