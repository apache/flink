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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/nephele-config.sh

HOSTLIST=$NEPHELE_SLAVES

if [ "$HOSTLIST" = "" ]; then
	HOSTLIST="${NEPHELE_CONF_DIR}/slaves"
fi

if [ ! -f $HOSTLIST ]; then
	echo $HOSTLIST is not a valid slave list
	exit 1
fi


# cluster mode, only bring up job manager locally and a task manager on every slave host
$NEPHELE_BIN_DIR/nephele-jobmanager.sh stop

for SLAVE in `cat "$HOSTLIST"`; do
  HOST=$( extractHostName $SLAVE)
  ssh $NEPHELE_SSH_OPTS $HOST nohup $NEPHELE_BIN_DIR/nephele-taskmanager.sh stop &
done
