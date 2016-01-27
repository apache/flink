#!/bin/bash

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




CONF=/usr/local/flink/conf
EXEC=/usr/local/flink/bin

#set nb_slots = nb CPUs
#let "nbslots=$2 * `nproc`"
sed -i -e "s/%nb_slots%/`nproc`/g" $CONF/flink-conf.yaml

#set parallelism
sed -i -e "s/%parallelism%/1/g" $CONF/flink-conf.yaml

if [ "$1" = "jobmanager" ]; then
    echo "Configuring Job Manager on this node"
    sed -i -e "s/%jobmanager%/`hostname -i`/g" $CONF/flink-conf.yaml
    $EXEC/jobmanager.sh start cluster
    $EXEC/start-webclient.sh

elif [ "$1" = "taskmanager" ]; then
    echo "Configuring Task Manager on this node"
    sed -i -e "s/%jobmanager%/$JOBMANAGER_PORT_6123_TCP_ADDR/g" $CONF/flink-conf.yaml
    $EXEC/taskmanager.sh start
fi

#print out config - debug
echo "config file: " && cat $CONF/flink-conf.yaml

#add ENV variable to shell for ssh login
echo "export JAVA_HOME=/usr/java/default;" >> ~/.profile
echo "export PATH=$PATH:$JAVA_HOME/bin;" >> ~/.profile
echo "export F=/usr/local/flink/;" >> ~/.profile
#Uncomment for SSH connection between nodes without prompts
#echo 'export FLINK_SSH_OPTS="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"' >> ~/.profile

#run ssh server and supervisor to keep container running.
/usr/sbin/sshd && supervisord -c /etc/supervisor/supervisor.conf
