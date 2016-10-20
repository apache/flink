#!/bin/sh

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

if [ "$1" = "jobmanager" ]; then
    echo "Starting Job Manager"
    #sed -i -e "s/jobmanager.rpc.address: localhost/jobmanager.rpc.address: `hostname -f`/g" $FLINK_HOME/conf/flink-conf.yaml

    # make use of container linking and exploit the jobmanager entry in /etc/hosts
    sed -i -e "s/jobmanager.rpc.address: localhost/jobmanager.rpc.address: jobmanager/g" $FLINK_HOME/conf/flink-conf.yaml

    echo "config file: " && grep '^[^\n#]' $FLINK_HOME/conf/flink-conf.yaml
    $FLINK_HOME/bin/jobmanager.sh start cluster
elif [ "$1" = "taskmanager" ]; then

    # make use of container linking and exploit the jobmanager entry in /etc/hosts
    sed -i -e "s/jobmanager.rpc.address: localhost/jobmanager.rpc.address: jobmanager/g" $FLINK_HOME/conf/flink-conf.yaml

    sed -i -e "s/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: `grep -c ^processor /proc/cpuinfo`/g" $FLINK_HOME/conf/flink-conf.yaml

    echo "Starting Task Manager"
    echo "config file: " && grep '^[^\n#]' $FLINK_HOME/conf/flink-conf.yaml
    $FLINK_HOME/bin/taskmanager.sh start
else
    $@
fi
