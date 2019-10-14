#!/bin/bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

: ${HADOOP_PREFIX:=/usr/local/hadoop}

$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

MAX_RETRY_SECONDS=800

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

sed -i "s#/usr/local/hadoop/bin/container-executor#${NM_CONTAINER_EXECUTOR_PATH}#g" $HADOOP_PREFIX/etc/hadoop/yarn-site.xml


service ssh start

if [ "$1" == "--help" -o "$1" == "-h" ]; then
    echo "Usage: $(basename $0) (master|worker|hive)"
    exit 0
elif [ "$1" == "master" ]; then
    yes| sudo -E -u hdfs $HADOOP_PREFIX/bin/hdfs namenode -format

    nohup sudo -E -u hdfs $HADOOP_PREFIX/bin/hdfs namenode 2>> /var/log/hadoop/namenode.err >> /var/log/hadoop/namenode.out &
    nohup sudo -E -u yarn $HADOOP_PREFIX/bin/yarn resourcemanager 2>> /var/log/hadoop/resourcemanager.err >> /var/log/hadoop/resourcemanager.out &
    nohup sudo -E -u yarn $HADOOP_PREFIX/bin/yarn timelineserver 2>> /var/log/hadoop/timelineserver.err >> /var/log/hadoop/timelineserver.out &
    nohup sudo -E -u mapred $HADOOP_PREFIX/bin/mapred historyserver 2>> /var/log/hadoop/historyserver.err >> /var/log/hadoop/historyserver.out &

    hdfs dfsadmin -safemode wait
    while [ $? -ne 0 ]; do hdfs dfsadmin -safemode wait; done

    hdfs dfs -chown hdfs:hadoop /
    hdfs dfs -chmod 755 /
    hdfs dfs -mkdir /tmp
    hdfs dfs -chown hdfs:hadoop /tmp
    hdfs dfs -chmod -R 1777 /tmp
    hdfs dfs -mkdir /tmp/logs
    hdfs dfs -chown yarn:hadoop /tmp/logs
    hdfs dfs -chmod 1777 /tmp/logs

    hdfs dfs -mkdir -p /user/hadoop-user
    hdfs dfs -chown hadoop-user:hadoop-user /user/hadoop-user

    echo "Finished master initialization"

    while true; do sleep 1000; done
elif [ "$1" == "worker" ]; then
    nohup sudo -E -u hdfs $HADOOP_PREFIX/bin/hdfs datanode 2>> /var/log/hadoop/datanode.err >> /var/log/hadoop/datanode.out &
    nohup sudo -E -u yarn $HADOOP_PREFIX/bin/yarn nodemanager 2>> /var/log/hadoop/nodemanager.err >> /var/log/hadoop/nodemanager.out &
    while true; do sleep 1000; done
elif [ "$1" == "hive" ]; then

    # wait hadoop service start
    until curl master:50070
    do sleep 5
    done

    echo "The hadoop cluster has started.."
    sleep 5
    hdfs dfs -mkdir -p /user/hive/warehouse

    schematool --dbType mysql --initSchema

    # start metastore
    nohup hive --service metastore 2>> /var/log/hive/hivemetastore.err >> /var/log/hive/hivemetastore.out &
    sleep 5
    # prepare hive data
    /etc/init-hive-data.sh

    echo "The hadoop cluster and hive service have been ready.."
    while true; do sleep 1000; done
fi

