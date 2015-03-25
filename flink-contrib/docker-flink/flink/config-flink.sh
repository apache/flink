#!/bin/bash

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