#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get Flink config
. "$bin"/config.sh

CLASSPATH_MESOS=$FLINK_LIB_DIR/*yarn-uberjar.jar

$JAVA_RUN $JVM_ARGS -classpath $CLASSPATH_MESOS org.apache.flink.mesos.MesosController -j $CLASSPATH_MESOS -c $FLINK_CONF_DIR $*
