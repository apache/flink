#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get Flink config
. "$bin"/config.sh

while getopts "h?m:l:" opt; do
    case "$opt" in
    h|\?)
        echo ""
        echo "required parameters:"
        echo "-m    <master_address:port>    e.g 127.0.0.1:5050"
        echo "-l   <master_build_dir>    e.g /tmp/mesos/build/src/.lib"
        echo ""
        exit 0
        ;;
    m)  MESOS_MASTER=$OPTARG
        ;;
    l)  MESOS_LIB=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift

CLASSPATH_MESOS=$FLINK_LIB_DIR/*yarn-uberjar.jar

$JAVA_RUN $JVM_ARGS -Djava.library.path=$MESOS_LIB -classpath $CLASSPATH_MESOS org.apache.flink.mesos.MesosController $MESOS_MASTER $CLASSPATH_MESOS $FLINK_CONF_DIR
