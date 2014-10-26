#!/usr/bin/env bash

# This script uses MESOS_SOURCE_DIR and MESOS_BUILD_DIR which come
# from configuration substitutions.
MESOS_SOURCE_DIR=/home/sebastian/IdeaProjects/mesos-0.20.1/build/..
MESOS_BUILD_DIR=/home/sebastian/IdeaProjects/mesos-0.20.1/build

# Locate Java from environment or use configure discovered location.
JAVA_HOME=${JAVA_HOME-/usr/lib/jvm/java-7-oracle/}
JAVA=${JAVA-${JAVA_HOME}/bin/java}

FLINK_LIB=/home/sebastian/IdeaProjects/incubator-flink/flink-dist/target/flink-0.7-incubating-SNAPSHOT-bin/flink-0.7-incubating-SNAPSHOT/lib
FLINK_CORE=${FLINK_LIB}/flink-core-0.7-incubating-SNAPSHOT.jar
FLINK_RUNTIME=${FLINK_LIB}/flink-runtime-0.7-incubating-SNAPSHOT.jar
FLINK_UBERJAR=/home/sebastian/IdeaProjects/incubator-flink/flink-dist/target/flink-dist-0.7-incubating-SNAPSHOT-yarn-uberjar.jar

# Use colors for errors.
. ${MESOS_SOURCE_DIR}/support/colors.sh

PROTOBUF_JAR=${MESOS_BUILD_DIR}/src/java/target/protobuf-java-2.5.0.jar

test ! -e ${PROTOBUF_JAR} && \
  echo "${RED}Failed to find ${PROTOBUF_JAR}${NORMAL}" && \
  exit 1

MESOS_JAR=${MESOS_BUILD_DIR}/src/java/target/mesos-0.20.1.jar

test ! -e ${MESOS_JAR} && \
  echo "${RED}Failed to find ${MESOS_JAR}${NORMAL}" && \
  exit 1

EXAMPLES_JAR=${MESOS_BUILD_DIR}/src/examples.jar

test ! -e ${EXAMPLES_JAR} && \
  echo "${RED}Failed to find ${EXAMPLES_JAR}${NORMAL}" && \
  exit 1

exec ${JAVA} -cp ${PROTOBUF_JAR}:${MESOS_JAR}:${EXAMPLES_JAR}:${FLINK_UBERJAR}:/home/sebastian/IdeaProjects/incubator-flink/flink-mesos/target/classes \
  -Djava.library.path=${MESOS_BUILD_DIR}/src/.libs \
 develop.FlinkMesosEx "${@}"
