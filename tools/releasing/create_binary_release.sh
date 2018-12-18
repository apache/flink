#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## Variables with defaults (if not overwritten by environment)
##
SCALA_VERSION=${SCALA_VERSION:-none}
HADOOP_VERSION=${HADOOP_VERSION:-none}
SKIP_GPG=${SKIP_GPG:-false}
MVN=${MVN:-mvn}

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

cd ..

FLINK_DIR=`pwd`
RELEASE_DIR=${FLINK_DIR}/tools/releasing/release
mkdir -p ${RELEASE_DIR}

###########################

# build maven package, create Flink distribution, generate signature
make_binary_release() {
  NAME=$1
  FLAGS=$2
  SCALA_VERSION=$3

  echo "Creating binary release name: $NAME, flags: $FLAGS, SCALA_VERSION: ${SCALA_VERSION}"
  if [[ -z $NAME ]]; then
    dir_name="flink-$RELEASE_VERSION-bin-scala_${SCALA_VERSION}"
  else
    dir_name="flink-$RELEASE_VERSION-bin-$NAME-scala_${SCALA_VERSION}"
  fi

  if [ $SCALA_VERSION = "2.12" ]; then
      FLAGS="$FLAGS -Dscala-2.12"
  elif [ $SCALA_VERSION = "2.11" ]; then
      FLAGS="$FLAGS -Dscala-2.11"
  else
      echo "Invalid Scala version ${SCALA_VERSION}"
  fi

  # enable release profile here (to check for the maven version)
  $MVN clean package $FLAGS -Prelease -pl flink-dist -am -Dgpg.skip -Dcheckstyle.skip=true -DskipTests

  cd flink-dist/target/flink-*-bin/
  tar czf "${dir_name}.tgz" flink-*

  cp flink-*.tgz ${RELEASE_DIR}
  cd ${RELEASE_DIR}

  # Sign sha the tgz
  if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig "${dir_name}.tgz"
  fi
  $SHASUM "${dir_name}.tgz" > "${dir_name}.tgz.sha512"

  cd ${FLINK_DIR}
}

HADOOP_CLASSIFIERS=("24" "26" "27" "28")
HADOOP_VERSIONS=("2.4.1" "2.6.5" "2.7.5" "2.8.3")

if [ "$SCALA_VERSION" == "none" ] && [ "$HADOOP_VERSION" == "none" ]; then
  make_binary_release "" "-DwithoutHadoop" "2.12"
  for i in "${!HADOOP_CLASSIFIERS[@]}"; do
    make_binary_release "hadoop${HADOOP_CLASSIFIERS[$i]}" "-Dhadoop.version=${HADOOP_VERSIONS[$i]}" "2.12"
  done
  make_binary_release "" "-DwithoutHadoop" "2.11"
  for i in "${!HADOOP_CLASSIFIERS[@]}"; do
    make_binary_release "hadoop${HADOOP_CLASSIFIERS[$i]}" "-Dhadoop.version=${HADOOP_VERSIONS[$i]}" "2.11"
  done
elif [ "$SCALA_VERSION" == none ] && [ "$HADOOP_VERSION" != "none" ]
then
  make_binary_release "hadoop2" "-Dhadoop.version=$HADOOP_VERSION" "2.11"
elif [ "$SCALA_VERSION" != none ] && [ "$HADOOP_VERSION" == "none" ]
then
  make_binary_release "" "-DwithoutHadoop" "$SCALA_VERSION"
  for i in "${!HADOOP_CLASSIFIERS[@]}"; do
    make_binary_release "hadoop${HADOOP_CLASSIFIERS[$i]}" "-Dhadoop.version=${HADOOP_VERSIONS[$i]}" "$SCALA_VERSION"
  done
else
  make_binary_release "hadoop2x" "-Dhadoop.version=$HADOOP_VERSION" "$SCALA_VERSION"
fi
