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
RELEASE_VERSION=${RELEASE_VERSION:-1.3-SNAPSHOT}
SCALA_VERSION=none
HADOOP_VERSION=none
SKIP_GPG=${SKIP_GPG:-false}
MVN=${MVN:-mvn}

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
    MD5SUM="md5 -r"
else
    SHASUM="sha512sum"
    MD5SUM="md5sum"
fi

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

  # enable release profile here (to check for the maven version)
  $MVN clean package $FLAGS -DskipTests -Prelease,scala-${SCALA_VERSION} -Dgpg.skip

  cd flink-dist/target/flink-*-bin/
  tar czf "${dir_name}.tgz" flink-*

  cp flink-*.tgz ../../../
  cd ../../../

  # Sign md5 and sha the tgz
  if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig "${dir_name}.tgz"
  fi
  $MD5SUM "${dir_name}.tgz" > "${dir_name}.tgz.md5"
  $SHASUM "${dir_name}.tgz" > "${dir_name}.tgz.sha"
}

cd ..


if [ "$SCALA_VERSION" == "none" ] && [ "$HADOOP_VERSION" == "none" ]; then
  make_binary_release "" "-DwithoutHadoop" "2.11"
  make_binary_release "hadoop24" "-Dhadoop.version=2.4.1" "2.11"
  make_binary_release "hadoop26" "-Dhadoop.version=2.6.5" "2.11"
  make_binary_release "hadoop27" "-Dhadoop.version=2.7.3" "2.11"
  make_binary_release "hadoop28" "-Dhadoop.version=2.8.0" "2.11"
elif [ "$SCALA_VERSION" == none ] && [ "$HADOOP_VERSION" != "none" ]
then
  make_binary_release "hadoop2" "-Dhadoop.version=$HADOOP_VERSION" "2.11"
elif [ "$SCALA_VERSION" != none ] && [ "$HADOOP_VERSION" == "none" ]
then
  make_binary_release "" "-DwithoutHadoop" "$SCALA_VERSION"
  make_binary_release "hadoop24" "-Dhadoop.version=2.4.1" "$SCALA_VERSION"
  make_binary_release "hadoop26" "-Dhadoop.version=2.6.5" "$SCALA_VERSION"
  make_binary_release "hadoop27" "-Dhadoop.version=2.7.3" "$SCALA_VERSION"
  make_binary_release "hadoop28" "-Dhadoop.version=2.8.0" "$SCALA_VERSION"
else
  make_binary_release "hadoop2x" "-Dhadoop.version=$HADOOP_VERSION" "$SCALA_VERSION"
fi
