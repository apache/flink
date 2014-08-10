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

# Quick-and-dirty automation of making maven and binary releases. Not robust at all.
# Publishes releases to Maven and packages/copies binary release artifacts.
# Expects to be run in a totally empty directory.
#

#
#  NOTE: The code in this file is based on code from the Apache Spark
#  project, licensed under the Apache License v 2.0
#
#  https://github.com/apache/spark/blob/master/dev/create-release/create-release.sh
#
CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

GIT_USERNAME=${GIT_USERNAME:-pwendell}
GIT_PASSWORD=${GIT_PASSWORD:-XXX}
GPG_PASSPHRASE=${GPG_PASSPHRASE:-XXX}
GPG_KEY=${GPG_KEY:-XXX}
GIT_BRANCH=${GIT_BRANCH:-branch-1.0}
RELEASE_VERSION=${NEW_VERSION}
USER_NAME=${USER_NAME:-pwendell}
MVN=${MVN:-mvn}

set -e

# create source package

git clone https://github.com/rmetzger/incubator-flink.git flink
cd flink
git checkout -b release-0.6 origin/release-0.6
rm .gitignore
rm .travis.yml
rm deploysettings.xml
rm CHANGELOG
rm -rf .git
cd ..


cp -r flink flink-$RELEASE_VERSION
tar cvzf flink-$RELEASE_VERSION.tgz flink-$RELEASE_VERSION
echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY --passphrase-fd 0 --armour --output flink-$RELEASE_VERSION.tgz.asc \
  --detach-sig flink-$RELEASE_VERSION.tgz
echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY --passphrase-fd 0 --print-md MD5 flink-$RELEASE_VERSION.tgz > \
  flink-$RELEASE_VERSION.tgz.md5
echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY --passphrase-fd 0 --print-md SHA512 flink-$RELEASE_VERSION.tgz > \
  flink-$RELEASE_VERSION.tgz.sha
rm -rf flink-$RELEASE_VERSION


make_binary_release() {
  NAME=$1
  FLAGS=$2
  cp -r flink flink-$RELEASE_VERSION-bin-$NAME
  
  cd flink-$RELEASE_VERSION-bin-$NAME
  # make distribution
  $MVN clean package $FLAGS -DskipTests
  cd flink-dist/target/flink-$RELEASE_VERSION-bin/
  tar cvzf flink-$RELEASE_VERSION-bin-$NAME.tgz flink-$RELEASE_VERSION
  if [ -d "flink-yarn-$RELEASE_VERSION" ] ; then
    tar cvzf flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz flink-yarn-$RELEASE_VERSION
  fi
  cp flink-*.tgz ../../../../
  cd ../../../../
  rm -rf flink-$RELEASE_VERSION

  # Sign md5 and sha the tgz
  echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY \
    --passphrase-fd 0 --armour \
    --output flink-$RELEASE_VERSION-bin-$NAME.tgz.asc \
    --detach-sig flink-$RELEASE_VERSION-bin-$NAME.tgz
  echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY \
    --passphrase-fd 0 --print-md \
    MD5 flink-$RELEASE_VERSION-bin-$NAME.tgz > \
    flink-$RELEASE_VERSION-bin-$NAME.tgz.md5
  echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY \
    --passphrase-fd 0 --print-md \
    SHA512 flink-$RELEASE_VERSION-bin-$NAME.tgz > \
    flink-$RELEASE_VERSION-bin-$NAME.tgz.sha

  if [ -f "flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz" ] ; then
    echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY \
    --passphrase-fd 0 --armour \
    --output flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz.asc \
    --detach-sig flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz
    echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY \
      --passphrase-fd 0 --print-md \
      MD5 flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz > \
      flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz.md5
    echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY \
      --passphrase-fd 0 --print-md \
      SHA512 flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz > \
      flink-$RELEASE_VERSION-bin-$NAME-yarn.tgz.sha
  fi
}

make_binary_release "hadoop1" ""
make_binary_release "cdh4" "-Pvendor-repos,cdh4 -Dhadoop.version=2.0.0-mr1-cdh4.2.0 -Dhadoop.cdh4.hadoop.version=2.0.0-cdh4.2.0"
# make_binary_release "cdh5" "-Dhadoop.profile=2 -Pvendor-repos -Dhadoop.version=2.3.0+cdh5.1.0+795"
make_binary_release "hadoop2" "-Dhadoop.profile=2"
# make_binary_release "mapr4" "-Dhadoop.profile=2 -Pvendor-repos -Dhadoop.version=2.3.0-mapr-4.0.0-FCS"
# make_binary_release "hdp21" "-Dhadoop.profile=2 -Pvendor-repos -Dhadoop.version=2.4.0.2.1.3.0-563"


# Copy data
echo "Copying release tarballs"
folder=flink-$RELEASE_VERSION
ssh $USER_NAME@people.apache.org \
  mkdir /home/$USER_NAME/public_html/$folder
scp flink-* \
  $USER_NAME@people.apache.org:/home/$USER_NAME/public_html/$folder/

