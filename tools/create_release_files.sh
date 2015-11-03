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

##
#
#   Flink release script
#   ===================
#
#   Can be called like this:
#
#    sonatype_user=APACHEID sonatype_pw=APACHEIDPASSWORD \
#     NEW_VERSION=0.9.0 \
#     RELEASE_CANDIDATE="rc1" RELEASE_BRANCH=release-0.9.0
#     OLD_VERSION=0.9-SNAPSHOT \
#     USER_NAME=APACHEID \
#     GPG_PASSPHRASE=XXX GPG_KEY=KEYID \
#     GIT_AUTHOR="`git config --get user.name` <`git config --get user.email`>" \
#     ./create_release_files.sh
#
##


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

##
## Variables with defaults (if not overwritten by environment)
##
GPG_PASSPHRASE=${GPG_PASSPHRASE:-XXX}
GPG_KEY=${GPG_KEY:-XXX}
GIT_AUTHOR=${GIT_AUTHOR:-"Your name <you@apache.org>"}
OLD_VERSION=${OLD_VERSION:-0.10-SNAPSHOT}
RELEASE_VERSION=${NEW_VERSION}
RELEASE_CANDIDATE=${RELEASE_CANDIDATE:-rc1}
RELEASE_BRANCH=${RELEASE_BRANCH:-master}
NEW_VERSION_HADOOP1=${NEW_VERSION_HADOOP1:-"$RELEASE_VERSION-hadoop1"}
USER_NAME=${USER_NAME:-yourapacheidhere}
MVN=${MVN:-mvn}
GPG=${GPG:-gpg}
sonatype_user=${sonatype_user:-yourapacheidhere}
sonatype_pw=${sonatype_pw:-XXX}


if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
    MD5SUM="md5 -r"
else
    SHASUM="sha512sum"
    MD5SUM="md5sum"
fi


prepare() {
  # prepare
  git clone http://git-wip-us.apache.org/repos/asf/flink.git flink
  cd flink
  git checkout -b "release-$RELEASE_VERSION-$RELEASE_CANDIDATE" origin/$RELEASE_BRANCH
  rm -f .gitignore
  rm -f .travis.yml
  rm -f deploysettings.xml
  rm -f CHANGELOG
  cd ..
}

# create source package
make_source_release() {

  cd flink

  #change version in all pom files
  find . -name 'pom.xml' -type f -exec perl -pi -e 's#<version>'$OLD_VERSION'</version>#<version>'$NEW_VERSION'</version>#' {} \;

  #change version in quickstart archetypes
  find . -name 'pom.xml' -type f -exec perl -pi -e 's#<flink.version>'$OLD_VERSION'</flink.version>#<flink.version>'$NEW_VERSION'</flink.version>#' {} \;

  #change version of documentation
  cd docs
  perl -pi -e "s#^version: .*#version: ${NEW_VERSION}#" _config.yml
  perl -pi -e "s#^version_hadoop1: .*#version_hadoop1: ${NEW_VERSION}-hadoop1#" _config.yml
  perl -pi -e "s#^version_short: .*#version_short: ${NEW_VERSION}#" _config.yml
  cd ..

  git commit --author="$GIT_AUTHOR" -am "Commit for release $RELEASE_VERSION"
  git remote add asf_push https://$USER_NAME@git-wip-us.apache.org/repos/asf/flink.git
  RELEASE_HASH=`git rev-parse HEAD`
  echo "Echo created release hash $RELEASE_HASH"

  cd ..

  echo "Creating source package"
  rsync -a --exclude ".git" flink/ flink-$RELEASE_VERSION
  tar czf flink-${RELEASE_VERSION}-src.tgz flink-$RELEASE_VERSION
  echo $GPG_PASSPHRASE | $GPG --batch --default-key $GPG_KEY --passphrase-fd 0 --armour --output flink-$RELEASE_VERSION-src.tgz.asc \
    --detach-sig flink-$RELEASE_VERSION-src.tgz
  $MD5SUM flink-$RELEASE_VERSION-src.tgz > flink-$RELEASE_VERSION-src.tgz.md5
  $SHASUM flink-$RELEASE_VERSION-src.tgz > flink-$RELEASE_VERSION-src.tgz.sha
  rm -rf flink-$RELEASE_VERSION
}


make_binary_release() {
  NAME=$1
  FLAGS=$2
  SCALA_VERSION=$3

  echo "Creating binary release name: $NAME, flags: $FLAGS, SCALA_VERSION: ${SCALA_VERSION}"
  dir_name="flink-$RELEASE_VERSION-bin-$NAME-scala_${SCALA_VERSION}"
  rsync -a --exclude "flink/.git" flink/ "${dir_name}"

  # make distribution
  cd "${dir_name}"
  ./tools/change-scala-version.sh ${SCALA_VERSION}

  $MVN clean package $FLAGS -DskipTests

  cd flink-dist/target/flink-$RELEASE_VERSION-bin/
  tar czf "${dir_name}.tgz" flink-$RELEASE_VERSION

  cp flink-*.tgz ../../../../
  cd ../../../../

  # Sign md5 and sha the tgz
  echo $GPG_PASSPHRASE | $GPG --batch --default-key $GPG_KEY \
    --passphrase-fd 0 --armour \
    --output "${dir_name}.tgz.asc" \
    --detach-sig "${dir_name}.tgz"
  $MD5SUM "${dir_name}.tgz" > "${dir_name}.tgz.md5"
  $SHASUM "${dir_name}.tgz" > "${dir_name}.tgz.sha"

}

deploy_to_maven() {
  echo "Deploying to repository.apache.org"

  cd flink
  cp ../../deploysettings.xml .
  echo "For your reference, the command:\n\t $MVN clean deploy -Prelease --settings deploysettings.xml -DskipTests -Dgpg.keyname=$GPG_KEY -Dgpg.passphrase=$GPG_PASSPHRASE ./tools/generate_specific_pom.sh $NEW_VERSION $NEW_VERSION_HADOOP1 pom.xml"
  $MVN clean deploy -Prelease,docs-and-source --settings deploysettings.xml -DskipTests -Dgpg.executable=$GPG -Dgpg.keyname=$GPG_KEY -Dgpg.passphrase=$GPG_PASSPHRASE -DretryFailedDeploymentCount=10
  cd tools && ./change-scala-version.sh 2.11 && cd ..
  $MVN clean deploy -Dgpg.executable=$GPG -Prelease,docs-and-source --settings deploysettings.xml -DskipTests -Dgpg.keyname=$GPG_KEY -Dgpg.passphrase=$GPG_PASSPHRASE -DretryFailedDeploymentCount=10
  cd tools && ../change-scala-version.sh 2.10 && cd ..
  ../generate_specific_pom.sh $NEW_VERSION $NEW_VERSION_HADOOP1 pom.xml
  sleep 4
  $MVN clean deploy -Dgpg.executable=$GPG -Prelease,docs-and-source --settings deploysettings.xml -DskipTests -Dgpg.keyname=$GPG_KEY -Dgpg.passphrase=$GPG_PASSPHRASE -DretryFailedDeploymentCount=10
}

copy_data() {
  # Copy data
  echo "Copying release tarballs"
  folder=flink-$RELEASE_VERSION-$RELEASE_CANDIDATE
  ssh $USER_NAME@people.apache.org mkdir -p /home/$USER_NAME/public_html/$folder
  rsync flink-*.tgz* $USER_NAME@people.apache.org:/home/$USER_NAME/public_html/$folder/
  echo "copy done"
}

prepare

make_source_release

make_binary_release "hadoop1" "-Dhadoop.profile=1" 2.10
make_binary_release "hadoop2" "" 2.10
make_binary_release "hadoop24" "-Dhadoop.version=2.4.1" 2.10
make_binary_release "hadoop26" "-Dhadoop.version=2.6.0" 2.10
make_binary_release "hadoop27" "-Dhadoop.version=2.7.0" 2.10
## make_binary_release "mapr4" "-Dhadoop.profile=2 -Pvendor-repos -Dhadoop.version=2.3.0-mapr-4.0.0-FCS"

make_binary_release "hadoop2" "" 2.11
make_binary_release "hadoop24" "-Dhadoop.version=2.4.1" 2.11
make_binary_release "hadoop26" "-Dhadoop.version=2.6.0" 2.11
make_binary_release "hadoop27" "-Dhadoop.version=2.7.0" 2.11
# make_binary_release "mapr4" "-Dhadoop.profile=2 -Pvendor-repos -Dhadoop.version=2.3.0-mapr-4.0.0-FCS"

copy_data

deploy_to_maven


echo "Done. Don't forget to commit the release version"
