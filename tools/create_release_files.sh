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
#     NEW_VERSION=1.2.0 \
#     RELEASE_CANDIDATE="rc1" RELEASE_BRANCH=release-1.2.0
#     OLD_VERSION=1.1-SNAPSHOT \
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
OLD_VERSION=${OLD_VERSION:-1.2-SNAPSHOT}
RELEASE_VERSION=${NEW_VERSION:-1.3-SNAPSHOT}
RELEASE_CANDIDATE=${RELEASE_CANDIDATE:-none}
RELEASE_BRANCH=${RELEASE_BRANCH:-master}
USER_NAME=${USER_NAME:-yourapacheidhere}
MVN=${MVN:-mvn}
GPG=${GPG:-gpg}
sonatype_user=${sonatype_user:-yourapacheidhere}
sonatype_pw=${sonatype_pw:-XXX}
# whether only build the dist local and don't release to apache
IS_LOCAL_DIST=${IS_LOCAL_DIST:-false}
GIT_REPO=${GIT_REPO:-git-wip-us.apache.org/repos/asf/flink.git}
SCALA_VERSION=none
HADOOP_VERSION=none

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
    MD5SUM="md5 -r"
else
    SHASUM="sha512sum"
    MD5SUM="md5sum"
fi

usage() {
  set +x
  echo "./create_release_files.sh --scala-version 2.11 --hadoop-version 2.7.3"
  echo ""
  echo "usage:"
  echo "[--scala-version <version>] [--hadoop-version <version>]"
  echo ""
  echo "example 1: build apache release"
  echo "  sonatype_user=APACHEID sonatype_pw=APACHEIDPASSWORD \ "
  echo "  NEW_VERSION=1.2.0 RELEASE_CANDIDATE="rc1" RELEASE_BRANCH=release-1.2.0 OLD_VERSION=1.1-SNAPSHOT \ "
  echo "  USER_NAME=APACHEID GPG_PASSPHRASE=XXX GPG_KEY=KEYID \ "
  echo "  GIT_AUTHOR=\"`git config --get user.name` <`git config --get user.email`>\" \ "
  echo "  GIT_REPO=github.com/apache/flink.git \ "
  echo "  ./create_release_files.sh --scala-version 2.11 --hadoop-version 2.7.3"
  echo ""
  echo "example 2: build local release"
  echo "  NEW_VERSION=1.2.0 RELEASE_BRANCH=master OLD_VERSION=1.2-SNAPSHOT \ "
  echo "  GPG_PASSPHRASE=XXX GPG_KEY=XXX IS_LOCAL_DIST=true \ "
  echo "  ./create_release_files.sh --scala-version 2.11 --hadoop-version 2.7.3"

  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --scala-version)
      SCALA_VERSION="$2"
      shift
      ;;
    --hadoop-version)
      HADOOP_VERSION="$2"
      shift
      ;;
    --help)
      usage
      ;;
    *)
      break
      ;;
  esac
  shift
done

###########################

prepare() {
  # prepare
  target_branch=release-$RELEASE_VERSION
  if [ "$RELEASE_CANDIDATE" != "none" ]; then
    target_branch=$target_branch-$RELEASE_CANDIDATE
  fi

  if [ ! -d ./flink ]; then
    git clone http://$GIT_REPO flink
  else
    # if flink git repo exist, delete target branch, delete builded distribution
    rm -rf flink-*.tgz
    cd flink
    # try-catch
    {
      git pull --all
      git checkout master
      git branch -D $target_branch -f
    } || {
      echo "branch $target_branch not found"
    }
    cd ..
  fi

  cd flink

  git checkout -b $target_branch origin/$RELEASE_BRANCH
  rm -rf .gitignore .gitattributes .travis.yml deploysettings.xml CHANGELOG .github

  cd ..
}

# create source package
make_source_release() {

  cd flink

  #change version in all pom files
  find . -name 'pom.xml' -type f -exec perl -pi -e 's#<version>'$OLD_VERSION'</version>#<version>'$NEW_VERSION'</version>#' {} \;

  #change version of documentation
  cd docs
  perl -pi -e "s#^version: .*#version: ${NEW_VERSION}#" _config.yml

  # The version in the title should not contain the bugfix version (e.g. 1.3)
  VERSION_TITLE=$(echo $NEW_VERSION | sed 's/\.[^.]*$//')
  perl -pi -e "s#^version_title: .*#version_title: ${VERSION_TITLE}#" _config.yml
  perl -pi -e "s#^version_javadocs: .*#version_javadocs: ${VERSION_TITLE}#" _config.yml
  cd ..

  # local dist have no need to commit to remote
  if [ "$IS_LOCAL_DIST" == "false" ]; then
    git commit --author="$GIT_AUTHOR" -am "Commit for release $RELEASE_VERSION"
    git remote add asf_push https://$USER_NAME@$GIT_REPO
    RELEASE_HASH=`git rev-parse HEAD`
    echo "Echo created release hash $RELEASE_HASH"
  fi

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

# build maven package, create Flink distribution, generate signature
make_binary_release() {
  NAME=$1
  FLAGS=$2
  SCALA_VERSION=$3

  echo "Creating binary release name: $NAME, flags: $FLAGS, SCALA_VERSION: ${SCALA_VERSION}"
  dir_name="flink-$RELEASE_VERSION-bin-$NAME-scala_${SCALA_VERSION}"
  rsync -a --exclude "flink/.git" flink/ "${dir_name}"

  # make distribution
  cd "${dir_name}"

  # enable release profile here (to check for the maven version)
  $MVN clean package $FLAGS -DskipTests -Prelease,scala-${SCALA_VERSION} -Dgpg.skip

  cd flink-dist/target/flink-*-bin/
  tar czf "${dir_name}.tgz" flink-*

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

  echo "Deploying Scala 2.11 version"
  $MVN clean deploy -Prelease,docs-and-source,scala-2.11 --settings deploysettings.xml -DskipTests -Dgpg.executable=$GPG -Dgpg.keyname=$GPG_KEY -Dgpg.passphrase=$GPG_PASSPHRASE -DretryFailedDeploymentCount=10
}

copy_data() {
  # Copy data
  echo "Copying release tarballs"
  folder=flink-$RELEASE_VERSION
  # candidate is not none, append it
  if [ "$RELEASE_CANDIDATE" != "none" ]; then
    folder=$folder-$RELEASE_CANDIDATE
  fi
  sftp $USER_NAME@home.apache.org <<EOF
mkdir public_html/$folder
put flink-*.tgz* public_html/$folder
bye
EOF
  echo "copy done"
}

prepare

make_source_release

# build dist by input parameter of "--scala-vervion xxx --hadoop-version xxx"
if [ "$SCALA_VERSION" == "none" ] && [ "$HADOOP_VERSION" == "none" ]; then
  make_binary_release "hadoop2" "" "2.11"
  make_binary_release "hadoop26" "-Dhadoop.version=2.6.5" "2.11"
  make_binary_release "hadoop27" "-Dhadoop.version=2.7.3" "2.11"
  make_binary_release "hadoop28" "-Dhadoop.version=2.8.0" "2.11"
elif [ "$SCALA_VERSION" == none ] && [ "$HADOOP_VERSION" != "none" ]
then
  make_binary_release "hadoop2" "-Dhadoop.version=$HADOOP_VERSION" "2.11"
elif [ "$SCALA_VERSION" != none ] && [ "$HADOOP_VERSION" == "none" ]
then
  make_binary_release "hadoop2" "" "$SCALA_VERSION"
  make_binary_release "hadoop26" "-Dhadoop.version=2.6.5" "$SCALA_VERSION"
  make_binary_release "hadoop27" "-Dhadoop.version=2.7.3" "$SCALA_VERSION"
  make_binary_release "hadoop28" "-Dhadoop.version=2.8.0" "$SCALA_VERSION"
else
  make_binary_release "hadoop2x" "-Dhadoop.version=$HADOOP_VERSION" "$SCALA_VERSION"
fi

if [ "$IS_LOCAL_DIST" == "false" ] ; then
    copy_data
    deploy_to_maven
fi

echo "Done. Don't forget to commit the release version"
