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

cd ..

echo "Creating source package"

# create a temporary git clone to ensure that we have a pristine source release
git clone . flink-tmp-clone
cd flink-tmp-clone

rsync -a \
  --exclude ".git" --exclude ".gitignore" --exclude ".gitattributes" --exclude ".travis.yml" \
  --exclude "deploysettings.xml" --exclude "CHANGELOG" --exclude ".github" --exclude "target" \
  --exclude ".idea" --exclude "*.iml" --exclude ".DS_Store" --exclude "build-target" \
  --exclude "docs/content" --exclude ".rubydeps" \
  . flink-$RELEASE_VERSION

tar czf flink-${RELEASE_VERSION}-src.tgz flink-$RELEASE_VERSION
gpg --armor --detach-sig flink-$RELEASE_VERSION-src.tgz
$MD5SUM flink-$RELEASE_VERSION-src.tgz > flink-$RELEASE_VERSION-src.tgz.md5
$SHASUM flink-$RELEASE_VERSION-src.tgz > flink-$RELEASE_VERSION-src.tgz.sha

mv flink-$RELEASE_VERSION-src.* ../
cd ..
rm -rf flink-tmp-clone
