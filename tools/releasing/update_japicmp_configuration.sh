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

if [ -z "${NEW_VERSION:-}" ]; then
    echo "NEW_VERSION was not set."
    exit 1
fi

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

# Idealized use-cases:
# Scenario A) New major release X.Y.0
#   Premise:
#     There is a master branch with a version X.Y-SNAPSHOT, with a japicmp reference version of X.(Y-1).0 .
#   Release flow:
#     - update the master to X.(Y+1)-SNAPSHOT, but keep the reference version intact since X.Y.0 is not released (yet)
#     - create snapshot branch for X.Y-SNAPSHOT (i.e. release-X.Y), but keep the reference version intact since X.Y.0 is not released (yet)
#     - release X.Y.0
#     - update the japicmp reference version of both master and the snapshot branch for X.Y-SNAPSHOT to X.Y.0
#     - enable stronger compatibility constraints for X.Y-SNAPSHOT in the snapshot branch to ensure compatibility for PublicEvolving
# Scenario B) New minor release X.Y.Z
#   Premise:
#     There is a snapshot branch release-X.Y having a version X.Y-SNAPSHOT, with a japicmp reference version of X.Y.(Z-1)
#   Release flow:
#     - create X.Y.Z-rc* branch
#     - update the japicmp reference version of X.Y.Z to X.Y.(Z-1)
#     - release X.Y.Z
#     - update the japicmp reference version of X.Y-SNAPSHOT (in the snapshot branch release-X.Y) to X.Y.Z

POM=../pom.xml
function enable_public_evolving_compatibility_checks() {
  perl -pi -e 's#<!--(<include>\@org.apache.flink.annotation.PublicEvolving</include>)-->#${1}#' ${POM}
  perl -pi -e 's#\t+<exclude>\@org.apache.flink.annotation.PublicEvolving.*\n##' ${POM}
}

function set_japicmp_reference_version() {
  local version=$1

  perl -pi -e 's#(<japicmp.referenceVersion>).*(</japicmp.referenceVersion>)#${1}'${version}'${2}#' ${POM}
}

function clear_exclusions() {
  exclusion_start=$(($(sed -n '/<!-- MARKER: start exclusions/=' ${POM}) + 1))
  exclusion_end=$(($(sed -n '/<!-- MARKER: end exclusions/=' ${POM}) - 1))

  if [[ $exclusion_start -lt $exclusion_end ]]; then
    sed -i "${exclusion_start},${exclusion_end}d" ${POM}
  fi
}

current_branch=$(git rev-parse --abbrev-ref HEAD)

if [[ ${current_branch} =~ -rc ]]; then
  # release branch
  version_prefix=$(echo "${NEW_VERSION}" | perl -p -e 's#(\d+\.\d+)\.\d+#$1#')
  minor=$(echo "${NEW_VERSION}" | perl -p -e 's#\d+\.\d+\.(\d+)#$1#')
  if ! [[ ${minor} == "0" ]]; then
    set_japicmp_reference_version ${version_prefix}.$((minor - 1))
    # this is a safeguard in case the manual step of enabling checks after the X.Y.0 release was forgotten
    enable_public_evolving_compatibility_checks
  fi
elif [[ ${current_branch} =~ ^master$ ]]; then
  # master branch
  set_japicmp_reference_version ${NEW_VERSION}
  clear_exclusions
elif [[ ${current_branch} =~ ^release- ]]; then
  # snapshot branch
  set_japicmp_reference_version ${NEW_VERSION}
  enable_public_evolving_compatibility_checks
  clear_exclusions
else
  echo "Script was called from unexpected branch ${current_branch}; should be rc/snapshot/master branch."
  exit 1
fi
