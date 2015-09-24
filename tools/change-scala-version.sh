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

# This shell script is from Apache Spark with some modification.

set -e

VALID_VERSIONS=( 2.10 2.11 )

usage() {
  echo "Usage: $(basename $0) [-h|--help] <scala version to be used>
where :
  -h| --help Display this help text
  valid scala version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

if [ $TO_VERSION = "2.11" ]; then
  FROM_SUFFIX=""
  TO_SUFFIX="_2.11"
else
  FROM_SUFFIX="_2\.11"
  TO_SUFFIX=""
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i

echo "sed_i 's/\(artifactId>flink.*'$FROM_SUFFIX'\)<\/artifactId>/\1'$TO_SUFFIX'<\/artifactId>/g' {}";

BASEDIR=$(dirname $0)/..
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(artifactId>flink.*\)'$FROM_SUFFIX'<\/artifactId>/\1'$TO_SUFFIX'<\/artifactId>/g' {}" \;

# fix for examples
find "$BASEDIR/flink-examples/flink-java-examples" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<copy file=\".*flink-java-examples\)'$FROM_SUFFIX'/\1'$TO_SUFFIX'/g' {}" \;

# fix for quickstart
find "$BASEDIR/flink-quickstart" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<exclude>org\.apache\.flink:flink-.*\)'$FROM_SUFFIX'<\/exclude>/\1'$TO_SUFFIX'<\/exclude>/g' {}" \;

# fix for flink-dist
find "$BASEDIR/flink-dist" -name 'bin.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<source>.*flink-dist\)'$FROM_SUFFIX'/\1'$TO_SUFFIX'/g' {}" \;
find "$BASEDIR/flink-dist" -name 'bin.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<include>org\.apache\.flink:flink-.*\)'$FROM_SUFFIX'<\/include>/\1'$TO_SUFFIX'<\/include>/g' {}" \;
