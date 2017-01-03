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
  FROM_SUFFIX="_2\.10"
  TO_SUFFIX="_2\.11"
else
  FROM_SUFFIX="_2\.11"
  TO_SUFFIX="_2\.10"
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
find "$BASEDIR/flink-examples/flink-examples-batch" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<copy file=\".*flink-examples-batch\)'$FROM_SUFFIX'/\1'$TO_SUFFIX'/g' {}" \;

find "$BASEDIR/flink-examples/flink-examples-streaming" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<copy file=\".*flink-examples-streaming\)'$FROM_SUFFIX'/\1'$TO_SUFFIX'/g' {}" \;

# fix for quickstart
find "$BASEDIR/flink-quickstart" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<exclude>org\.apache\.flink:flink-.*\)'$FROM_SUFFIX'<\/exclude>/\1'$TO_SUFFIX'<\/exclude>/g' {}" \;

# fix for flink-dist (bin.xml)
find "$BASEDIR/flink-dist" -name 'bin.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<source>.*flink-dist\)'$FROM_SUFFIX'/\1'$TO_SUFFIX'/g' {}" \;
find "$BASEDIR/flink-dist" -name 'bin.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<include>org\.apache\.flink:flink-.*\)'$FROM_SUFFIX'<\/include>/\1'$TO_SUFFIX'<\/include>/g' {}" \;

# fix for flink-dist (opt.xml)
find "$BASEDIR/flink-dist" -name 'opt.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<source>.*flink-.*\)'$FROM_SUFFIX'/\1'$TO_SUFFIX'/g' {}" \;
find "$BASEDIR/flink-dist" -name 'opt.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<destName>flink-.*\)'$FROM_SUFFIX'/\1'$TO_SUFFIX'/g' {}" \;

# fix for shading curator with Scala 2.11
find "$BASEDIR/flink-runtime" -name 'pom.xml' -not -path '*target*' -print \
     -exec bash -c "sed_i 's/\(<include>org\.apache\.flink:flink-shaded-curator.*\)'$FROM_SUFFIX'<\/include>/\1'$TO_SUFFIX'<\/include>/g' {}" \;

if [ "$TO_VERSION" == "2.11" ]; then
  # set the profile activation to !scala-2.11 in parent pom, so that it activates by default
  bash -c "sed_i 's/<name>scala-2.11<\/name>/<name>!scala-2.11<\/name>/g' $BASEDIR/pom.xml" \;
  # set the profile activation in all sub modules to scala-2.11 (so that they are disabled by default)
  find $BASEDIR/flink-* -name 'pom.xml' -not -path '*target*' -print \
    -exec bash -c "sed_i 's/<name>!scala-2.11<\/name>/<name>scala-2.11<\/name>/g' {}" \;

  # set the name of the shading artifact properly
  bash -c "sed_i 's/\(shading-artifact.name>flink-shaded[a-z0-9\-]*\)'$FROM_SUFFIX'<\/shading-artifact.name>/\1'$TO_SUFFIX'<\/shading-artifact.name>/g' $BASEDIR/pom.xml" \;
fi

if [ "$TO_VERSION" == "2.10" ]; then
  # do the opposite as above
  bash -c "sed_i 's/<name>!scala-2.11<\/name>/<name>scala-2.11<\/name>/g' $BASEDIR/pom.xml" \;
  # also for the other files
  find $BASEDIR/flink-* -name 'pom.xml' -not -path '*target*' -print \
    -exec bash -c "sed_i 's/<name>scala-2.11<\/name>/<name>!scala-2.11<\/name>/g' {}" \;

  # unset shading artifact name
  bash -c "sed_i 's/\(shading-artifact.name>flink-shaded[a-z0-9\-]*\)'$FROM_SUFFIX'<\/shading-artifact.name>/\1'$TO_SUFFIX'<\/shading-artifact.name>/g' $BASEDIR/pom.xml" \;
fi

