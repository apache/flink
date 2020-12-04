#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

#
# This file contains tooling for compiling Flink
#

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
    exit 1  # fail
fi
CI_DIR="$HERE/../ci"
MVN_CLEAN_COMPILE_OUT="/tmp/clean_compile.out"

# source required ci scripts
source "${CI_DIR}/stage.sh"
source "${CI_DIR}/shade.sh"
source "${CI_DIR}/maven-utils.sh"

echo "Maven version:"
run_mvn -version

echo "=============================================================================="
echo "Compiling Flink"
echo "=============================================================================="

EXIT_CODE=0

run_mvn clean install $MAVEN_OPTS -Dflink.convergence.phase=install -Pcheck-convergence -Dflink.forkCount=2 \
    -Dflink.forkCountTestPackage=2 -Dmaven.javadoc.skip=true -U -DskipTests | tee $MVN_CLEAN_COMPILE_OUT

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "Compiling Flink failed."
    echo "=============================================================================="

    grep "0 Unknown Licenses" target/rat.txt > /dev/null

    if [ $? != 0 ]; then
        echo "License header check failure detected. Printing first 50 lines for convenience:"
        head -n 50 target/rat.txt
    fi

    exit $EXIT_CODE
fi

echo "============ Checking Javadocs ============"

# use the same invocation as on buildbot (https://svn.apache.org/repos/infra/infrastructure/buildbot/aegis/buildmaster/master1/projects/flink.conf)
run_mvn javadoc:aggregate -Paggregate-scaladoc -DadditionalJOption='-Xdoclint:none' \
      -Dmaven.javadoc.failOnError=false -Dcheckstyle.skip=true -Denforcer.skip=true \
      -Dheader=someTestHeader > javadoc.out
EXIT_CODE=$?
if [ $EXIT_CODE != 0 ] ; then
  echo "ERROR in Javadocs. Printing full output:"
  cat javadoc.out ; rm javadoc.out
  exit $EXIT_CODE
fi

echo "============ Checking Scaladocs ============"

cd flink-scala
run_mvn scala:doc 2> scaladoc.out
EXIT_CODE=$?
if [ $EXIT_CODE != 0 ] ; then
  echo "ERROR in Scaladocs. Printing full output:"
  cat scaladoc.out ; rm scaladoc.out
  exit $EXIT_CODE
fi
cd ..

echo "============ Checking scala suffixes ============"

${CI_DIR}/verify_scala_suffixes.sh "${PROFILE}" || exit $?

echo "============ Checking shaded dependencies ============"

check_shaded_artifacts
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_s3_fs hadoop
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_s3_fs presto
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_connector_elasticsearch 5
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_connector_elasticsearch 6
EXIT_CODE=$(($EXIT_CODE+$?))

echo "============ Run license check ============"

if [[ ${PROFILE} != *"scala-2.12"* ]]; then
  ${CI_DIR}/license_check.sh $MVN_CLEAN_COMPILE_OUT $CI_DIR $(pwd) || exit $?
fi

exit $EXIT_CODE

