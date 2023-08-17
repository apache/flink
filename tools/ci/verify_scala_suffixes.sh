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
## Checks for correct Scala suffixes on the Maven artifact names
##
#
# Flink must only use one version of the Scala library in its classpath. The
# Scala library is compatible across minor versions, i.e. any 2.10.X release
# may be freely mixed. However, 2.X and 2.Y are not compatible with each other.
# That's why we suffix the Maven modules which depend on Scala with the Scala
# major release version.
#
# This script uses Maven's dependency plugin to get a list of modules which
# depend on the Scala library. Further, it checks whether all modules have
# correct suffixes, i.e. modules with Scala dependency should carry a suffix
# but modules without Scala should be suffix-free.
#
# Prior to executing the script it is advised to run 'mvn clean install -DskipTests'
# to build and install the latest version of Flink.
#
# The script uses 'mvn dependency:tree -Dincludes=org.scala-lang' to list Scala
# dependent modules.


usage() {
  echo "Usage: $0"
  echo ""
  echo "The environment variable MVN is used to specify the Maven binaries; defaults to 'mvnw'."
  echo "See further details in the JavaDoc of ScalaSuffixChecker."
}

while getopts 'h' o; do
  case "${o}" in
    h)
      usage
      exit 0
      ;;
  esac
done

MVN=${MVN:-./mvnw}

echo "--- Flink Scala Dependency Analyzer ---"
echo "Analyzing modules for Scala dependencies using 'mvn dependency:tree'."
echo "If you haven't built the project, please do so first by running \"mvn clean install -DskipTests\""

dependency_plugin_output=/tmp/dependency_tree_scala.txt

# run with -T1 because our maven output parsers don't support multi-threaded builds
$MVN dependency:tree -Dincludes=org.scala-lang,:*_2.1*:: ${MAVEN_ARGUMENTS} -T1 > "${dependency_plugin_output}"
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    cat ${dependency_plugin_output}
    echo "=============================================================================="
    echo "Suffix Check failed. The dependency tree could not be determined. See previous output for details."
    echo "=============================================================================="
    exit $EXIT_CODE
fi

$MVN -pl tools/ci/flink-ci-tools exec:java exec:java -Dexec.mainClass=org.apache.flink.tools.ci.suffixcheck.ScalaSuffixChecker -Dexec.args="${dependency_plugin_output} $(pwd)"
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "Suffix Check failed. See previous output for details."
    echo "=============================================================================="
    exit $EXIT_CODE
fi

exit 0

