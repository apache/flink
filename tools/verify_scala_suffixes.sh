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
# The script may be run from the /tools directory or the Flink root. Prior to
# executing the script it is advised to run 'mvn clean install -DskipTests' to
# build and install the latest version of Flink.
#
# The script uses 'mvn dependency:tree -Dincludes=org.scala-lang' to list Scala
# dependent modules.
#
# Some example output:
#
### Example of a dependency-free module:
# [INFO] --------------------< org.apache.flink:flink-java >---------------------
# [INFO] Building flink-java 1.10-SNAPSHOT                               [10/166]
# [INFO] --------------------------------[ jar ]---------------------------------
# [INFO]
# [INFO] --- maven-dependency-plugin:3.1.1:tree (default-cli) @ flink-java ---
# [INFO]

### Example of a Scala dependent module:
# [INFO] -----------------< org.apache.flink:flink-scala_2.11 >------------------
# [INFO] Building flink-scala 1.10-SNAPSHOT                              [16/166]
# [INFO] --------------------------------[ jar ]---------------------------------
# [INFO]
# [INFO] --- maven-dependency-plugin:3.1.1:tree (default-cli) @ flink-scala_2.11 ---
# [INFO] org.apache.flink:flink-scala_2.11:jar:1.10-SNAPSHOT
# [INFO] +- org.scala-lang:scala-reflect:jar:2.11.12:compile
# [INFO] +- org.scala-lang:scala-library:jar:2.11.12:compile
# [INFO] +- org.scala-lang:scala-compiler:jar:2.11.12:compile
# [INFO] |  +- org.scala-lang.modules:scala-xml_2.11:jar:1.0.5:compile
# [INFO] |  \- org.scala-lang.modules:scala-parser-combinators_2.11:jar:1.1.1:compile
# [INFO] +- org.scalatest:scalatest_2.11:jar:3.0.0:test
# [INFO] |  \- org.scalactic:scalactic_2.11:jar:3.0.0:test
# [INFO] +- com.twitter:chill_2.11:jar:0.7.6:test
# [INFO] \- org.apache.flink:flink-runtime_2.11:jar:1.10-SNAPSHOT:test
# [INFO]    +- com.typesafe.akka:akka-actor_2.11:jar:2.5.21:test
# [INFO]    |  \- org.scala-lang.modules:scala-java8-compat_2.11:jar:0.7.0:test
# [INFO]    +- com.typesafe.akka:akka-remote_2.11:jar:2.5.21:test
# [INFO]    +- com.typesafe.akka:akka-stream_2.11:jar:2.5.21:test
# [INFO]    |  \- com.typesafe:ssl-config-core_2.11:jar:0.3.7:test
# [INFO]    +- com.typesafe.akka:akka-protobuf_2.11:jar:2.5.21:test
# [INFO]    +- com.typesafe.akka:akka-slf4j_2.11:jar:2.5.21:test
# [INFO]    +- org.clapper:grizzled-slf4j_2.11:jar:1.3.2:test
# [INFO]    \- com.github.scopt:scopt_2.11:jar:3.5.0:test
# [INFO]

MAVEN_ARGUMENTS=${1:-""}

if [[ `basename $PWD` == "tools" ]] ; then
    cd ..
fi

echo "--- Flink Scala Dependency Analyzer ---"

# Parser for mvn:dependency output

BEGIN="[INFO] Building flink"
SEPARATOR="[INFO] ------------------------------------------------------------------------"
END=$SEPARATOR

reached_block=0
in_block=0
block_name=""
block_infected=0

# exclude e2e modules and flink-docs for convenience as they
# a) are not deployed during a release
# b) exist only for dev purposes
# c) no-one should depend on them
e2e_modules=$(find flink-end-to-end-tests -mindepth 2 -maxdepth 5 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')
excluded_modules=\!${e2e_modules//,/,\!},!flink-docs

echo "Analyzing modules for Scala dependencies using 'mvn dependency:tree'."
echo "If you haven't built the project, please do so first by running \"mvn clean install -DskipTests\""

infected=""
clean=""

while read line; do
    if [[ $line == *"$BEGIN"* ]]; then
        reached_block=1
        # Maven module name
        block_name=`[[ "$line" =~ .*(flink-?[-a-zA-Z0-9.]*).* ]] && echo ${BASH_REMATCH[1]}`
    elif [[ $line == *"$SEPARATOR"* ]] && [[ $reached_block -eq 1 ]]; then
        reached_block=0
        in_block=1
    elif [[ $line == *"$END"* ]] && [[ $in_block -eq 1 ]]; then
        if [[ $block_infected -eq 0 ]]; then
            clean="$block_name $clean"
        fi
        in_block=0
        reached_block=0
        block_name=""
        block_infected=0
    elif [[ $in_block -eq 1 ]]; then
        echo $line | grep -E "org.scala-lang|- [^:]+:[^:]+_2\.1[0-9]" | grep --invert-match "org.scala-lang.*:.*:.*:test" | grep --invert-match "[^:]*:[^:]*_2\.1[0-9]:.*:.*:test" >/dev/null
        if [ $? -eq 0 ]; then
            #echo $block_name
            infected="$block_name $infected"
            block_infected=1
        fi
    fi
done < <(mvn -nsu dependency:tree -Dincludes=org.scala-lang,:*_2.1*:: -pl ${excluded_modules} ${MAVEN_ARGUMENTS} | tee /dev/tty)


# deduplicate and sort
clean=`echo $clean | xargs -n1 | sort -u`

# deduplicate and sort
infected=`echo $infected | xargs -n1 | sort -u`


# #
# ### Check whether the suffixes are correct
# #

RED='\e[0;31m'
GREEN='\e[0;32m'
NC='\033[0m'

exit_code=0

echo
echo "Checking Scala-free modules:"

for module in $clean; do
    out=`find . -maxdepth 3 -name 'pom.xml' -not -path '*target*' -exec grep "${module}_\\${scala.binary.version}</artifactId>" "{}" \;`
    if [[ "$out" == "" ]]; then
        printf "$GREEN OK $NC $module\n"
    else
        printf "$RED NOT OK $NC $module\n"
        echo "$out"
        exit_code=1
    fi
done

echo
echo "Checking Scala-dependent modules:"

for module in $infected; do
    out=`find . -maxdepth 3 -name 'pom.xml' -not -path '*target*' -exec grep "${module}</artifactId>" "{}" \;`
    if [[ "$out" == "" ]]; then
        printf "$GREEN OK $NC $module\n"
    else
        printf "$RED NOT OK $NC $module\n"
        echo "$out"
        exit_code=1
    fi
done

exit $exit_code

