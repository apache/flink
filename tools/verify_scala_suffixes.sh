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
# [INFO] ------------------------------------------------------------------------
# [INFO] Building flink-java 1.0-SNAPSHOT
# [INFO] ------------------------------------------------------------------------
# [INFO]
# [INFO] --- maven-dependency-plugin:2.8:tree (default-cli) @ flink-java ---
# [INFO]

### Example of a Scala dependent module:
# [INFO] ------------------------------------------------------------------------
# [INFO] Building flink-storm 1.0-SNAPSHOT
# [INFO] ------------------------------------------------------------------------
# [INFO]
# [INFO] --- maven-dependency-plugin:2.8:tree (default-cli) @ flink-storm ---
# [INFO] org.apache.flink:flink-storm:jar:1.0-SNAPSHOT
# [INFO] \- org.apache.flink:flink-streaming-java:jar:1.0-SNAPSHOT:compile
# [INFO]    \- org.apache.flink:flink-runtime:jar:1.0-SNAPSHOT:compile
# [INFO]       \- org.scala-lang:scala-library:jar:2.10.4:compile
# [INFO]


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

echo "Analyzing modules for Scala dependencies using 'mvn dependency:tree'."
echo "If you haven't built the project, please do so first by running \"mvn clean install -DskipTests\""

infected=""
clean=""

while read line; do
    if [[ $line == "$BEGIN"* ]]; then
        reached_block=1
        # Maven module name
        block_name=`[[ "$line" =~ .*(flink-?[-a-zA-Z0-9.]*).* ]] && echo ${BASH_REMATCH[1]}`
    elif [[ $line == "$SEPARATOR" ]] && [[ $reached_block -eq 1 ]]; then
        reached_block=0
        in_block=1
    elif [[ $line == "$END" ]] && [[ $in_block -eq 1 ]]; then
        if [[ $block_infected -eq 0 ]]; then
            clean="$block_name $clean"
        fi
        in_block=0
        reached_block=0
        block_name=""
        block_infected=0
    elif [[ $in_block -eq 1 ]]; then
        echo $line | grep org.scala-lang >/dev/null
        if [ $? -eq 0 ]; then
            #echo $block_name
            infected="$block_name $infected"
            block_infected=1
        fi
    fi
done < <(mvn -o dependency:tree -Dincludes=org.scala-lang | tee /dev/tty)


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
    out=`find . -name 'pom.xml' -not -path '*target*' -exec grep "${module}_\d\+\.\d\+</artifactId>" "{}" \;`
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
    out=`find . -name 'pom.xml' -not -path '*target*' -exec grep "${module}</artifactId>" "{}" \;`
    if [[ "$out" == "" ]]; then
        printf "$GREEN OK $NC $module\n"
    else
        printf "$RED NOT OK $NC $module\n"
        echo "$out"
        exit_code=1
    fi
done

exit $exit_code

