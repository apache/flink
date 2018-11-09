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

HERE="`dirname \"$0\"`"				# relative
HERE="`( cd \"$HERE\" && pwd )`" 	# absolutized and normalized
if [ -z "$HERE" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi

FLINK_DIR=HERE

if [[ $(basename ${HERE}) == "tools" ]] ; then
  FLINK_DIR="${HERE}/.."
fi

FLINK_DIR="`( cd \"${FLINK_DIR}\" && pwd )`" 

echo ${FLINK_DIR}

# get list of all flink modules
# searches for directories containing a pom.xml file
# sorts the list alphabetically
# only accepts directories starting with "flink" to filter force-shading
modules=$(find . -maxdepth 3 -name 'pom.xml' -printf '%h\n' | sort -u | grep "flink")

for module in ${modules}
do
    # There are no Scala 2.12 dependencies for older Kafka versions
    if [[ $PROFILE == *"scala-2.12"* ]]; then
        if [[ $module == *"kafka-0.8"* ]]; then
            echo "excluding ${module} because we build for Scala 2.12"
            continue 2
        fi
        if [[ $module == *"kafka-0.9"* ]]; then
            echo "excluding ${module} because we build for Scala 2.12"
            continue 2
        fi
    fi

    # we are only interested in child modules
    for other_module in ${modules}
    do 
        if [[ "${other_module}" != "${module}" && "${other_module}" = "${module}"/* ]]; then
        echo "excluding ${module} since it is not a leaf module"
            continue 2
        fi
    done
    
    cd "${module}"
    echo "checking ${module}"
    output=$(mvn validate $PROFILE -nsu -Dcheckstyle.skip=true -Dcheck-convergence)
    exit_code=$?
    if [[ ${exit_code} != 0 ]]; then
        echo "dependency convergence failed."
        echo "${output}"
        exit ${exit_code}
    fi
    cd "${FLINK_DIR}"
done

exit 0
