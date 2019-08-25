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

# This script extracts from all jars in the specified directory the NOTICE files and the
# licenses folders. It then concatenates all NOTICE files and collects the contents of all
# licenses folders in the specified output directory.
#
# This tool can be used to generate a rough skeleton for the binary NOTICE file. Be aware,
# that it does not deduplicate contents.

set -Eeuo pipefail

SRC=${1:-.}
DST=${2:-licenses-output}
PWD=$(pwd)
TMP="${DST}/tmp"

USAGE="collect_license_files <SOURCE_DIRECTORY:-.> <OUTPUT_DIRECTORY:-licenses-output>"

if [ "${SRC}" = "-h" ]; then
	echo "${USAGE}"
	exit 0
fi

for i in $(find -L "${SRC}" -name "*.jar")
do
	DIR="${TMP}/$(basename -- "$i" .jar)"
	mkdir -p "${DIR}"
	JAR="${PWD}/${i}"
	(cd "${DIR}" && jar xf ${JAR} META-INF/NOTICE META-INF/licenses)
done

NOTICE="${DST}/NOTICE"
[ -f "${NOTICE}" ] && rm "${NOTICE}"
find "${TMP}" -name "NOTICE" | sort | xargs cat >> "${NOTICE}"

LICENSES="${DST}/licenses"
[ -f "${LICENSES}" ] && rm -r ""
find "${TMP}" -name "licenses" -type d -exec cp -r -- "{}" "${DST}" \;

rm -r "${TMP}"
