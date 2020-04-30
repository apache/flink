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

# This script updates the year in all NOTICE files

set -Eeuo pipefail

FLINK_DIR=${1:-}

USAGE="update_notice_year <FLINK_DIR>"

if [[ -z "${FLINK_DIR}" || "${FLINK_DIR}" = "-h" ]]; then
	echo "${USAGE}"
	exit 0
fi

NEW_YEAR=`date +'%Y'`

for path in $(find "${FLINK_DIR}" -name "NOTICE*"); do
	echo "Updating: ${path}"
	sed "s/Copyright 2014-.* The Apache Software Foundation/Copyright 2014-${NEW_YEAR} The Apache Software Foundation/" "${path}" > "${path}_new"
	mv "${path}_new" "${path}"
done

echo "The script is just a helper tool. Please verify the performed changes manually again!"