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

function prepare_debug_files {
	MODULE=$1
	export DEBUG_FILES_OUTPUT_DIR="$AGENT_TEMPDIRECTORY/debug_files/"
	export DEBUG_FILES_NAME="$(echo $MODULE | tr -dc '[:alnum:]\n\r')-$(date +%s)"
	echo "##vso[task.setvariable variable=DEBUG_FILES_OUTPUT_DIR]$DEBUG_FILES_OUTPUT_DIR"
	echo "##vso[task.setvariable variable=DEBUG_FILES_NAME]$DEBUG_FILES_NAME"
	mkdir -p $DEBUG_FILES_OUTPUT_DIR || { echo "FAILURE: cannot create log directory '${DEBUG_FILES_OUTPUT_DIR}'." ; exit 1; }
}

function compress_debug_files {
	echo "Compressing debug files"
	tar -zcvf /tmp/$DEBUG_FILES_NAME.tgz -C $DEBUG_FILES_OUTPUT_DIR .
	# clean directory
	rm -rf $DEBUG_FILES_OUTPUT_DIR ; mkdir -p $DEBUG_FILES_OUTPUT_DIR
	mv /tmp/$DEBUG_FILES_NAME.tgz $DEBUG_FILES_OUTPUT_DIR
}
