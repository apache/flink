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
	if [ "$#" != "2" ]; then
		echo "[ERROR] Invalid number of parameters passed. Expected parameters for $0: <parent-directory> <module-label>"
		exit 1
	fi

	local parent_directory module
	parent_directory="$1"
	module="$2"

	export DEBUG_FILES_OUTPUT_DIR="${parent_directory}/debug_files"
	export DEBUG_FILES_NAME="$(echo "${module}" | tr -c '[:alnum:]\n\r' '_')-$(date +%s)"

	if [ -n "${TF_BUILD+x}" ]; then
		echo "[INFO] Azure Pipelines environment detected: $0 will export the variables in the Azure-specific way."

		echo "##vso[task.setvariable variable=DEBUG_FILES_OUTPUT_DIR]$DEBUG_FILES_OUTPUT_DIR"
		echo "##vso[task.setvariable variable=DEBUG_FILES_NAME]$DEBUG_FILES_NAME"
	elif [ -n "${GITHUB_ACTIONS+x}" ]; then
		echo "[INFO] GitHub Actions environment detected: $0 will export the variables in the GHA-specific way."

		if [ -z "${GITHUB_OUTPUT+x}" ]; then
			echo "[ERROR] The GITHUB_OUTPUT variable is not set."
			exit 1
		elif [ ! -f "$GITHUB_OUTPUT" ]; then
			echo "[ERROR] The GITHUB_OUTPUT variable doesn't refer to a file: $GITHUB_OUTPUT"
			exit 1
		fi

		echo "debug-files-output-dir=${DEBUG_FILES_OUTPUT_DIR}" >> "$GITHUB_OUTPUT"
		echo "debug-files-name=${DEBUG_FILES_NAME}" >> "$GITHUB_OUTPUT"
	else
		echo "[ERROR] No CI environment detected. Debug artifact-related variables couldn't be exported."
		exit 1
	fi

	mkdir -p $DEBUG_FILES_OUTPUT_DIR || { echo "FAILURE: cannot create debug files directory '${DEBUG_FILES_OUTPUT_DIR}'." ; exit 1; }
}

function unset_debug_artifacts_if_empty {
	if [ -z "${DEBUG_FILES_OUTPUT_DIR+x}" ]; then
		echo "[ERROR] No environment variable DEBUG_FILES_OUTPUT_DIR was set."
		exit 1
	elif [ "$(ls -A ${DEBUG_FILES_OUTPUT_DIR} | wc -l)" -eq 0 ]; then
		echo "[INFO] Unsetting environment variable DEBUG_FILES_OUTPUT_DIR because there were no artifacts produced."

		if [ -n "${TF_BUILD+x}" ]; then
			echo "##vso[task.setvariable variable=DEBUG_FILES_OUTPUT_DIR]"
		elif [ -n "${GITHUB_ACTIONS+x}" ]; then
			echo "debug-files-output-dir=" >> "$GITHUB_OUTPUT"
		else
			echo "[ERROR] No CI environment detected. Debug artifact-related variable won't be unset."
			exit 1
		fi
	fi
}
