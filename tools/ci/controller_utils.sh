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

print_system_info() {
    echo "CPU information"
    lscpu

    echo "Memory information"
    cat /proc/meminfo

    echo "Disk information"
    df -hH

    echo "Running build as"
    whoami
}

# locate YARN logs and put them into artifacts directory
put_yarn_logs_to_artifacts() {
	for file in `find ./flink-yarn-tests/target -type f -name '*.log' -or -name '*.out'`; do
		TARGET_FILE=`echo "$file" | grep -Eo "container_[0-9_]+/(.*).[a-z]{3}"`
		TARGET_DIR=`dirname	 "$TARGET_FILE"`
		mkdir -p "$DEBUG_FILES_OUTPUT_DIR/yarn-tests/$TARGET_DIR"
		cp $file "$DEBUG_FILES_OUTPUT_DIR/yarn-tests/$TARGET_FILE"
	done
}

print_stacktraces () {
	echo "=============================================================================="
	echo "The following Java processes are running (JPS)"
	echo "=============================================================================="

	JAVA_PROCESSES=`jps`
	echo "$JAVA_PROCESSES"

	local pids=( $(echo "$JAVA_PROCESSES" | awk '{print $1}') )

	for pid in "${pids[@]}"; do
		echo "=============================================================================="
		echo "Printing stack trace of Java process ${pid}"
		echo "=============================================================================="

		jstack $pid
	done
}

