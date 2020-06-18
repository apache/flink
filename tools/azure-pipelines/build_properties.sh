#!/usr/bin/env bash
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
# Returns 0 if the change is a documentation-only pull request
#
function is_docs_only_pullrequest() {
	# check if it is a pull request:
	if [[ ! $BUILD_SOURCEBRANCHNAME == ci_* ]] ; then
		return 1
	fi
	PR_ID=`echo "$BUILD_SOURCEBRANCHNAME" | cut -f2 -d_`
	if ! [[ "$PR_ID" =~ ^[0-9]+$ ]] ; then
		echo "ERROR: Extracted PR_ID is not a number, but this: '$PR_ID'"
	 	return 1
	fi
	# check if it is docs only pull request:
	CHANGES=`curl --silent "https://api.github.com/repos/apache/flink/pulls/$PR_ID/files" | jq -r ".[].filename"`
	echo "This build contains the following changed files:"
	echo "$CHANGES"

	if [[ $CHANGES == "" ]] ; then 
		return 1
	fi

	if [[ $(echo $CHANGES | grep -v "docs/") == "" ]] ; then
		return 0
	fi
	return 1
}
