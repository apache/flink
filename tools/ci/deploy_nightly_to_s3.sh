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

# fail on errors	
set -e -x


function upload_to_s3() {
	local FILES_DIR=$1

	# The AWS CLI reads credentials from the environment (AWS_ACCESS_KEY_ID /
	# AWS_SECRET_ACCESS_KEY), so they are never placed on a command line and
	# cannot leak through the `set -x` trace enabled above.
	if ! command -v aws >/dev/null 2>&1; then
		echo "Installing AWS CLI v2"
		local AWS_TMP_DIR
		AWS_TMP_DIR=$(mktemp -d)
		curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$AWS_TMP_DIR/awscliv2.zip"
		unzip -q "$AWS_TMP_DIR/awscliv2.zip" -d "$AWS_TMP_DIR"
		"$AWS_TMP_DIR/aws/install" --bin-dir "$HOME/bin" --install-dir "$HOME/aws-cli" --update
		PATH="$HOME/bin:$PATH"
	fi

	echo "Uploading contents of $FILES_DIR to S3:"

	# Mirrors the previous uploader's behaviour: copy the directory contents to
	# the bucket root using the bucket's default (private) ACL.
	aws s3 cp "$FILES_DIR" "s3://$ARTIFACTS_S3_BUCKET/" --recursive --no-progress
}
