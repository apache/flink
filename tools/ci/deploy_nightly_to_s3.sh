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

	echo "Installing artifacts deployment script"	
	export ARTIFACTS_DEST="$HOME/bin/artifacts"	
	curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash	
	PATH="$(dirname "$ARTIFACTS_DEST"):$PATH"	

	echo "Uploading contents of $FILES_DIR to S3:"	


	artifacts upload \
		  --bucket $ARTIFACTS_S3_BUCKET \
		  --key $ARTIFACTS_AWS_ACCESS_KEY_ID \
		  --secret $ARTIFACTS_AWS_SECRET_ACCESS_KEY \
		  --target-paths / $FILES_DIR

}	


