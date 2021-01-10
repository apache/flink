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

gem update --system
gem install bundler -v 1.17.2

# build the docs w/o any additional arguments, to build both the en and zh variant
JEKYLL_BUILD_CONFIG="--baseurl=" CACHE_DIR=$HOME/gem_cache ./docs/build_docs.sh

if [ $? -ne 0 ]; then
	echo "Error building the docs"
	exit 1
fi

# serve the docs
cd docs/content
python -m SimpleHTTPServer 4000 &
cd ../..


for i in `seq 1 90`;
do
	echo "Waiting for server..."
	curl -Is http://localhost:4000 --fail
	if [ $? -eq 0 ]; then
		./docs/check_links.sh
		EXIT_CODE=$?
		exit $EXIT_CODE
	fi
	sleep 10
done

echo "Jekyll server wasn't started within 15 minutes"
exit 1
