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

set -e
cd "$(dirname ${BASH_SOURCE[0]})"

DIR="`pwd`"

# We need at least bundler to proceed
if [ "`command -v bundle`" == "" ]; then
	echo "WARN: Could not find bundle."
    echo "Attempting to install locally. If this doesn't work, please install with 'gem install bundler'."

    # Adjust the PATH to discover the locally installed Ruby gem
    if which ruby >/dev/null && which gem >/dev/null; then
        export PATH="$(ruby -rubygems -e 'puts Gem.user_dir')/bin:$PATH"
    fi

    # install bundler locally
    gem install --user-install bundler
fi

# Install Ruby dependencies locally
bundle install --path .rubydeps

DOCS_SRC=${DIR}
DOCS_DST=${DOCS_SRC}/content

# default jekyll command is to just build site
JEKYLL_CMD="build"

# if -p flag is provided, serve site on localhost
while getopts ":p" opt; do
	case $opt in
		p)
		JEKYLL_CMD="serve --baseurl= --watch"
		;;
	esac
done

# use 'bundle exec' to insert the local Ruby dependencies
bundle exec jekyll ${JEKYLL_CMD} --source "${DOCS_SRC}" --destination "${DOCS_DST}"
