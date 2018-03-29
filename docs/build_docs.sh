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

RUBY=${RUBY:-ruby}
GEM=${GEM:-gem}

set -e
cd "$(dirname ${BASH_SOURCE[0]})"

DIR="`pwd`"

# We need at least bundler to proceed
if [ "`command -v bundle`" == "" ]; then
	RUBYGEM_BINDIR=""

	# Adjust the PATH to discover locally installed ruby gem binaries
	export PATH="$(${RUBY} -e 'puts Gem.user_dir')/bin:$PATH"

	if [ "`command -v bundle`" == "" ]; then
		echo "WARN: Could not find bundle."
		echo "Attempting to install locally. If this doesn't work, please install with 'gem install bundler'."

		# install bundler locally
		${GEM} install --user-install --no-format-executable bundler
	fi
fi

# Install Ruby dependencies locally
bundle install --path .rubydeps

DOCS_SRC=${DIR}
DOCS_DST=${DOCS_SRC}/content

# default jekyll command is to just build site
JEKYLL_CMD="build"

# if -p flag is provided, serve site on localhost
# -i is like -p, but incremental (only rebuilds the modified file)
while getopts "pi" opt; do
	case $opt in
		p)
		JEKYLL_CMD="serve --baseurl= --watch"
		;;
		i)
		[[ `${RUBY} -v` =~ 'ruby 1' ]] && echo "Error: building the docs with the incremental option requires at least ruby 2.0" && exit 1
		JEKYLL_CMD="liveserve --baseurl= --watch --incremental"
		;;
	esac
done

# use 'bundle exec' to insert the local Ruby dependencies
bundle exec jekyll ${JEKYLL_CMD} --source "${DOCS_SRC}" --destination "${DOCS_DST}"
