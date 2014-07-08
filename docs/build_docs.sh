#!/bin/bash
########################################################################################################################
# Copyright (C) 2010-2014 by the Stratos	phere project (http://stratosphere.eu)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#	  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
########################################################################################################################

HAS_JEKYLL=true

command -v jekyll > /dev/null
if [ $? -ne 0 ]; then
	echo -n "ERROR: Could not find jekyll. "
	echo "Please install with 'gem install jekyll' (see http://jekyllrb.com)."

	HAS_JEKYLL=false
fi

command -v redcarpet > /dev/null
if [ $? -ne 0 ]; then
	echo -n "WARN: Could not find redcarpet. "
	echo -n "Please install with 'sudo gem install redcarpet' (see https://github.com/vmg/redcarpet). "
	echo "Redcarpet is needed for Markdown parsing and table of contents generation."
fi

command -v pygmentize > /dev/null
if [ $? -ne 0 ]; then
	echo -n "WARN: Could not find pygments. "
	echo -n "Please install with 'sudo easy_install Pygments' (requires Python; see http://pygments.org). "
	echo "Pygments is needed for syntax highlighting of the code examples."
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DOCS_SRC=${DIR}
DOCS_DST=${DOCS_SRC}/target

# default jekyll command is to just build site
JEKYLL_CMD="build"

# if -p flag is provided, serve site on localhost
while getopts ":p" opt; do
	case $opt in
		p)
		JEKYLL_CMD="serve --watch"
		;;
	esac
done

if $HAS_JEKYLL; then
	jekyll ${JEKYLL_CMD} --source ${DOCS_SRC} --destination ${DOCS_DST}
fi