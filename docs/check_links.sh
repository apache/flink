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

DOCS_CHECK_DIR="`dirname \"$0\"`" # relative
DOCS_CHECK_DIR="`( cd \"$DOCS_CHECK_DIR\" && pwd -P)`" # absolutized and normalized
if [ -z "$DOCS_CHECK_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

echo "Check docs directory: $DOCS_CHECK_DIR"

target=${1:-"http://localhost:4000"}

# Crawl the docs, ignoring robots.txt, storing nothing locally
wget --spider -r -nd -nv -e robots=off -p -o $DOCS_CHECK_DIR/spider.log "$target"

# Abort for anything other than 0 and 4 ("Network failure")
status=$?

if [ $status -ne 0 ] && [ $status -ne 4 ]; then
    exit $status
fi

# Fail the build if any broken links are found
no_broken_links_str_count=$(grep 'Found no broken links' $DOCS_CHECK_DIR/spider.log | wc -l)
if [ $no_broken_links_str_count -ne 1 ]; then
    grep -B 1 "Remote file does not exist -- broken link!!!" $DOCS_CHECK_DIR/spider.log
    echo "---------------------------------------------------------------------------"
    echo "Check the spider.log file for errors!"
    exit 1
fi

echo 'All links in docs are valid!'
exit 0
