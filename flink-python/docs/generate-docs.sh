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

TARGET_PATH=
# parse_opts
USAGE="
usage: $0 [options]
-h          print this help message and exit
-t          list all checks supported.
"
while getopts "ht:" arg; do
    case "$arg" in
        h)
            printf "%s\\n" "$USAGE"
            exit 2
            ;;
        t)
            TARGET_PATH=$OPTARG
            ;;
        ?)
            printf "ERROR: did not recognize option '%s', please try -h\\n" "$1"
            exit 1
            ;;
    esac
done

LINTPYTHON_PATH="../dev/lint-python.sh"
if [[ ! -f "$LINTPYTHON_PATH" ]]; then
	echo "The $LINTPYTHON_PATH is missing."
	exit 1
fi

# create python api doc
$LINTPYTHON_PATH -i "sphinx"

if [[ $? -ne 0 ]]; then
	echo "create doc failed"
	exit 1
fi

_BUILD_PATH="./_build"

if [[ ! -d "$_BUILD_PATH" ]]; then
	echo "The directory $_BUILD_PATH is not created.you can retry to this script"
	exit 1
fi

_HTML_PATH="${_BUILD_PATH}/html"
if [[ ! -d "$_HTML_PATH" ]]; then
	echo "The directory $_HTML_PATH is not created.you can retry to this script"
	exit 1
fi

# Whether specified the option -t
if [[ -z "$TARGET_PATH" ]]; then
	exit 0
fi

if [[ ! -d "$TARGET_PATH" ]]; then
	echo "The supported dir is not exist."
	exit 1
fi

#copy the python docs html to the target path.
cp -r $_HTML_PATH/. $TARGET_PATH

if [[ $? -ne 0 ]]; then
	echo "Copy python api docs to $TARGET_PATH failed."
	exit 1
fi

