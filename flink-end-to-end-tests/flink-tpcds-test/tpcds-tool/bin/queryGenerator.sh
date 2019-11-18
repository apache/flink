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

# TPC-DS query generator based onTPC-DS tools version  2.11.0.
# support linux and mac OS only. For other OS can get support from http://www.tpc.org/.
# .

set -Eeuo pipefail

if [ $# -lt 2 ]; then
	echo "[ERROR] Insufficient params, need 2 parameters: <scaleFactor>  <outputQueryDir>"
	exit 127
fi
scale_factor=$1
query_dir=$2
templateList=./../query_templates_qualified/templates.lst

case "$(uname -s)" in
    Linux*)     OS_TYPE=linux;;
    Darwin*)    OS_TYPE=mac;;
    *)          OS_TYPE="UNKNOWN:${unameOut}"
esac

workDir=`dirname $0`
cd $workDir
workDir=`pwd`

# Obtain OS from shell
if [[ "$OS_TYPE" == "mac" ]]; then
    echo "[INFO] Current os: Mac OS X OS"
    echo "[INFO] Generating TPC-DS qualification query, this need several seconds, please wait..."
    while read line
    do
       template=$line
       fileNmae=${template%*.tpl}.sql
       $workDir/dsqgen_macos -SCALE $scale_factor -DIALECT netezza -TEMPLATE ../query_templates_qualified/$template -DIRECTORY ../query_templates_qualified -QUIET Y -FILTER Y > $query_dir/tmp.sql
       cat $query_dir/tmp.sql | sed 's/\(.*\);\(.*\)/\1\2/' > $query_dir/$fileNmae
       rm $query_dir/tmp.sql
    done < $templateList
elif [[ "$OS_TYPE" == "linux" ]]; then
    echo "[INFO] Current os: GNU/Linux OS"
    echo "[INFO] Generating TPC-DS qualification query, this need several seconds, please wait..."
    while read line
    do
       template=$line
       fileNmae=${template%*.tpl}.sql
       $workDir/dsqgen_linux -SCALE $scale_factor -DIALECT netezza -TEMPLATE ../query_templates_qualified/$template -DIRECTORY ../query_templates_qualified -QUIET Y -FILTER Y > $query_dir/tmp.sql
       cat $query_dir/tmp.sql | sed 's/\(.*\);\(.*\)/\1\2/' > $query_dir/$fileNmae
       rm $query_dir/tmp.sql
    done < $templateList
else
    echo "[ERROR] Unsupported OS, only support Mac OS、Linux."
    exit 127
fi

echo "[INFO] Generating TPC-DS qualification query success."
