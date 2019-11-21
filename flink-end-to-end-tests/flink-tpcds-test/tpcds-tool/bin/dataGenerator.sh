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

# TPC-DS data generator，support linux and mac OS only.
# For other OS can get support from http://www.tpc.org/.
# the TPC-DS tools version is 2.11.0.

set -Eeuo pipefail

if [ $# -lt 2 ]; then
	echo "[ERROR] `date +%H:%M:%S` Insufficient params, need 2 parameters: <scaleFactor>  <outputDataDir>"
	exit 127
fi

scale_factor=$1
data_dir=$2

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
    echo "[INFO] `date +%H:%M:%S` Current OS: Mac OS X OS"
    echo "[INFO] `date +%H:%M:%S` Download data generator from github..."
    #TODO download form ververica github account
    echo "[INFO] `date +%H:%M:%S` Download data generator from success."
    echo "[INFO] `date +%H:%M:%S` Generating TPC-DS qualification data, this need several minutes, please wait..."

    $workDir/dsdgen_macos -SCALE $scale_factor -FORCE Y -DIR $data_dir
elif  [[ "$OS_TYPE" == "linux" ]]; then
    echo "[INFO] `date +%H:%M:%S` Current OS: GNU/Linux OS"
    echo "[INFO] `date +%H:%M:%S` Download data generator from github..."
    #TODO download form ververica github account
    echo "[INFO] `date +%H:%M:%S` Download data generator from success."
    echo "[INFO] `date +%H:%M:%S` Generating TPC-DS qualification data, this need several minutes, please wait..."
    $workDir/dsdgen_linux -SCALE $scale_factor -FORCE Y -DIR $data_dir
else
    echo "[ERROR] `date +%H:%M:%S` Unsupported OS, only support Mac OS、Linux."
    exit 127
fi

echo "[INFO] `date +%H:%M:%S` Generate TPC-DS qualification data success."

