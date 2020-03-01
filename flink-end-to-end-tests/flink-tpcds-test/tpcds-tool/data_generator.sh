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

if [ $# -lt 3 ]; then
	echo "[ERROR] `date +%H:%M:%S` Insufficient params, need 3 parameters: <generatorDir> <scaleFactor>  <outputDataDir>"
	exit 127
fi

generator_dir=$1
scale_factor=$2
data_dir=$3

dsdgen_linux_url=https://raw.githubusercontent.com/ververica/tpc-ds-generators/f5d6c11681637908ce15d697ae683676a5383641/generators/dsdgen_linux
dsdgen_linux_url_aarch64=https://raw.githubusercontent.com/ververica/tpc-ds-generators/master/generators/dsdgen_linux_aarch64
dsdgen_macos_url=https://raw.githubusercontent.com/ververica/tpc-ds-generators/f5d6c11681637908ce15d697ae683676a5383641/generators/dsdgen_macos
tpcds_idx_url=https://raw.githubusercontent.com/ververica/tpc-ds-generators/f5d6c11681637908ce15d697ae683676a5383641/generators/tpcds.idx


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
    echo "[INFO] Current OS: Mac OS X OS"
    echo "[INFO] Download data generator from github..."
    curl -o $generator_dir/dsdgen_macos $dsdgen_macos_url
    curl -o $generator_dir/tpcds.idx $tpcds_idx_url
    if [[ -e $generator_dir/dsdgen_macos ]] && [[ -e $generator_dir/tpcds.idx ]]; then
        echo "[INFO] Download data generator success."
        echo "[INFO] `date +%H:%M:%S` Generating TPC-DS qualification data, this need several minutes, please wait..."
        chmod +x  $generator_dir/dsdgen_macos
        cd  $generator_dir
        ./dsdgen_macos -SCALE $scale_factor -FORCE Y -DIR $data_dir
    else
        echo "[ERROR] Download data generator fail, please check your network."
        exit 127
    fi
elif  [[ "$OS_TYPE" == "linux" ]]; then
    echo "[INFO] `date +%H:%M:%S` Current OS: GNU/Linux OS"
    echo "[INFO] `date +%H:%M:%S` Download data generator from github..."
    if [[ `uname -i` == 'aarch64' ]]; then
      curl -o $generator_dir/dsdgen_linux $dsdgen_linux_url_aarch64
    else
      curl -o $generator_dir/dsdgen_linux $dsdgen_linux_url
    fi
    curl -o $generator_dir/tpcds.idx $tpcds_idx_url
    if [[ -e $generator_dir/dsdgen_linux ]] && [[ -e $generator_dir/tpcds.idx ]]; then
        echo "[INFO] Download data generator success."
        echo "[INFO] `date +%H:%M:%S` Generating TPC-DS qualification data, this need several minutes, please wait..."
        chmod +x  $generator_dir/dsdgen_linux
        cd  $generator_dir
        ./dsdgen_linux -SCALE $scale_factor -FORCE Y -DIR $data_dir
    else
        echo "[ERROR] Download data generator fail, please check your network."
        exit 127
    fi
else
    echo "[ERROR] `date +%H:%M:%S` Unsupported OS, only support Mac OS、Linux."
    exit 127
fi

echo "[INFO] `date +%H:%M:%S` Generate TPC-DS qualification data success."

