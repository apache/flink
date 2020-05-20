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
common_scripts_dir=$4

source "$common_scripts_dir"/common.sh

# download urls
dsdgen_linux_url=https://raw.githubusercontent.com/ververica/tpc-ds-generators/f5d6c11681637908ce15d697ae683676a5383641/generators/dsdgen_linux
dsdgen_linux_url_aarch64=https://raw.githubusercontent.com/ververica/tpc-ds-generators/master/generators/dsdgen_linux_aarch64
dsdgen_macos_url=https://raw.githubusercontent.com/ververica/tpc-ds-generators/f5d6c11681637908ce15d697ae683676a5383641/generators/dsdgen_macos
tpcds_idx_url=https://raw.githubusercontent.com/ververica/tpc-ds-generators/f5d6c11681637908ce15d697ae683676a5383641/generators/tpcds.idx

# file md5sums
dsdgen_linux_md5="299216f04d490a154f632b0b9b842241"
dsdgen_linux_aarch64_md5="faf26047d0bea5017b99e6f53ceaf5e5"
dsdgen_macos_md5="a1019fc63e43324decac1b68d14ff4da"
tpcds_idx_md5="376152c9aa150c59a386b148f954c47d"

case "$(uname -s)" in
    Linux*)     OS_TYPE=linux;;
    Darwin*)    OS_TYPE=mac;;
    *)          OS_TYPE="UNKNOWN:${unameOut}"
esac

workDir=`dirname $0`
cd $workDir
workDir=`pwd`

function download_and_validate() {
  fileName=$1
  destDir=$2
  url=$3
  expectedMd5=$4
  osType=$5
  curl -o $destDir/$fileName $url
  if [[ -e $generator_dir/$fileName ]]; then
      if [[ ${osType} == "mac" ]]; then
        actualMd5=`md5 $generator_dir/$fileName`
      else
        actualMd5=`md5sum $generator_dir/$fileName`
      fi
      if [[ ${actualMd5} == *${expectedMd5}* ]]; then
        echo "[INFO] Download and validate ${fileName} success."
        return 0
      else
        return 1
      fi
  fi
}

function cleanup() {
  fileName=$1
  destDir=$2
  echo "[WARN] Download file ${fileName} failed."
  if [[ -e $destDir/$fileName ]]; then
    rm $destDir/$fileName
  fi
}

errCode_download_dsgen=0
errCode_download_idx=0
# Obtain OS from shell
if [[ "$OS_TYPE" == "mac" ]]; then
    echo "[INFO] Current OS: Mac OS X OS"
    echo "[INFO] Download data generator from github..."

    retry_times_with_backoff_and_cleanup 3 5 "download_and_validate "dsdgen_macos" $generator_dir $dsdgen_macos_url $dsdgen_macos_md5 $OS_TYPE" \
    "cleanup "dsdgen_macos" $generator_dir" || errCode_download_dsgen=$?

    retry_times_with_backoff_and_cleanup 3 5 "download_and_validate "tpcds.idx" $generator_dir $tpcds_idx_url $tpcds_idx_md5 $OS_TYPE" \
    "cleanup "tpcds.idx" $generator_dir" || errCode_download_idx=$?

    if [[ "$errCode_download_dsgen" == "0" ]] && [[ "$errCode_download_idx" == "0" ]]; then
        echo "[INFO] Download and validate data generator files success."
        echo "[INFO] `date +%H:%M:%S` Generating TPC-DS qualification data, this need several minutes, please wait..."
        chmod +x  $generator_dir/dsdgen_macos
        cd  $generator_dir
        ./dsdgen_macos -SCALE $scale_factor -FORCE Y -DIR $data_dir
    else
        echo "[ERROR] Download and validate data generator files fail, please check the network."
        exit 127
    fi
elif  [[ "$OS_TYPE" == "linux" ]]; then
    echo "[INFO] `date +%H:%M:%S` Current OS: GNU/Linux OS"
    echo "[INFO] `date +%H:%M:%S` Download data generator from github..."
    if [[ `uname -i` == 'aarch64' ]]; then
      retry_times_with_backoff_and_cleanup 3 5 "download_and_validate "dsdgen_linux" $generator_dir $dsdgen_linux_url_aarch64 $dsdgen_linux_aarch64_md5 $OS_TYPE" \
      "cleanup "dsdgen_linux" $generator_dir" || errCode_download_dsgen=$?
    else
      retry_times_with_backoff_and_cleanup 3 5 "download_and_validate "dsdgen_linux" $generator_dir $dsdgen_linux_url $dsdgen_linux_md5 $OS_TYPE" \
      "cleanup "dsdgen_linux" $generator_dir" || errCode_download_dsgen=$?
    fi
    retry_times_with_backoff_and_cleanup 3 5 "download_and_validate "tpcds.idx" $generator_dir $tpcds_idx_url $tpcds_idx_md5 $OS_TYPE" \
    "cleanup "tpcds.idx" $generator_dir" || errCode_download_idx=$?

    if [[ "$errCode_download_dsgen" == "0" ]] && [[ "$errCode_download_idx" == "0" ]]; then
        echo "[INFO] Download and validate data generator files success."
        echo "[INFO] `date +%H:%M:%S` Generating TPC-DS qualification data, this need several minutes, please wait..."
        chmod +x  $generator_dir/dsdgen_linux
        cd  $generator_dir
        ./dsdgen_linux -SCALE $scale_factor -FORCE Y -DIR $data_dir
    else
        echo "[ERROR] Download and validate data generator files fail, please check the network."
        exit 127
    fi
else
    echo "[ERROR] `date +%H:%M:%S` Unsupported OS, only support Mac OS、Linux."
    exit 127
fi

echo "[INFO] `date +%H:%M:%S` Generate TPC-DS qualification data success."

