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

source "$(dirname "$0")"/gpu-discovery-common.sh

if [ $# -lt 1 ]; then
  echo "Usage: ./nvidia-gpu-discovery.sh gpu-amount [--enable-coordination-mode] [--coordination-file filePath]"
  exit 1
fi

AMOUNT=$1
shift
COORDINATION_FILE="/var/tmp/flink-gpu-coordination"
COORDINATION_MODE=""
EXIT_NON_ZERO=""

while [[ $# -ge 1 ]]
do
key="$1"
shift
  case $key in
    --enable-coordination-mode)
    COORDINATION_MODE="coordination"
    ;;
    --coordination-file)
    COORDINATION_FILE="$1"
    shift
    ;;
    *)
    # unknown option
    ;;
  esac
done

if [ $AMOUNT -eq 0 ]; then
  exit 0
fi

csv_index=`nvidia-smi --query-gpu=index --format=csv,noheader`
if [ $? -ne 0 ]; then
  exit 1
fi
IFS=',' read -r -a indexes <<< $(echo $csv_index)
gpu_discovery "${indexes[*]}" $AMOUNT $COORDINATION_MODE $COORDINATION_FILE
