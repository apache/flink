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

# Usage: ./test-coordination-mode.sh target_function

test_non_coordination_mode() {
  IFS=',' read -r -a output <<< $(bash -c "$(dirname "$0")/testing-gpu-discovery.sh 2")
  if [ ${#output[@]} -ne 2 ]; then
    exit 1
  fi
}

test_coordinate_indexes() {
  IFS=',' read -r -a output1 <<< $(bash -c "$(dirname "$0")/testing-gpu-discovery.sh 2 --enable-coordination-mode --available-gpu-amount 4")
  IFS=',' read -r -a output2 <<< $(bash -c "$(dirname "$0")/testing-gpu-discovery.sh 2 --enable-coordination-mode --available-gpu-amount 4")

  if [[ ${#output1[@]} -ne 2 || ${#output2[@]} -ne 2 ]]; then
    exit 1
  fi

  for i in output1
  do
    for j in output2
    do
      if [ $i == $j ]; then
        exit 1
      fi
    done
  done
}

test_preempt_from_dead_processes() {
  local test_pid
  while [[ -z $test_pid || $(ps -p $test_pid | grep -c $test_pid) -ne 0 ]]
  do
    test_pid=$(shuf -i 1-32768 -n 1)
  done
  echo 0 $test_pid >> /var/tmp/flink-gpu-coordination
  IFS=',' read -r -a output1 <<< $(bash -c "$(dirname "$0")/testing-gpu-discovery.sh 2 --enable-coordination-mode --available-gpu-amount 4")
  IFS=',' read -r -a output2 <<< $(bash -c "$(dirname "$0")/testing-gpu-discovery.sh 2 --enable-coordination-mode --available-gpu-amount 4")

  if [[ ${#output1[@]} -ne 2 || ${#output2[@]} -ne 2 ]]; then
    exit 1
  fi
}

test_coordination_file() {
  coordination_file=/var/tmp/flink-test-coordination
  $(dirname "$0")/testing-gpu-discovery.sh 2 --enable-coordination-mode --coordination-file $coordination_file

  if ![ -f $coordination_file ]; then
    exit 1;
  fi
}

clean_state() {
  rm -f /var/tmp/flink-gpu-coordination
  rm -f /var/tmp/flink-test-coordination
}
trap clean_state EXIT

$@
