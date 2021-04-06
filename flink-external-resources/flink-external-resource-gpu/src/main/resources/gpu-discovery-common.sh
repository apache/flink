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

non_coordination_allocate() {
  indexes=($1)
  amount=$2
  to_occupy_indexes=(${indexes[@]:0:$amount})
  if [ $amount -gt ${#to_occupy_indexes[@]} ]; then
    echo "Could not get enough GPU resources."
    exit 1
  fi
  echo ${to_occupy_indexes[@]} | sed 's/ /,/g'
}

coordination_allocate() {
  indexes=($1)
  amount=$2
  coordination_file=${3:-/var/tmp/flink-gpu-coordination}
  (
    flock -x 200
    # GPU indexes to be occupied.
    to_occupy_indexes=()
    # GPU indexes which are already recorded in the coordination file. These indexes should not be occupied unless the associated
    # processes are no longer alive.
    recorded_indexes=()
    for i in ${indexes[@]}
    do
      if [ ${#to_occupy_indexes[@]} -eq $amount ]; then
        break
      elif [ `grep -c "^$i " $coordination_file` -ne 0 ]; then
        recorded_indexes[${#recorded_indexes[@]}]=$i
      else
        to_occupy_indexes[${#to_occupy_indexes[@]}]=$i
      fi
    done

    # If there are not enough indexes, we will try to occupy indexes whose associated processes are dead.
    for i in ${!recorded_indexes[@]}
    do
      if [ ${#to_occupy_indexes[@]} -eq $amount ];then
        break
      fi
      owner=`grep "^${recorded_indexes[$i]} " $coordination_file | awk '{print $2}'`
      if [ -n $owner ] && [ `ps -p $owner | grep -c $owner` -eq 0 ]; then
        # The owner does not exist anymore. We could occupy it.
        sed -i "/${recorded_indexes[$i]} /d" $coordination_file
        to_occupy_indexes[${#to_occupy_indexes[@]}]=${recorded_indexes[$i]}
        unset recorded_indexes[$i]
      fi
    done

    if [ $amount -gt ${#to_occupy_indexes[@]} ]; then
      echo "Could not get enough GPU resources."
      exit 1
    fi

    for i in "${to_occupy_indexes[@]}"
    do
      echo "$i $PPID" >> $coordination_file
    done

    echo ${to_occupy_indexes[@]} | sed 's/ /,/g'
  ) 200<> $coordination_file
}

gpu_discovery() {
  indexes=$1
  amount=$2
  coordination_mode=$3
  coordination_file=${4:-/var/tmp/flink-gpu-coordination}
  if [ "$coordination_mode" == "coordination" ]; then
    coordination_allocate "$indexes" $amount $coordination_file
  else
    non_coordination_allocate "$indexes" $amount
  fi
}
