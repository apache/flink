#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

MAX_RETRY_SECONDS=180
echo "Wait yarn cluster and hive server start..."

# wait for hadoop cluster to be set up
start_time=$(date +%s)
until docker logs hive 2>&1 | grep -q "The hadoop cluster and hive service have been ready.."; do
    current_time=$(date +%s)
    time_diff=$((current_time - start_time))

    if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
        return 1
    else
        echo "Waiting for hadoop cluster and hive service to come up. We have been trying for $time_diff seconds, retrying ..."
        sleep 5
    fi
done

# perform health checks
if ! { [[ $(docker inspect -f '{{.State.Running}}' master 2>&1) = 'true' ]] &&
       [[ $(docker inspect -f '{{.State.Running}}' slave1 2>&1) = 'true' ]] &&
       [[ $(docker inspect -f '{{.State.Running}}' slave2 2>&1) = 'true' ]] &&
       [[ $(docker inspect -f '{{.State.Running}}' mysql 2>&1) = 'true' ]] &&
       [[ $(docker inspect -f '{{.State.Running}}' hive 2>&1) = 'true' ]]; };
then
    echo "some containers run unnormally... exit!"
    exit
fi

# try and see if NodeManagers are up, otherwise the Flink job will not have enough resources
# to run
nm_running=`docker exec master bash -c "yarn node -list" | grep RUNNING | wc -l`
start_time=$(date +%s)
while [[ "$nm_running" -lt "2" ]]; do
    current_time=$(date +%s)
    time_diff=$((current_time - start_time))

    if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
        echo "Wait node manager to set up exceed the $MAX_RETRY_SECONDS seconds, exit "
        break
    else
        echo "We only have $nm_running NodeManagers up. We have been trying for $time_diff seconds, retrying ..."
        sleep 1
    fi
    nm_running=`docker exec master bash -c "yarn node -list" | grep RUNNING | wc -l`
done