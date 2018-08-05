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

source "$(dirname "$0")"/common.sh

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-bucketing-sink-test/target/BucketingSinkTestProgram.jar

# enable DEBUG logging level to retrieve truncate length later
sed -i -e 's/#log4j.logger.org.apache.flink=INFO/log4j.logger.org.apache.flink=DEBUG/g' $FLINK_DIR/conf/log4j.properties

backup_config
set_conf_ssl
start_cluster
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

function bucketing_cleanup() {

  stop_cluster
  $FLINK_DIR/bin/taskmanager.sh stop-all

  # restore default logging level
  sed -i -e 's/log4j.logger.org.apache.flink=DEBUG/#log4j.logger.org.apache.flink=INFO/g' $FLINK_DIR/conf/log4j.properties
}
trap bucketing_cleanup INT
trap bucketing_cleanup EXIT

JOB_ID=$($FLINK_DIR/bin/flink run -d -p 4 $TEST_PROGRAM_JAR -outputPath $TEST_DATA_DIR/out/result \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running ${JOB_ID}

wait_num_checkpoints "${JOB_ID}" 5

echo "Killing TM"

# kill task manager
kill_random_taskmanager

echo "Starting TM"

# start task manager again
$FLINK_DIR/bin/taskmanager.sh start

echo "Killing 2 TMs"

# kill two task managers again shortly after
kill_random_taskmanager
kill_random_taskmanager

echo "Starting 2 TMs and waiting for successful completion"

# start task manager again and let job finish
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

# the job should complete in under 60s because half of the work has been checkpointed
wait_job_terminal_state "${JOB_ID}" "FINISHED"

# get truncate information
# e.g. "xxx xxx DEBUG xxx.BucketingSink  - Writing valid-length file for xxx/out/result8/part-0-0 to specify valid length 74994"
LOG_LINES=$(grep -rnw $FLINK_DIR/log -e 'Writing valid-length file')

# perform truncate on every line
echo "Truncating buckets"
while read -r LOG_LINE; do
  PART=$(echo "$LOG_LINE" | awk '{ print $10 }' FS=" ")
  LENGTH=$(echo "$LOG_LINE" | awk '{ print $15 }' FS=" ")

  echo "Truncating $PART to $LENGTH"

  dd if=$PART of="$PART.truncated" bs=$LENGTH count=1
  rm $PART
  mv "$PART.truncated" $PART
done <<< "$LOG_LINES"

# get all lines in pending or part files
find ${TEST_DATA_DIR}/out -type f \( -iname "*.pending" -or -iname "part-*" \) -exec cat {} + > ${TEST_DATA_DIR}/complete_result

# for debugging purposes
#echo "Checking proper result..."
#for KEY in {0..9}; do
#  for IDX in {0..5999}; do
#    FOUND_LINES=$(grep "($KEY,10,$IDX,Some payload...)" ${TEST_DATA_DIR}/complete_result | wc -l)
#    if [ ${FOUND_LINES} != 1 ] ; then
#      echo "Unexpected count $FOUND_LINES for ($KEY,10,$IDX,Some payload...)"
#      PASS=""
#      exit 1
#    fi
#  done
#done

check_result_hash "Bucketing Sink" $TEST_DATA_DIR/complete_result "01aba5ff77a0ef5e5cf6a727c248bdc3"
