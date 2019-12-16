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

END_TO_END_DIR="`dirname \"$0\"`" # relative
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd )`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

export END_TO_END_DIR

if [ -z "$FLINK_DIR" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

source "${END_TO_END_DIR}/test-scripts/test-runner-common.sh"

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd )`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

# Template for adding a test:

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>"

run_test "Resuming Savepoint (file, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 file true"
run_test "Resuming Savepoint (file, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 file false"
run_test "Resuming Savepoint (file, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 file true"
run_test "Resuming Savepoint (file, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 file false"
run_test "Resuming Savepoint (file, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 file true"
run_test "Resuming Savepoint (file, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 file false"
run_test "Resuming Savepoint (rocks, no parallelism change, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false heap"
run_test "Resuming Savepoint (rocks, scale up, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false heap"
run_test "Resuming Savepoint (rocks, scale down, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false heap"
run_test "Resuming Savepoint (rocks, no parallelism change, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false rocks"
run_test "Resuming Savepoint (rocks, scale up, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false rocks"
run_test "Resuming Savepoint (rocks, scale down, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false rocks"

run_test "Resuming Externalized Checkpoint (file, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 file true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 file false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 file true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 file false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, non-incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 rocks true false" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 rocks true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 rocks true false" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 rocks true true" "skip_check_exceptions"

run_test "Resuming Externalized Checkpoint after terminal failure (file, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file true false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint after terminal failure (file, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file false false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint after terminal failure (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint after terminal failure (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true true" "skip_check_exceptions"

printf "\n[PASS] All tests passed\n"
exit 0
