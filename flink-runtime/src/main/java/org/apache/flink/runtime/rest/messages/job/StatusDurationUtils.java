/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;

import java.util.HashMap;
import java.util.Map;

/** Utilities to calculate status duration. */
public class StatusDurationUtils {
    public static Map<ExecutionState, Long> getExecutionStateDuration(AccessExecution execution) {
        Map<ExecutionState, Long> executionStateDuration = new HashMap<>();
        long now = System.currentTimeMillis();
        ExecutionState state = execution.getState();
        executionStateDuration.put(
                ExecutionState.CREATED,
                calculateStateDuration(
                        execution.getStateTimestamp(ExecutionState.CREATED),
                        state == ExecutionState.CREATED
                                ? now
                                : execution.getStateEndTimestamp(ExecutionState.CREATED)));
        executionStateDuration.put(
                ExecutionState.SCHEDULED,
                calculateStateDuration(
                        execution.getStateTimestamp(ExecutionState.SCHEDULED),
                        state == ExecutionState.SCHEDULED
                                ? now
                                : execution.getStateEndTimestamp(ExecutionState.SCHEDULED)));
        executionStateDuration.put(
                ExecutionState.DEPLOYING,
                calculateStateDuration(
                        execution.getStateTimestamp(ExecutionState.DEPLOYING),
                        state == ExecutionState.DEPLOYING
                                ? now
                                : execution.getStateEndTimestamp(ExecutionState.DEPLOYING)));
        executionStateDuration.put(
                ExecutionState.INITIALIZING,
                calculateStateDuration(
                        execution.getStateTimestamp(ExecutionState.INITIALIZING),
                        state == ExecutionState.INITIALIZING
                                ? now
                                : execution.getStateEndTimestamp(ExecutionState.INITIALIZING)));
        executionStateDuration.put(
                ExecutionState.RUNNING,
                calculateStateDuration(
                        execution.getStateTimestamp(ExecutionState.RUNNING),
                        state == ExecutionState.RUNNING
                                ? now
                                : execution.getStateEndTimestamp(ExecutionState.RUNNING)));
        return executionStateDuration;
    }

    private static long calculateStateDuration(long start, long end) {
        if (start == 0 || end == 0) {
            return -1;
        }
        return end - start;
    }
}
