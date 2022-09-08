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

package org.apache.flink.runtime.scheduler.metrics.utils;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobType;

import java.util.EnumSet;
import java.util.function.Supplier;

/** Helper class to provide predicates for whether we are in a given sub-state. */
public class SubStatePredicateProvider {

    /**
     * Returns a predicated for whether the job is currently in the target sub-state.
     *
     * @param semantic whether we use Batch or Streaming semantics
     * @param previousStates the sub-states that precede the target state
     * @param targetState the state which the predicate should evaluate
     * @param nextStates the sub-state that follow the target state
     * @param executionStateCounts provides how many executions are in each state
     * @return predicate that determines whether the job is currently in the target sub-state
     */
    public static Supplier<Boolean> getSubStatePredicate(
            JobType semantic,
            EnumSet<ExecutionState> previousStates,
            ExecutionState targetState,
            EnumSet<ExecutionState> nextStates,
            ExecutionStateCountsHolder executionStateCounts) {

        return semantic == JobType.BATCH
                ? () ->
                        executionStateCounts.getNumExecutionsInState(targetState) > 0
                                && isNoExecutionInAnyStateOf(nextStates, executionStateCounts)
                : () ->
                        executionStateCounts.getNumExecutionsInState(targetState) > 0
                                && isNoExecutionInAnyStateOf(previousStates, executionStateCounts);
    }

    private static boolean isNoExecutionInAnyStateOf(
            EnumSet<ExecutionState> executionStates,
            ExecutionStateCountsHolder executionStateCounts) {
        return executionStates.stream()
                .map(executionStateCounts::getNumExecutionsInState)
                .noneMatch(count -> count > 0);
    }
}
