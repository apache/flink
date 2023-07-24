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

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class SubStatePredicateProviderTest {

    @Test
    void testStreamingWithoutPreviousState() {
        testStreamingPredicate(
                EnumSet.noneOf(ExecutionState.class),
                ExecutionState.DEPLOYING,
                EnumSet.of(ExecutionState.INITIALIZING));
    }

    @Test
    void testStreamingWithPreviousAndNextState() {
        testStreamingPredicate(
                EnumSet.of(ExecutionState.DEPLOYING),
                ExecutionState.INITIALIZING,
                EnumSet.of(ExecutionState.RUNNING));
    }

    @Test
    void testStreamingWithoutNextState() {
        testStreamingPredicate(
                EnumSet.of(ExecutionState.DEPLOYING),
                ExecutionState.INITIALIZING,
                EnumSet.noneOf(ExecutionState.class));
    }

    private static void testStreamingPredicate(
            EnumSet<ExecutionState> previousStates,
            ExecutionState targetState,
            EnumSet<ExecutionState> nextStates) {

        final ExecutionStateCounts executionStateCounts = new ExecutionStateCounts();

        final Supplier<Boolean> predicate =
                SubStatePredicateProvider.getSubStatePredicate(
                        JobType.STREAMING,
                        previousStates,
                        targetState,
                        nextStates,
                        executionStateCounts);

        assertThat(predicate.get()).isFalse();

        executionStateCounts.incrementCount(targetState);
        assertThat(predicate.get()).isTrue();

        for (ExecutionState nextState : nextStates) {
            executionStateCounts.incrementCount(nextState);
            assertThat(predicate.get()).isTrue();
            executionStateCounts.decrementCount(nextState);
        }

        for (ExecutionState previousState : previousStates) {
            executionStateCounts.incrementCount(previousState);
            assertThat(predicate.get()).isFalse();
            executionStateCounts.decrementCount(previousState);
        }

        executionStateCounts.decrementCount(targetState);
        assertThat(predicate.get()).isFalse();
    }

    @Test
    void testBatchWithoutPreviousState() {
        testBatchPredicate(
                EnumSet.noneOf(ExecutionState.class),
                ExecutionState.DEPLOYING,
                EnumSet.of(ExecutionState.INITIALIZING));
    }

    @Test
    void testBatchWithPreviousAndNextState() {
        testBatchPredicate(
                EnumSet.of(ExecutionState.DEPLOYING),
                ExecutionState.INITIALIZING,
                EnumSet.of(ExecutionState.RUNNING));
    }

    @Test
    void testBatchWithoutNextState() {
        testBatchPredicate(
                EnumSet.of(ExecutionState.DEPLOYING),
                ExecutionState.INITIALIZING,
                EnumSet.noneOf(ExecutionState.class));
    }

    private static void testBatchPredicate(
            EnumSet<ExecutionState> previousStates,
            ExecutionState targetState,
            EnumSet<ExecutionState> nextStates) {

        final ExecutionStateCounts executionStateCounts = new ExecutionStateCounts();

        final Supplier<Boolean> predicate =
                SubStatePredicateProvider.getSubStatePredicate(
                        JobType.BATCH,
                        previousStates,
                        targetState,
                        nextStates,
                        executionStateCounts);

        assertThat(predicate.get()).isFalse();

        executionStateCounts.incrementCount(targetState);
        assertThat(predicate.get()).isTrue();

        for (ExecutionState nextState : nextStates) {
            executionStateCounts.incrementCount(nextState);
            assertThat(predicate.get()).isFalse();
            executionStateCounts.decrementCount(nextState);
        }

        for (ExecutionState previousState : previousStates) {
            executionStateCounts.incrementCount(previousState);
            assertThat(predicate.get()).isTrue();
            executionStateCounts.decrementCount(previousState);
        }

        executionStateCounts.decrementCount(targetState);
        assertThat(predicate.get()).isFalse();
    }
}
