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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecutionFailureHandler}. */
class ExecutionFailureHandlerTest {

    private static final long RESTART_DELAY_MS = 1234L;

    private SchedulingTopology schedulingTopology;

    private TestFailoverStrategy failoverStrategy;

    private TestRestartBackoffTimeStrategy backoffTimeStrategy;

    private ExecutionFailureHandler executionFailureHandler;

    @BeforeEach
    void setUp() {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();
        topology.newExecutionVertex();
        schedulingTopology = topology;

        failoverStrategy = new TestFailoverStrategy();
        backoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, RESTART_DELAY_MS);
        executionFailureHandler =
                new ExecutionFailureHandler(
                        schedulingTopology, failoverStrategy, backoffTimeStrategy);
    }

    /** Tests the case that task restarting is accepted. */
    @Test
    void testNormalFailureHandling() {
        final Set<ExecutionVertexID> tasksToRestart =
                Collections.singleton(new ExecutionVertexID(new JobVertexID(), 0));
        failoverStrategy.setTasksToRestart(tasksToRestart);

        Exception cause = new Exception("test failure");
        long timestamp = System.currentTimeMillis();
        // trigger a task failure
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(
                        new ExecutionVertexID(new JobVertexID(), 0), cause, timestamp);

        // verify results
        assertThat(result.canRestart()).isTrue();
        assertThat(result.getRestartDelayMS()).isEqualTo(RESTART_DELAY_MS);
        assertThat(result.getVerticesToRestart()).isEqualTo(tasksToRestart);
        assertThat(result.getError()).isSameAs(cause);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(executionFailureHandler.getNumberOfRestarts()).isEqualTo(1);
    }

    /** Tests the case that task restarting is suppressed. */
    @Test
    void testRestartingSuppressedFailureHandlingResult() {
        // restart strategy suppresses restarting
        backoffTimeStrategy.setCanRestart(false);

        // trigger a task failure
        final Throwable error = new Exception("expected test failure");
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(
                        new ExecutionVertexID(new JobVertexID(), 0), error, timestamp);

        // verify results
        assertThat(result.canRestart()).isFalse();
        assertThat(result.getError()).hasCause(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(ExecutionFailureHandler.isUnrecoverableError(result.getError())).isFalse();

        assertThatThrownBy(result::getVerticesToRestart)
                .as("getVerticesToRestart is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(result::getRestartDelayMS)
                .as("getRestartDelayMS is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThat(executionFailureHandler.getNumberOfRestarts()).isZero();
    }

    /** Tests the case that the failure is non-recoverable type. */
    @Test
    void testNonRecoverableFailureHandlingResult() {
        // trigger an unrecoverable task failure
        final Throwable error =
                new Exception(new SuppressRestartsException(new Exception("test failure")));
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(
                        new ExecutionVertexID(new JobVertexID(), 0), error, timestamp);

        // verify results
        assertThat(result.canRestart()).isFalse();
        assertThat(result.getError()).isNotNull();
        assertThat(ExecutionFailureHandler.isUnrecoverableError(result.getError())).isTrue();
        assertThat(result.getTimestamp()).isEqualTo(timestamp);

        assertThatThrownBy(result::getVerticesToRestart)
                .as("getVerticesToRestart is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(result::getRestartDelayMS)
                .as("getRestartDelayMS is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThat(executionFailureHandler.getNumberOfRestarts()).isZero();
    }

    /** Tests the check for unrecoverable error. */
    @Test
    void testUnrecoverableErrorCheck() {
        // normal error
        assertThat(ExecutionFailureHandler.isUnrecoverableError(new Exception())).isFalse();

        // direct unrecoverable error
        assertThat(
                        ExecutionFailureHandler.isUnrecoverableError(
                                new SuppressRestartsException(new Exception())))
                .isTrue();

        // nested unrecoverable error
        assertThat(
                        ExecutionFailureHandler.isUnrecoverableError(
                                new Exception(new SuppressRestartsException(new Exception()))))
                .isTrue();
    }

    @Test
    void testGlobalFailureHandling() {
        final Throwable error = new Exception("Expected test failure");
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getGlobalFailureHandlingResult(error, timestamp);

        assertThat(result.getVerticesToRestart())
                .isEqualTo(
                        IterableUtils.toStream(schedulingTopology.getVertices())
                                .map(SchedulingExecutionVertex::getId)
                                .collect(Collectors.toSet()));
        assertThat(result.getError()).isSameAs(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    /**
     * A FailoverStrategy implementation for tests. It always suggests restarting the given tasks to
     * restart.
     */
    private static class TestFailoverStrategy implements FailoverStrategy {

        private Set<ExecutionVertexID> tasksToRestart;

        public TestFailoverStrategy() {}

        public void setTasksToRestart(final Set<ExecutionVertexID> tasksToRestart) {
            this.tasksToRestart = tasksToRestart;
        }

        @Override
        public Set<ExecutionVertexID> getTasksNeedingRestart(
                final ExecutionVertexID executionVertexId, final Throwable cause) {

            return tasksToRestart;
        }
    }
}
