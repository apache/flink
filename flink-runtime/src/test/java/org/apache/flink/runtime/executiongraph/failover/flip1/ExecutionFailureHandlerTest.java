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

import org.apache.flink.core.failure.TestingFailureEnricher;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResultTest.createExecution;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecutionFailureHandler}. */
class ExecutionFailureHandlerTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final long RESTART_DELAY_MS = 1234L;

    private SchedulingTopology schedulingTopology;

    private TestFailoverStrategy failoverStrategy;

    private TestRestartBackoffTimeStrategy backoffTimeStrategy;

    private ExecutionFailureHandler executionFailureHandler;

    private TestingFailureEnricher testingFailureEnricher;

    @BeforeEach
    void setUp() {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();
        topology.newExecutionVertex();
        schedulingTopology = topology;

        failoverStrategy = new TestFailoverStrategy();
        testingFailureEnricher = new TestingFailureEnricher();
        backoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, RESTART_DELAY_MS);
        executionFailureHandler =
                new ExecutionFailureHandler(
                        schedulingTopology,
                        failoverStrategy,
                        backoffTimeStrategy,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        Collections.singleton(testingFailureEnricher),
                        null,
                        null);
    }

    /** Tests the case that task restarting is accepted. */
    @Test
    void testNormalFailureHandling() throws Exception {
        final Set<ExecutionVertexID> tasksToRestart =
                Collections.singleton(new ExecutionVertexID(new JobVertexID(), 0));
        failoverStrategy.setTasksToRestart(tasksToRestart);

        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());
        Exception cause = new Exception("test failure");
        long timestamp = System.currentTimeMillis();
        // trigger a task failure
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(execution, cause, timestamp);

        // verify results
        assertThat(result.canRestart()).isTrue();
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
        assertThat(result.getRestartDelayMS()).isEqualTo(RESTART_DELAY_MS);
        assertThat(result.getVerticesToRestart()).isEqualTo(tasksToRestart);
        assertThat(result.getError()).isSameAs(cause);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(cause);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
        assertThat(executionFailureHandler.getNumberOfRestarts()).isOne();
    }

    /** Tests the case that task restarting is suppressed. */
    @Test
    void testRestartingSuppressedFailureHandlingResult() throws Exception {
        // restart strategy suppresses restarting
        backoffTimeStrategy.setCanRestart(false);

        // trigger a task failure
        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());
        final Throwable error = new Exception("expected test failure");
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(execution, error, timestamp);

        // verify results
        assertThat(result.canRestart()).isFalse();
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
        assertThat(result.getError()).hasCause(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(error);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
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
    void testNonRecoverableFailureHandlingResult() throws Exception {

        // trigger an unrecoverable task failure
        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());
        final Throwable error =
                new Exception(new SuppressRestartsException(new Exception("test failure")));
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(execution, error, timestamp);

        // verify results
        assertThat(result.canRestart()).isFalse();
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
        assertThat(result.getError()).isNotNull();
        assertThat(ExecutionFailureHandler.isUnrecoverableError(result.getError())).isTrue();
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(error);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
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
    void testGlobalFailureHandling() throws ExecutionException, InterruptedException {
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
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(error);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
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
