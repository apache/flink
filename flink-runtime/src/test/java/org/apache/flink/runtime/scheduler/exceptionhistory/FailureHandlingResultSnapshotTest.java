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

package org.apache.flink.runtime.scheduler.exceptionhistory;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code FailureHandlingResultSnapshotTest} tests the creation of {@link
 * FailureHandlingResultSnapshot}.
 */
class FailureHandlingResultSnapshotTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private ExecutionGraph executionGraph;

    @BeforeEach
    void setup() throws JobException, JobExecutionException {
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobGraph.getVertices().forEach(v -> v.setParallelism(3));

        executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    @Test
    void testRootCauseVertexNotFailed() {
        final ExecutionVertex rootCauseExecutionVertex = extractExecutionVertex(0);
        final FailureHandlingResult failureHandlingResult =
                FailureHandlingResult.restartable(
                        rootCauseExecutionVertex.getCurrentExecutionAttempt(),
                        new RuntimeException("Expected exception: root cause"),
                        System.currentTimeMillis(),
                        FailureEnricherUtils.EMPTY_FAILURE_LABELS,
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        false);

        assertThatThrownBy(
                        () ->
                                FailureHandlingResultSnapshot.create(
                                        failureHandlingResult, this::getCurrentExecutions))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test // see FLINK-22060/FLINK-21376
    void testMissingThrowableHandling() throws ExecutionException, InterruptedException {
        final ExecutionVertex rootCauseExecutionVertex = extractExecutionVertex(0);

        final long rootCauseTimestamp = triggerFailure(rootCauseExecutionVertex, null);

        final FailureHandlingResult failureHandlingResult =
                FailureHandlingResult.restartable(
                        rootCauseExecutionVertex.getCurrentExecutionAttempt(),
                        null,
                        rootCauseTimestamp,
                        CompletableFuture.completedFuture(
                                Collections.singletonMap("key2", "value2")),
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        false);

        // FailedExecution with failure labels
        assertThat(failureHandlingResult.getFailureLabels().get())
                .isEqualTo(Collections.singletonMap("key2", "value2"));

        final FailureHandlingResultSnapshot testInstance =
                FailureHandlingResultSnapshot.create(
                        failureHandlingResult, this::getCurrentExecutions);

        final Throwable actualException =
                new SerializedThrowable(testInstance.getRootCause())
                        .deserializeError(ClassLoader.getSystemClassLoader());

        assertThat(actualException).isInstanceOf(FlinkException.class);
        assertThat(actualException)
                .hasMessageContaining(ErrorInfo.handleMissingThrowable(null).getMessage());
        assertThat(testInstance.getTimestamp()).isEqualTo(rootCauseTimestamp);
        assertThat(testInstance.getRootCauseExecution()).isPresent();
        assertThat(testInstance.getRootCauseExecution().get())
                .isSameAs(rootCauseExecutionVertex.getCurrentExecutionAttempt());
    }

    @Test
    void testLocalFailureHandlingResultSnapshotCreation() {
        final ExecutionVertex rootCauseExecutionVertex = extractExecutionVertex(0);
        final Throwable rootCause = new RuntimeException("Expected exception: root cause");
        final ExecutionVertex otherFailedExecutionVertex = extractExecutionVertex(1);
        final Throwable otherFailure =
                new IllegalStateException("Expected exception: other failure");

        final long rootCauseTimestamp = triggerFailure(rootCauseExecutionVertex, rootCause);
        triggerFailure(otherFailedExecutionVertex, otherFailure);

        final FailureHandlingResult failureHandlingResult =
                FailureHandlingResult.restartable(
                        rootCauseExecutionVertex.getCurrentExecutionAttempt(),
                        rootCause,
                        rootCauseTimestamp,
                        FailureEnricherUtils.EMPTY_FAILURE_LABELS,
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        false);

        final FailureHandlingResultSnapshot testInstance =
                FailureHandlingResultSnapshot.create(
                        failureHandlingResult, this::getCurrentExecutions);

        assertThat(testInstance.getRootCause()).isSameAs(rootCause);
        assertThat(testInstance.getTimestamp()).isEqualTo(rootCauseTimestamp);
        assertThat(testInstance.getRootCauseExecution()).isPresent();
        assertThat(testInstance.getRootCauseExecution().get())
                .isSameAs(rootCauseExecutionVertex.getCurrentExecutionAttempt());
        assertThat(testInstance.getConcurrentlyFailedExecution())
                .containsExactly(otherFailedExecutionVertex.getCurrentExecutionAttempt());
    }

    @Test
    void testFailureHandlingWithRootCauseExecutionBeingPartOfConcurrentlyFailedExecutions() {
        final Execution rootCauseExecution = extractExecutionVertex(0).getCurrentExecutionAttempt();

        assertThatThrownBy(
                        () ->
                                new FailureHandlingResultSnapshot(
                                        rootCauseExecution,
                                        new RuntimeException("Expected exception"),
                                        System.currentTimeMillis(),
                                        FailureEnricherUtils.EMPTY_FAILURE_LABELS,
                                        Collections.singleton(rootCauseExecution)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGlobalFailureHandlingResultSnapshotCreation()
            throws ExecutionException, InterruptedException {
        final Throwable rootCause = new FlinkException("Expected exception: root cause");
        final long timestamp = System.currentTimeMillis();

        final ExecutionVertex failedExecutionVertex0 = extractExecutionVertex(0);
        final Throwable failure0 = new RuntimeException("Expected exception: failure #0");
        final ExecutionVertex failedExecutionVertex1 = extractExecutionVertex(1);
        final Throwable failure1 = new IllegalStateException("Expected exception: failure #1");

        triggerFailure(failedExecutionVertex0, failure0);
        triggerFailure(failedExecutionVertex1, failure1);

        final FailureHandlingResult failureHandlingResult =
                FailureHandlingResult.restartable(
                        null,
                        rootCause,
                        timestamp,
                        CompletableFuture.completedFuture(
                                Collections.singletonMap("key2", "value2")),
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        true);

        // FailedExecution with failure labels
        assertThat(failureHandlingResult.getFailureLabels().get())
                .isEqualTo(Collections.singletonMap("key2", "value2"));

        final FailureHandlingResultSnapshot testInstance =
                FailureHandlingResultSnapshot.create(
                        failureHandlingResult, this::getCurrentExecutions);

        assertThat(testInstance.getRootCause()).isSameAs(rootCause);
        assertThat(testInstance.getTimestamp()).isEqualTo(timestamp);
        assertThat(testInstance.getRootCauseExecution()).isNotPresent();
        assertThat(testInstance.getConcurrentlyFailedExecution())
                .containsExactlyInAnyOrder(
                        failedExecutionVertex0.getCurrentExecutionAttempt(),
                        failedExecutionVertex1.getCurrentExecutionAttempt());
    }

    private Collection<Execution> getCurrentExecutions(ExecutionVertexID executionVertexId) {
        if (!executionGraph.getAllVertices().containsKey(executionVertexId.getJobVertexId())) {
            throw new IllegalArgumentException(
                    "The ExecutionJobVertex having the ID "
                            + executionVertexId.getJobVertexId()
                            + " does not exist.");
        }

        final ExecutionVertex[] executions =
                executionGraph
                        .getAllVertices()
                        .get(executionVertexId.getJobVertexId())
                        .getTaskVertices();

        if (executions.length <= executionVertexId.getSubtaskIndex()) {
            throw new IllegalArgumentException(
                    "The ExecutionVertex having the subtask ID "
                            + executionVertexId.getSubtaskIndex()
                            + " for ExecutionJobVertex "
                            + executionVertexId.getJobVertexId()
                            + " does not exist.");
        }

        return executions[executionVertexId.getSubtaskIndex()].getCurrentExecutions();
    }

    private long triggerFailure(ExecutionVertex executionVertex, Throwable throwable) {
        executionGraph.updateState(
                new TaskExecutionStateTransition(
                        new TaskExecutionState(
                                executionVertex.getCurrentExecutionAttempt().getAttemptId(),
                                ExecutionState.FAILED,
                                throwable)));

        return executionVertex
                .getFailureInfo()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "The transition into failed state didn't succeed for ExecutionVertex "
                                                + executionVertex.getID()
                                                + "."))
                .getTimestamp();
    }

    private ExecutionVertex extractExecutionVertex(int pos) {
        final ExecutionVertex executionVertex =
                Iterables.get(executionGraph.getAllExecutionVertices(), pos);
        executionVertex.tryAssignResource(
                new TestingLogicalSlotBuilder().createTestingLogicalSlot());

        return executionVertex;
    }
}
