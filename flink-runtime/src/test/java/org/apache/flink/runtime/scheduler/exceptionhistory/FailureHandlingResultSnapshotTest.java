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

import org.apache.flink.core.testutils.FlinkMatchers;
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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * {@code FailureHandlingResultSnapshotTest} tests the creation of {@link
 * FailureHandlingResultSnapshot}.
 */
public class FailureHandlingResultSnapshotTest extends TestLogger {

    private ExecutionGraph executionGraph;

    @Before
    public void setup() throws JobException, JobExecutionException {
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobGraph.getVertices().forEach(v -> v.setParallelism(3));

        executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();
        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRootCauseVertexNotFailed() {
        final ExecutionVertex rootCauseExecutionVertex = extractExecutionVertex(0);
        final FailureHandlingResult failureHandlingResult =
                FailureHandlingResult.restartable(
                        rootCauseExecutionVertex.getID(),
                        new RuntimeException("Expected exception: root cause"),
                        System.currentTimeMillis(),
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        false);

        FailureHandlingResultSnapshot.create(failureHandlingResult, this::getLatestExecution);
    }

    @Test // see FLINK-22060/FLINK-21376
    public void testMissingThrowableHandling() {
        final ExecutionVertex rootCauseExecutionVertex = extractExecutionVertex(0);

        final long rootCauseTimestamp = triggerFailure(rootCauseExecutionVertex, null);

        final FailureHandlingResult failureHandlingResult =
                FailureHandlingResult.restartable(
                        rootCauseExecutionVertex.getID(),
                        null,
                        rootCauseTimestamp,
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        false);

        final FailureHandlingResultSnapshot testInstance =
                FailureHandlingResultSnapshot.create(
                        failureHandlingResult, this::getLatestExecution);

        final Throwable actualException =
                new SerializedThrowable(testInstance.getRootCause())
                        .deserializeError(ClassLoader.getSystemClassLoader());
        assertThat(actualException, IsInstanceOf.instanceOf(FlinkException.class));
        assertThat(
                actualException,
                FlinkMatchers.containsMessage(ErrorInfo.handleMissingThrowable(null).getMessage()));
        assertThat(testInstance.getTimestamp(), is(rootCauseTimestamp));
        assertThat(testInstance.getRootCauseExecution().isPresent(), is(true));
        assertThat(
                testInstance.getRootCauseExecution().get(),
                is(rootCauseExecutionVertex.getCurrentExecutionAttempt()));
    }

    @Test
    public void testLocalFailureHandlingResultSnapshotCreation() {
        final ExecutionVertex rootCauseExecutionVertex = extractExecutionVertex(0);
        final Throwable rootCause = new RuntimeException("Expected exception: root cause");
        final ExecutionVertex otherFailedExecutionVertex = extractExecutionVertex(1);
        final Throwable otherFailure =
                new IllegalStateException("Expected exception: other failure");

        final long rootCauseTimestamp = triggerFailure(rootCauseExecutionVertex, rootCause);
        triggerFailure(otherFailedExecutionVertex, otherFailure);

        final FailureHandlingResult failureHandlingResult =
                FailureHandlingResult.restartable(
                        rootCauseExecutionVertex.getID(),
                        rootCause,
                        rootCauseTimestamp,
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        false);

        final FailureHandlingResultSnapshot testInstance =
                FailureHandlingResultSnapshot.create(
                        failureHandlingResult, this::getLatestExecution);

        assertThat(testInstance.getRootCause(), is(rootCause));
        assertThat(testInstance.getTimestamp(), is(rootCauseTimestamp));
        assertThat(testInstance.getRootCauseExecution().isPresent(), is(true));
        assertThat(
                testInstance.getRootCauseExecution().get(),
                is(rootCauseExecutionVertex.getCurrentExecutionAttempt()));

        assertThat(
                testInstance.getConcurrentlyFailedExecution(),
                IsIterableContainingInOrder.contains(
                        otherFailedExecutionVertex.getCurrentExecutionAttempt()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailureHandlingWithRootCauseExecutionBeingPartOfConcurrentlyFailedExecutions() {
        final Execution rootCauseExecution = extractExecutionVertex(0).getCurrentExecutionAttempt();
        new FailureHandlingResultSnapshot(
                rootCauseExecution,
                new RuntimeException("Expected exception"),
                System.currentTimeMillis(),
                Collections.singleton(rootCauseExecution));
    }

    @Test
    public void testGlobalFailureHandlingResultSnapshotCreation() {
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
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()),
                        0L,
                        true);

        final FailureHandlingResultSnapshot testInstance =
                FailureHandlingResultSnapshot.create(
                        failureHandlingResult, this::getLatestExecution);

        assertThat(testInstance.getRootCause(), is(rootCause));
        assertThat(testInstance.getTimestamp(), is(timestamp));
        assertThat(testInstance.getRootCauseExecution().isPresent(), is(false));

        assertThat(
                testInstance.getConcurrentlyFailedExecution(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(
                        failedExecutionVertex0.getCurrentExecutionAttempt(),
                        failedExecutionVertex1.getCurrentExecutionAttempt()));
    }

    private Execution getLatestExecution(ExecutionVertexID executionVertexId) {
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

        return executions[executionVertexId.getSubtaskIndex()].getCurrentExecutionAttempt();
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
