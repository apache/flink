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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertThat;

/**
 * {@code RootExceptionHistoryEntryTest} tests the creation of {@link RootExceptionHistoryEntry}.
 */
public class RootExceptionHistoryEntryTest extends TestLogger {

    private ExecutionGraph executionGraph;

    @Before
    public void setup() throws JobException, JobExecutionException {
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobGraph.getVertices().forEach(v -> v.setParallelism(3));

        executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();
        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    @Test
    public void testFromFailureHandlingResultSnapshot() {
        final Throwable rootException = new RuntimeException("Expected root failure");
        final ExecutionVertex rootExecutionVertex = extractExecutionVertex(0);
        final long rootTimestamp = triggerFailure(rootExecutionVertex, rootException);

        final Throwable concurrentException = new IllegalStateException("Expected other failure");
        final ExecutionVertex concurrentlyFailedExecutionVertex = extractExecutionVertex(1);
        final long concurrentExceptionTimestamp =
                triggerFailure(concurrentlyFailedExecutionVertex, concurrentException);

        final FailureHandlingResultSnapshot snapshot =
                new FailureHandlingResultSnapshot(
                        rootExecutionVertex.getCurrentExecutionAttempt(),
                        rootException,
                        rootTimestamp,
                        Collections.singleton(
                                concurrentlyFailedExecutionVertex.getCurrentExecutionAttempt()));
        final RootExceptionHistoryEntry actualEntry =
                RootExceptionHistoryEntry.fromFailureHandlingResultSnapshot(snapshot);

        assertThat(
                actualEntry,
                ExceptionHistoryEntryMatcher.matchesFailure(
                        rootException,
                        rootTimestamp,
                        rootExecutionVertex.getTaskNameWithSubtaskIndex(),
                        rootExecutionVertex.getCurrentAssignedResourceLocation()));
        assertThat(
                actualEntry.getConcurrentExceptions(),
                IsIterableContainingInOrder.contains(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                concurrentException,
                                concurrentExceptionTimestamp,
                                concurrentlyFailedExecutionVertex.getTaskNameWithSubtaskIndex(),
                                concurrentlyFailedExecutionVertex
                                        .getCurrentAssignedResourceLocation())));
    }

    @Test
    public void testFromGlobalFailure() {
        final Throwable concurrentException0 =
                new RuntimeException("Expected concurrent failure #0");
        final ExecutionVertex concurrentlyFailedExecutionVertex0 = extractExecutionVertex(0);
        final long concurrentExceptionTimestamp0 =
                triggerFailure(concurrentlyFailedExecutionVertex0, concurrentException0);

        final Throwable concurrentException1 =
                new IllegalStateException("Expected concurrent failure #1");
        final ExecutionVertex concurrentlyFailedExecutionVertex1 = extractExecutionVertex(1);
        final long concurrentExceptionTimestamp1 =
                triggerFailure(concurrentlyFailedExecutionVertex1, concurrentException1);

        final Throwable rootCause = new Exception("Expected root failure");
        final long rootTimestamp = System.currentTimeMillis();
        final RootExceptionHistoryEntry actualEntry =
                RootExceptionHistoryEntry.fromGlobalFailure(
                        rootCause,
                        rootTimestamp,
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getCurrentExecutionAttempt)
                                .collect(Collectors.toSet()));

        assertThat(
                actualEntry,
                ExceptionHistoryEntryMatcher.matchesGlobalFailure(rootCause, rootTimestamp));
        assertThat(
                actualEntry.getConcurrentExceptions(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                concurrentException0,
                                concurrentExceptionTimestamp0,
                                concurrentlyFailedExecutionVertex0.getTaskNameWithSubtaskIndex(),
                                concurrentlyFailedExecutionVertex0
                                        .getCurrentAssignedResourceLocation()),
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                concurrentException1,
                                concurrentExceptionTimestamp1,
                                concurrentlyFailedExecutionVertex1.getTaskNameWithSubtaskIndex(),
                                concurrentlyFailedExecutionVertex1
                                        .getCurrentAssignedResourceLocation())));
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
