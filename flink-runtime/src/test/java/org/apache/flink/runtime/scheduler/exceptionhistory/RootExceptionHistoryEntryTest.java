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
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code RootExceptionHistoryEntryTest} tests the creation of {@link RootExceptionHistoryEntry}.
 */
class RootExceptionHistoryEntryTest {

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
    void testFromFailureHandlingResultSnapshot() throws ExecutionException, InterruptedException {
        final Throwable rootException = new RuntimeException("Expected root failure");
        final ExecutionVertex rootExecutionVertex = extractExecutionVertex(0);
        final long rootTimestamp = triggerFailure(rootExecutionVertex, rootException);
        final CompletableFuture<Map<String, String>> rootFailureLabels =
                CompletableFuture.completedFuture(Collections.singletonMap("key", "value"));

        final Throwable concurrentException1 = new IllegalStateException("Expected other failure1");
        final ExecutionVertex concurrentlyFailedExecutionVertex1 = extractExecutionVertex(1);
        Predicate<ExceptionHistoryEntry> exception1Predicate =
                triggerFailureAndCreateEntryMatcher(
                        concurrentException1, concurrentlyFailedExecutionVertex1);

        final FailureHandlingResultSnapshot snapshot =
                new FailureHandlingResultSnapshot(
                        rootExecutionVertex.getCurrentExecutionAttempt(),
                        rootException,
                        rootTimestamp,
                        rootFailureLabels,
                        Collections.singleton(
                                concurrentlyFailedExecutionVertex1.getCurrentExecutionAttempt()),
                        true);
        final RootExceptionHistoryEntry actualEntry =
                RootExceptionHistoryEntry.fromFailureHandlingResultSnapshot(snapshot);

        assertThat(actualEntry)
                .matches(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                rootException,
                                rootTimestamp,
                                rootFailureLabels.get(),
                                rootExecutionVertex.getTaskNameWithSubtaskIndex(),
                                rootExecutionVertex.getCurrentAssignedResourceLocation()));

        assertThat(actualEntry.getConcurrentExceptions()).hasSize(1).allMatch(exception1Predicate);

        // Test for addConcurrentExceptions
        final Throwable concurrentException2 = new IllegalStateException("Expected other failure2");
        final ExecutionVertex concurrentlyFailedExecutionVertex2 = extractExecutionVertex(2);
        Predicate<ExceptionHistoryEntry> exception2Predicate =
                triggerFailureAndCreateEntryMatcher(
                        concurrentException2, concurrentlyFailedExecutionVertex2);

        actualEntry.addConcurrentExceptions(
                concurrentlyFailedExecutionVertex2.getCurrentExecutions());
        assertThat(actualEntry.getConcurrentExceptions())
                .hasSize(2)
                .allMatch(
                        exceptionHistoryEntry ->
                                exception1Predicate.test(exceptionHistoryEntry)
                                        || exception2Predicate.test(exceptionHistoryEntry));
    }

    @Test
    void testFromGlobalFailure() throws ExecutionException, InterruptedException {
        final Throwable concurrentException0 =
                new RuntimeException("Expected concurrent failure #0");
        final ExecutionVertex concurrentlyFailedExecutionVertex0 = extractExecutionVertex(0);
        final Predicate<ExceptionHistoryEntry> exception0Predicate =
                triggerFailureAndCreateEntryMatcher(
                        concurrentException0, concurrentlyFailedExecutionVertex0);

        final Throwable concurrentException1 =
                new IllegalStateException("Expected concurrent failure #1");
        final ExecutionVertex concurrentlyFailedExecutionVertex1 = extractExecutionVertex(1);
        final Predicate<ExceptionHistoryEntry> exception1Predicate =
                triggerFailureAndCreateEntryMatcher(
                        concurrentException1, concurrentlyFailedExecutionVertex1);

        final Throwable rootCause = new Exception("Expected root failure");
        final long rootTimestamp = System.currentTimeMillis();
        final CompletableFuture<Map<String, String>> rootFailureLabels =
                CompletableFuture.completedFuture(Collections.singletonMap("key", "value"));
        final RootExceptionHistoryEntry actualEntry =
                RootExceptionHistoryEntry.fromGlobalFailure(
                        rootCause,
                        rootTimestamp,
                        rootFailureLabels,
                        StreamSupport.stream(
                                        executionGraph.getAllExecutionVertices().spliterator(),
                                        false)
                                .map(ExecutionVertex::getCurrentExecutionAttempt)
                                .collect(Collectors.toSet()));

        assertThat(actualEntry)
                .matches(
                        ExceptionHistoryEntryMatcher.matchesGlobalFailure(
                                rootCause, rootTimestamp, rootFailureLabels.get()));

        assertThat(actualEntry.getConcurrentExceptions())
                .allMatch(
                        exceptionHistoryEntry ->
                                exception0Predicate.test(exceptionHistoryEntry)
                                        || exception1Predicate.test(exceptionHistoryEntry));
    }

    private Predicate<ExceptionHistoryEntry> triggerFailureAndCreateEntryMatcher(
            Throwable concurrentException0, ExecutionVertex concurrentlyFailedExecutionVertex0) {
        final long concurrentExceptionTimestamp0 =
                triggerFailure(concurrentlyFailedExecutionVertex0, concurrentException0);
        return ExceptionHistoryEntryMatcher.matchesFailure(
                concurrentException0,
                concurrentExceptionTimestamp0,
                concurrentlyFailedExecutionVertex0.getTaskNameWithSubtaskIndex(),
                concurrentlyFailedExecutionVertex0.getCurrentAssignedResourceLocation());
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
