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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.taskexecutor.IdleTestTask;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.util.JvmUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.taskexecutor.IdleTestTask.executeWithTerminationGuarantee;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ThreadInfoRequestCoordinator}. */
class ThreadInfoRequestCoordinatorTest {

    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(100);
    private static final String REQUEST_TIMEOUT_MESSAGE = "Request timeout.";

    private static final int DEFAULT_NUMBER_OF_SAMPLES = 1;
    private static final int DEFAULT_MAX_STACK_TRACE_DEPTH = 100;
    private static final Duration DEFAULT_DELAY_BETWEEN_SAMPLES = Duration.ofMillis(50);

    private static ScheduledExecutorService executorService;
    private ThreadInfoRequestCoordinator coordinator;

    @BeforeAll
    static void setUp() {
        executorService = new ScheduledThreadPoolExecutor(1);
    }

    @AfterAll
    static void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @BeforeEach
    void initCoordinator() {
        coordinator = new ThreadInfoRequestCoordinator(executorService, REQUEST_TIMEOUT);
    }

    @AfterEach
    void shutdownCoordinator() {
        if (coordinator != null) {
            // verify no more pending request
            assertThat(coordinator.getNumberOfPendingRequests()).isZero();
            coordinator.shutDown();
        }
    }

    /** Tests successful thread info stats request. */
    @Test
    void testSuccessfulThreadInfoRequest() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                CompletionType.SUCCESSFULLY, CompletionType.SUCCESSFULLY);

        CompletableFuture<VertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        VertexThreadInfoStats threadInfoStats = requestFuture.get();

        // verify the request result
        assertThat(threadInfoStats.getRequestId()).isEqualTo(0);

        Map<ExecutionAttemptID, Collection<ThreadInfoSample>> samplesBySubtask =
                threadInfoStats.getSamplesBySubtask();

        for (Collection<ThreadInfoSample> result : samplesBySubtask.values()) {
            StackTraceElement[] stackTrace = result.iterator().next().getStackTrace();
            assertThat(stackTrace).isNotEmpty();
        }
    }

    /** Tests that failed thread info request to one of the tasks fails the future. */
    @Test
    void testThreadInfoRequestWithException() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                CompletionType.SUCCESSFULLY, CompletionType.EXCEPTIONALLY);

        CompletableFuture<VertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        assertThatThrownBy(requestFuture::get, "The request must be failed.")
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class);
    }

    /** Tests that thread info stats request times out if not finished in time. */
    @Test
    void testThreadInfoRequestTimeout() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                CompletionType.SUCCESSFULLY, CompletionType.TIMEOUT);

        CompletableFuture<VertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        try {
            assertThatThrownBy(requestFuture::get, "The request must be failed.")
                    .satisfies(anyCauseMatches(REQUEST_TIMEOUT_MESSAGE));
        } finally {
            coordinator.shutDown();
        }
    }

    /** Tests that shutdown fails all pending requests and future request triggers. */
    @Test
    void testShutDown() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                // request future will only be completed after all gateways
                                // successfully return thread infos.
                                CompletionType.SUCCESSFULLY, CompletionType.NEVER_COMPLETE);

        List<CompletableFuture<VertexThreadInfoStats>> requestFutures = new ArrayList<>();

        CompletableFuture<VertexThreadInfoStats> requestFuture1 =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        CompletableFuture<VertexThreadInfoStats> requestFuture2 =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        // trigger request
        requestFutures.add(requestFuture1);
        requestFutures.add(requestFuture2);

        for (CompletableFuture<VertexThreadInfoStats> future : requestFutures) {
            assertThat(future).isNotDone();
        }

        // shut down
        coordinator.shutDown();

        // verify all completed
        for (CompletableFuture<VertexThreadInfoStats> future : requestFutures) {
            assertThat(future).isCompletedExceptionally();
        }

        // verify new trigger returns failed future
        CompletableFuture<VertexThreadInfoStats> future =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        assertThat(future).isCompletedExceptionally();
    }

    private static CompletableFuture<TaskExecutorThreadInfoGateway> createMockTaskManagerGateway(
            CompletionType completionType) throws Exception {

        final CompletableFuture<TaskThreadInfoResponse> responseFuture = new CompletableFuture<>();
        switch (completionType) {
            case SUCCESSFULLY:
                Set<IdleTestTask> tasks = new HashSet<>();
                executeWithTerminationGuarantee(
                        () -> {
                            tasks.add(new IdleTestTask());
                            tasks.add(new IdleTestTask());
                            Map<Long, ExecutionAttemptID> threads =
                                    tasks.stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            task ->
                                                                    task.getExecutingThread()
                                                                            .getId(),
                                                            IdleTestTask::getExecutionId));

                            Map<ExecutionAttemptID, Collection<ThreadInfoSample>> threadInfoSample =
                                    JvmUtils.createThreadInfoSample(threads.keySet(), 100)
                                            .entrySet().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            entry -> threads.get(entry.getKey()),
                                                            entry ->
                                                                    Collections.singletonList(
                                                                            entry.getValue())));

                            responseFuture.complete(new TaskThreadInfoResponse(threadInfoSample));
                        },
                        tasks);

                break;
            case EXCEPTIONALLY:
                responseFuture.completeExceptionally(new RuntimeException("Request failed."));
                break;
            case TIMEOUT:
                executorService.schedule(
                        () ->
                                responseFuture.completeExceptionally(
                                        new TimeoutException(REQUEST_TIMEOUT_MESSAGE)),
                        REQUEST_TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS);
                break;
            case NEVER_COMPLETE:
                // do nothing
                break;
            default:
                throw new RuntimeException("Unknown completion type.");
        }

        final TaskExecutorThreadInfoGateway executorGateway =
                (taskExecutionAttemptId, requestParams, timeout) -> responseFuture;

        return CompletableFuture.completedFuture(executorGateway);
    }

    private static Map<
                    ImmutableSet<ExecutionAttemptID>,
                    CompletableFuture<TaskExecutorThreadInfoGateway>>
            createMockSubtaskWithGateways(CompletionType... completionTypes) throws Exception {
        final Map<
                        ImmutableSet<ExecutionAttemptID>,
                        CompletableFuture<TaskExecutorThreadInfoGateway>>
                result = new HashMap<>();
        for (CompletionType completionType : completionTypes) {
            ImmutableSet<ExecutionAttemptID> ids =
                    ImmutableSet.of(createExecutionAttemptId(), createExecutionAttemptId());
            result.put(ids, createMockTaskManagerGateway(completionType));
        }
        return result;
    }

    /** Completion types of the request future. */
    private enum CompletionType {
        SUCCESSFULLY,
        EXCEPTIONALLY,
        TIMEOUT,
        NEVER_COMPLETE
    }
}
