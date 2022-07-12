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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.taskexecutor.IdleTestTask.executeWithTerminationGuarantee;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Fail.fail;

/** Tests for the {@link ThreadInfoRequestCoordinator}. */
public class ThreadInfoRequestCoordinatorTest extends TestLogger {

    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(100);
    private static final String REQUEST_TIMEOUT_MESSAGE = "Request timeout.";

    private static final int DEFAULT_NUMBER_OF_SAMPLES = 1;
    private static final int DEFAULT_MAX_STACK_TRACE_DEPTH = 100;
    private static final Duration DEFAULT_DELAY_BETWEEN_SAMPLES = Duration.ofMillis(50);

    private static ScheduledExecutorService executorService;
    private ThreadInfoRequestCoordinator coordinator;

    @BeforeAll
    public static void setUp() throws Exception {
        executorService = new ScheduledThreadPoolExecutor(1);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @BeforeEach
    public void initCoordinator() throws Exception {
        coordinator = new ThreadInfoRequestCoordinator(executorService, REQUEST_TIMEOUT);
    }

    @AfterEach
    public void shutdownCoordinator() throws Exception {
        if (coordinator != null) {
            // verify no more pending request
            assertThat(coordinator.getNumberOfPendingRequests()).isEqualTo(0);
            coordinator.shutDown();
        }
    }

    /** Tests successful thread info stats request. */
    @Test
    public void testSuccessfulThreadInfoRequest() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                CompletionType.SUCCESSFULLY, CompletionType.SUCCESSFULLY);

        CompletableFuture<JobVertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        JobVertexThreadInfoStats threadInfoStats = requestFuture.get();

        // verify the request result
        assertThat(threadInfoStats.getRequestId()).isEqualTo(0);

        Map<ImmutableSet<ExecutionAttemptID>, Collection<ThreadInfoSample>> samplesBySubtask =
                threadInfoStats.getSamplesBySubtask();

        for (Collection<ThreadInfoSample> result : samplesBySubtask.values()) {
            StackTraceElement[] stackTrace = result.iterator().next().getStackTrace();
            assertThat(stackTrace).isNotEmpty();
        }
    }

    /** Tests that failed thread info request to one of the tasks fails the future. */
    @Test
    public void testThreadInfoRequestWithException() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                CompletionType.SUCCESSFULLY, CompletionType.EXCEPTIONALLY);

        CompletableFuture<JobVertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        try {
            requestFuture.get();
            fail("Exception expected.");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(RuntimeException.class);
        }
    }

    /** Tests that thread info stats request times out if not finished in time. */
    @Test
    public void testThreadInfoRequestTimeout() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                CompletionType.SUCCESSFULLY, CompletionType.TIMEOUT);

        CompletableFuture<JobVertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        try {
            requestFuture.get();
            fail("Exception expected.");
        } catch (ExecutionException e) {
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(e, REQUEST_TIMEOUT_MESSAGE)
                                    .isPresent())
                    .isTrue();
        } finally {
            coordinator.shutDown();
        }
    }

    /** Tests that shutdown fails all pending requests and future request triggers. */
    @Test
    public void testShutDown() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateways =
                        createMockSubtaskWithGateways(
                                CompletionType.SUCCESSFULLY, CompletionType.TIMEOUT);

        List<CompletableFuture<JobVertexThreadInfoStats>> requestFutures = new ArrayList<>();

        CompletableFuture<JobVertexThreadInfoStats> requestFuture1 =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        CompletableFuture<JobVertexThreadInfoStats> requestFuture2 =
                coordinator.triggerThreadInfoRequest(
                        executionWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        // trigger request
        requestFutures.add(requestFuture1);
        requestFutures.add(requestFuture2);

        for (CompletableFuture<JobVertexThreadInfoStats> future : requestFutures) {
            assertThat(future).isNotDone();
        }

        // shut down
        coordinator.shutDown();

        // verify all completed
        for (CompletableFuture<JobVertexThreadInfoStats> future : requestFutures) {
            assertThat(future).isCompletedExceptionally();
        }

        // verify new trigger returns failed future
        CompletableFuture<JobVertexThreadInfoStats> future =
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
                            //                            Thread.sleep(100);
                            List<Long> threadIds =
                                    tasks.stream()
                                            .map(t -> t.getExecutingThread().getId())
                                            .collect(Collectors.toList());
                            Collection<ThreadInfoSample> threadInfoSample =
                                    JvmUtils.createThreadInfoSample(threadIds, 100);
                            responseFuture.complete(
                                    new TaskThreadInfoResponse(new ArrayList<>(threadInfoSample)));
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
