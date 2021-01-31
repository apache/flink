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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link ThreadInfoRequestCoordinator}. */
public class ThreadInfoRequestCoordinatorTest extends TestLogger {

    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(100);
    private static final String REQUEST_TIMEOUT_MESSAGE = "Request timeout.";

    private static final int DEFAULT_NUMBER_OF_SAMPLES = 1;
    private static final int DEFAULT_MAX_STACK_TRACE_DEPTH = 100;
    private static final Duration DEFAULT_DELAY_BETWEEN_SAMPLES = Duration.ofMillis(50);

    private static ScheduledExecutorService executorService;
    private ThreadInfoRequestCoordinator coordinator;

    @Rule public Timeout caseTimeout = new Timeout(10, TimeUnit.SECONDS);

    @BeforeClass
    public static void setUp() throws Exception {
        executorService = new ScheduledThreadPoolExecutor(1);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Before
    public void initCoordinator() throws Exception {
        coordinator = new ThreadInfoRequestCoordinator(executorService, REQUEST_TIMEOUT);
    }

    @After
    public void shutdownCoordinator() throws Exception {
        if (coordinator != null) {
            // verify no more pending request
            assertEquals(0, coordinator.getNumberOfPendingRequests());
            coordinator.shutDown();
        }
    }

    /** Tests successful thread info stats request. */
    @Test
    public void testSuccessfulThreadInfoRequest() throws Exception {
        List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                subtaskWithGateways =
                        Arrays.asList(
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.SUCCESSFULLY),
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.SUCCESSFULLY));

        CompletableFuture<JobVertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        subtaskWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        JobVertexThreadInfoStats threadInfoStats = requestFuture.get();

        // verify the request result
        assertEquals(0, threadInfoStats.getRequestId());

        Map<ExecutionAttemptID, List<ThreadInfoSample>> samplesBySubtask =
                threadInfoStats.getSamplesBySubtask();

        for (List<ThreadInfoSample> result : samplesBySubtask.values()) {
            assertThat(result.get(0).getStackTrace(), not(emptyArray()));
        }
    }

    /** Tests that thread info stats request of non-running tasks fails the future. */
    @Test
    public void testRequestNotRunningTasks() throws Exception {
        List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                subtaskWithGateways =
                        Arrays.asList(
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.DEPLOYING,
                                        CompletionType.SUCCESSFULLY),
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.SUCCESSFULLY));

        CompletableFuture<JobVertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        subtaskWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        assertTrue(requestFuture.isDone());
        try {
            requestFuture.get();
            fail("Exception expected.");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    /** Tests that failed thread info request to one of the tasks fails the future. */
    @Test
    public void testThreadInfoRequestWithException() throws Exception {
        List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                subtaskWithGateways =
                        Arrays.asList(
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.DEPLOYING,
                                        CompletionType.SUCCESSFULLY),
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.EXCEPTIONALLY));

        CompletableFuture<JobVertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        subtaskWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        try {
            requestFuture.get();
            fail("Exception expected.");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
        }
    }

    /** Tests that thread info stats request times out if not finished in time. */
    @Test
    public void testThreadInfoRequestTimeout() throws Exception {
        List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                subtaskWithGateways =
                        Arrays.asList(
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.SUCCESSFULLY),
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.TIMEOUT));

        CompletableFuture<JobVertexThreadInfoStats> requestFuture =
                coordinator.triggerThreadInfoRequest(
                        subtaskWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        try {
            requestFuture.get();
            fail("Exception expected.");
        } catch (ExecutionException e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(e, REQUEST_TIMEOUT_MESSAGE)
                            .isPresent());
        } finally {
            coordinator.shutDown();
        }
    }

    /** Tests that shutdown fails all pending requests and future request triggers. */
    @Test
    public void testShutDown() throws Exception {
        List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                subtaskWithGateways =
                        Arrays.asList(
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.SUCCESSFULLY),
                                createMockSubtaskWithGateway(
                                        new ExecutionAttemptID(),
                                        ExecutionState.RUNNING,
                                        CompletionType.TIMEOUT));

        List<CompletableFuture<JobVertexThreadInfoStats>> requestFutures = new ArrayList<>();

        CompletableFuture<JobVertexThreadInfoStats> requestFuture1 =
                coordinator.triggerThreadInfoRequest(
                        subtaskWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        CompletableFuture<JobVertexThreadInfoStats> requestFuture2 =
                coordinator.triggerThreadInfoRequest(
                        subtaskWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        // trigger request
        requestFutures.add(requestFuture1);
        requestFutures.add(requestFuture2);

        for (CompletableFuture<JobVertexThreadInfoStats> future : requestFutures) {
            assertFalse(future.isDone());
        }

        // shut down
        coordinator.shutDown();

        // verify all completed
        for (CompletableFuture<JobVertexThreadInfoStats> future : requestFutures) {
            assertTrue(future.isCompletedExceptionally());
        }

        // verify new trigger returns failed future
        CompletableFuture<JobVertexThreadInfoStats> future =
                coordinator.triggerThreadInfoRequest(
                        subtaskWithGateways,
                        DEFAULT_NUMBER_OF_SAMPLES,
                        DEFAULT_DELAY_BETWEEN_SAMPLES,
                        DEFAULT_MAX_STACK_TRACE_DEPTH);

        assertTrue(future.isCompletedExceptionally());
    }

    private static CompletableFuture<TaskExecutorGateway> createMockTaskManagerGateway(
            ExecutionAttemptID executionAttemptId, CompletionType completionType) {

        final CompletableFuture<TaskThreadInfoResponse> responseFuture = new CompletableFuture<>();
        switch (completionType) {
            case SUCCESSFULLY:
                ThreadInfoSample sample =
                        JvmUtils.createThreadInfoSample(Thread.currentThread().getId(), 100).get();
                responseFuture.complete(
                        new TaskThreadInfoResponse(
                                0, executionAttemptId, Collections.singletonList(sample)));
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

        final TaskExecutorGateway executorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestThreadInfoSamplesSupplier(() -> responseFuture)
                        .createTestingTaskExecutorGateway();

        return CompletableFuture.completedFuture(executorGateway);
    }

    private Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>
            createMockSubtaskWithGateway(
                    ExecutionAttemptID executionAttemptId,
                    ExecutionState state,
                    CompletionType completionType) {
        final AccessExecutionVertex executionVertex =
                createMockExecutionVertex(executionAttemptId, state);
        final CompletableFuture<TaskExecutorGateway> executorGateway =
                createMockTaskManagerGateway(executionAttemptId, completionType);
        return Tuple2.of(executionVertex, executorGateway);
    }

    private AccessExecutionVertex createMockExecutionVertex(
            ExecutionAttemptID executionAttemptId, ExecutionState state) {
        final ArchivedExecution execution = createMockExecution(executionAttemptId, state);
        return new ArchivedExecutionVertex(0, "", execution, null);
    }

    private ArchivedExecution createMockExecution(
            ExecutionAttemptID executionAttemptId, ExecutionState state) {
        return new ArchivedExecution(
                null, null, executionAttemptId, 0, state, null, null, null, 0, null);
    }

    /** Completion types of the request future. */
    private enum CompletionType {
        SUCCESSFULLY,
        EXCEPTIONALLY,
        TIMEOUT,
        NEVER_COMPLETE
    }
}
