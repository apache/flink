/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory.ExceptionInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.UpperLimitExceptionParameter;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobExceptionsHandler}. */
class JobExceptionsHandlerTest {

    private final JobExceptionsHandler testInstance =
            new JobExceptionsHandler(
                    CompletableFuture::new,
                    TestingUtils.TIMEOUT,
                    Collections.emptyMap(),
                    JobExceptionsHeaders.getInstance(),
                    new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                    Executors.directExecutor(),
                    () -> CompletableFuture.completedFuture(new TestingResourceManagerGateway()));

    @Test
    void testNoExceptions() throws HandlerRequestException {
        final ExecutionGraphInfo executionGraphInfo =
                new ExecutionGraphInfo(new ArchivedExecutionGraphBuilder().build());

        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getRootException()).isNull();
        assertThat(response.getRootTimestamp()).isNull();
        assertThat(response.isTruncated()).isFalse();
        assertThat(response.getAllExceptions()).isEmpty();
        assertThat(response.getExceptionHistory().getEntries()).isEmpty();
    }

    @Test
    void testOnlyRootCause() throws HandlerRequestException {
        final Throwable rootCause = new RuntimeException("root cause");
        final long rootCauseTimestamp = System.currentTimeMillis();

        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(fromGlobalFailure(rootCause, rootCauseTimestamp));
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getRootException())
                .isEqualTo(ExceptionUtils.stringifyException(rootCause));
        assertThat(response.getRootTimestamp()).isEqualTo(rootCauseTimestamp);
        assertThat(response.isTruncated()).isFalse();
        assertThat(response.getAllExceptions()).isEmpty();

        assertThat(response.getExceptionHistory().getEntries())
                .anyMatch(
                        info ->
                                matchesSafely(
                                        info,
                                        rootCause,
                                        rootCauseTimestamp,
                                        null,
                                        null,
                                        null,
                                        false));
    }

    @Test
    void testOnlyExceptionHistory() throws HandlerRequestException {
        final RuntimeException rootThrowable = new RuntimeException("exception #0");
        final long rootTimestamp = System.currentTimeMillis();
        final RootExceptionHistoryEntry rootEntry = fromGlobalFailure(rootThrowable, rootTimestamp);
        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfoWithoutFailureCause(rootEntry);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getRootException()).isNull();
        assertThat(response.getRootTimestamp()).isNull();

        assertThat(response.getExceptionHistory().getEntries())
                .anyMatch(
                        info ->
                                matchesSafely(
                                        info,
                                        rootThrowable,
                                        rootTimestamp,
                                        null,
                                        null,
                                        null,
                                        false));
    }

    @Test
    void testWithExceptionHistory() throws HandlerRequestException {
        final RootExceptionHistoryEntry rootCause =
                fromGlobalFailure(new RuntimeException("exception #0"), System.currentTimeMillis());
        final RootExceptionHistoryEntry otherFailure =
                new RootExceptionHistoryEntry(
                        new RuntimeException("exception #1"),
                        System.currentTimeMillis(),
                        CompletableFuture.completedFuture(Collections.singletonMap("key", "value")),
                        "task name",
                        new LocalTaskManagerLocation(),
                        Collections.emptySet());

        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(rootCause, otherFailure);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getExceptionHistory().getEntries())
                .anyMatch(
                        info ->
                                matchesSafely(
                                        info,
                                        rootCause.getException(),
                                        rootCause.getTimestamp(),
                                        null,
                                        null,
                                        null,
                                        false));
        assertThat(response.getExceptionHistory().getEntries())
                .anyMatch(
                        info ->
                                matchesSafely(
                                        info,
                                        otherFailure.getException(),
                                        otherFailure.getTimestamp(),
                                        otherFailure.getFailingTaskName(),
                                        JobExceptionsHandler.toString(
                                                otherFailure.getTaskManagerLocation()),
                                        JobExceptionsHandler.toTaskManagerId(
                                                otherFailure.getTaskManagerLocation()),
                                        false));
        assertThat(response.getExceptionHistory().isTruncated()).isFalse();
    }

    @Test
    void testWithLocalExceptionHistoryEntryNotHavingATaskManagerInformationAvailable()
            throws HandlerRequestException {
        final RootExceptionHistoryEntry failure =
                new RootExceptionHistoryEntry(
                        new RuntimeException("exception #1"),
                        System.currentTimeMillis(),
                        CompletableFuture.completedFuture(Collections.singletonMap("key", "value")),
                        "task name",
                        null,
                        Collections.emptySet());

        final ExecutionGraphInfo executionGraphInfo = createExecutionGraphInfo(failure);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getExceptionHistory().getEntries())
                .anyMatch(
                        info ->
                                matchesSafely(
                                        info,
                                        failure.getException(),
                                        failure.getTimestamp(),
                                        failure.getFailingTaskName(),
                                        JobExceptionsHandler.toString(
                                                failure.getTaskManagerLocation()),
                                        JobExceptionsHandler.toTaskManagerId(
                                                failure.getTaskManagerLocation()),
                                        false));
    }

    @Test
    void testWithExceptionHistoryWithTruncationThroughParameter() throws HandlerRequestException {
        final RootExceptionHistoryEntry rootCause =
                fromGlobalFailure(new RuntimeException("exception #0"), System.currentTimeMillis());
        final RootExceptionHistoryEntry otherFailure =
                new RootExceptionHistoryEntry(
                        new RuntimeException("exception #1"),
                        System.currentTimeMillis(),
                        CompletableFuture.completedFuture(Collections.singletonMap("key", "value")),
                        "task name",
                        new LocalTaskManagerLocation(),
                        Collections.emptySet());

        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(rootCause, otherFailure);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 1);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getExceptionHistory().getEntries())
                .anyMatch(
                        info ->
                                matchesSafely(
                                        info,
                                        rootCause.getException(),
                                        rootCause.getTimestamp(),
                                        null,
                                        null,
                                        null,
                                        false));
        assertThat(response.getExceptionHistory().getEntries()).hasSize(1);
        assertThat(response.getExceptionHistory().isTruncated()).isTrue();
    }

    @Test
    void testTaskManagerLocationFallbackHandling() {
        assertThat(JobExceptionsHandler.toString((TaskManagerLocation) null))
                .isEqualTo("(unassigned)");
    }

    @Test
    void testTaskManagerLocationHandling() {
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        assertThat(JobExceptionsHandler.toString(taskManagerLocation))
                .isEqualTo(
                        String.format(
                                "%s:%s",
                                taskManagerLocation.getFQDNHostname(),
                                taskManagerLocation.dataPort()));
    }

    @Test
    void testArchivedTaskManagerLocationFallbackHandling() {
        assertThat(
                        JobExceptionsHandler.toString(
                                (ExceptionHistoryEntry.ArchivedTaskManagerLocation) null))
                .isNull();
    }

    @Test
    void testArchivedTaskManagerLocationHandling() {
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        assertThat(JobExceptionsHandler.toString(taskManagerLocation))
                .isEqualTo(
                        String.format(
                                "%s:%s",
                                taskManagerLocation.getFQDNHostname(),
                                taskManagerLocation.dataPort()));
    }

    @Test
    void testGetJobExceptionsInfo() throws HandlerRequestException {
        final int numExceptions = 20;
        final ExecutionGraphInfo archivedExecutionGraph = createAccessExecutionGraph(numExceptions);
        checkExceptionLimit(testInstance, archivedExecutionGraph, numExceptions, 10);
        checkExceptionLimit(testInstance, archivedExecutionGraph, numExceptions, numExceptions);
        checkExceptionLimit(testInstance, archivedExecutionGraph, numExceptions, 30);
    }

    private static void checkExceptionLimit(
            JobExceptionsHandler jobExceptionsHandler,
            ExecutionGraphInfo graph,
            int maxNumExceptions,
            int numExpectedException)
            throws HandlerRequestException {
        final HandlerRequest<EmptyRequestBody> handlerRequest =
                createRequest(graph.getJobId(), numExpectedException);
        final JobExceptionsInfo jobExceptionsInfo =
                jobExceptionsHandler.handleRequest(handlerRequest, graph);
        final int numReportedException = Math.min(maxNumExceptions, numExpectedException);
        assertThat(jobExceptionsInfo.getAllExceptions().size()).isEqualTo(numReportedException);
    }

    private static ExecutionGraphInfo createAccessExecutionGraph(int numTasks) {
        Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
        for (int i = 0; i < numTasks; i++) {
            final JobVertexID jobVertexId = new JobVertexID();
            tasks.put(jobVertexId, createArchivedExecutionJobVertex(jobVertexId));
        }

        final Throwable failureCause = new RuntimeException("root cause");
        final long failureTimestamp = System.currentTimeMillis();
        final List<RootExceptionHistoryEntry> exceptionHistory =
                Collections.singletonList(
                        new RootExceptionHistoryEntry(
                                failureCause,
                                failureTimestamp,
                                FailureEnricherUtils.EMPTY_FAILURE_LABELS,
                                "test task #1",
                                new LocalTaskManagerLocation(),
                                Collections.emptySet()));
        return new ExecutionGraphInfo(
                new ArchivedExecutionGraphBuilder()
                        .setFailureCause(new ErrorInfo(failureCause, failureTimestamp))
                        .setTasks(tasks)
                        .build(),
                exceptionHistory);
    }

    private static ArchivedExecutionJobVertex createArchivedExecutionJobVertex(
            JobVertexID jobVertexID) {
        final StringifiedAccumulatorResult[] emptyAccumulators =
                new StringifiedAccumulatorResult[0];
        final long[] timestamps = new long[ExecutionState.values().length];
        final long[] endTimestamps = new long[ExecutionState.values().length];
        final ExecutionState expectedState = ExecutionState.RUNNING;

        final LocalTaskManagerLocation assignedResourceLocation = new LocalTaskManagerLocation();
        final AllocationID allocationID = new AllocationID();

        final int subtaskIndex = 1;
        final int attempt = 2;
        return new ArchivedExecutionJobVertex(
                new ArchivedExecutionVertex[] {
                    new ArchivedExecutionVertex(
                            subtaskIndex,
                            "test task",
                            new ArchivedExecution(
                                    new StringifiedAccumulatorResult[0],
                                    null,
                                    createExecutionAttemptId(jobVertexID, subtaskIndex, attempt),
                                    expectedState,
                                    new ErrorInfo(
                                            new RuntimeException("error"),
                                            System.currentTimeMillis()),
                                    assignedResourceLocation,
                                    allocationID,
                                    timestamps,
                                    endTimestamps),
                            new ExecutionHistory(0))
                },
                jobVertexID,
                jobVertexID.toString(),
                1,
                1,
                ResourceProfile.UNKNOWN,
                emptyAccumulators);
    }

    // -------- exception history related utility methods for creating the input data --------

    private static ExecutionGraphInfo createExecutionGraphInfo(
            RootExceptionHistoryEntry... historyEntries) {
        return createExecutionGraphInfo(true, historyEntries);
    }

    private static ExecutionGraphInfo createExecutionGraphInfoWithoutFailureCause(
            RootExceptionHistoryEntry... historyEntries) {
        return createExecutionGraphInfo(false, historyEntries);
    }

    private static ExecutionGraphInfo createExecutionGraphInfo(
            boolean setFailureCause, RootExceptionHistoryEntry... historyEntries) {
        final ArchivedExecutionGraphBuilder executionGraphBuilder =
                new ArchivedExecutionGraphBuilder();
        final List<RootExceptionHistoryEntry> historyEntryCollection = new ArrayList<>();

        for (int i = 0; i < historyEntries.length; i++) {
            if (i == 0 && setFailureCause) {
                // first entry is root cause
                executionGraphBuilder.setFailureCause(
                        new ErrorInfo(
                                historyEntries[i].getException(),
                                historyEntries[i].getTimestamp()));
            }

            historyEntryCollection.add(historyEntries[i]);
        }

        // we have to reverse it to simulate how the Scheduler collects it
        Collections.reverse(historyEntryCollection);

        return new ExecutionGraphInfo(executionGraphBuilder.build(), historyEntryCollection);
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(JobID jobId, int size)
            throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());
        final Map<String, List<String>> queryParameters = new HashMap<>();
        queryParameters.put(UpperLimitExceptionParameter.KEY, Collections.singletonList("" + size));

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JobExceptionsMessageParameters(),
                pathParameters,
                queryParameters,
                Collections.emptyList());
    }

    private static RootExceptionHistoryEntry fromGlobalFailure(Throwable cause, long timestamp) {
        return new RootExceptionHistoryEntry(
                cause,
                timestamp,
                FailureEnricherUtils.EMPTY_FAILURE_LABELS,
                null,
                null,
                Collections.emptySet());
    }

    private boolean matchesSafely(
            ExceptionInfo info,
            Throwable expectedFailureCause,
            long expectedFailureTimestamp,
            String expectedTaskNameWithSubtaskId,
            String expectedTaskManagerLocation,
            String expectedTaskManagerId,
            boolean expectedIsTaskManagerAlive) {
        expectedFailureCause = deserializeSerializedThrowable(expectedFailureCause);
        return matches(
                        info,
                        ExceptionInfo::getExceptionName,
                        expectedFailureCause.getClass().getName())
                && matches(
                        info,
                        ExceptionInfo::getStacktrace,
                        ExceptionUtils.stringifyException(expectedFailureCause))
                && matches(info, ExceptionInfo::getTimestamp, expectedFailureTimestamp)
                && matches(info, ExceptionInfo::getTaskName, expectedTaskNameWithSubtaskId)
                && matches(info, ExceptionInfo::getLocation, expectedTaskManagerLocation)
                && matches(info, ExceptionInfo::getTaskManagerId, expectedTaskManagerId)
                && matches(info, ExceptionInfo::isTaskManagerAlive, expectedIsTaskManagerAlive);
    }

    private <R> boolean matches(
            ExceptionInfo info, Function<ExceptionInfo, R> extractor, R expectedValue) {
        final R actualValue = extractor.apply(info);
        if (actualValue == null) {
            return expectedValue == null;
        }

        return actualValue.equals(expectedValue);
    }

    /**
     * Utility method for unwrapping a {@link SerializedThrowable} again.
     *
     * @param throwable The {@code Throwable} that might be unwrapped.
     * @return The unwrapped {@code Throwable} if the {@code throwable} was actually a {@code
     *     SerializedThrowable}; otherwise the {@code throwable} itself.
     */
    protected Throwable deserializeSerializedThrowable(Throwable throwable) {
        return throwable instanceof SerializedThrowable
                ? ((SerializedThrowable) throwable)
                        .deserializeError(ClassLoader.getSystemClassLoader())
                : throwable;
    }
}
