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
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory.ExceptionInfo;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory.RootExceptionInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.FailureLabelFilterParameter;
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
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.collection.IsEmptyIterable;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Test for the {@link JobExceptionsHandler}. */
public class JobExceptionsHandlerTest extends TestLogger {

    private final JobExceptionsHandler testInstance =
            new JobExceptionsHandler(
                    CompletableFuture::new,
                    TestingUtils.TIMEOUT,
                    Collections.emptyMap(),
                    JobExceptionsHeaders.getInstance(),
                    new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                    Executors.directExecutor());

    @Test
    public void testNoExceptions() throws HandlerRequestException {
        final ExecutionGraphInfo executionGraphInfo =
                new ExecutionGraphInfo(new ArchivedExecutionGraphBuilder().build());

        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getRootException(), is(nullValue()));
        assertThat(response.getRootTimestamp(), is(nullValue()));
        assertFalse(response.isTruncated());
        assertThat(response.getAllExceptions(), empty());
        assertThat(response.getExceptionHistory().getEntries(), empty());
    }

    @Test
    public void testOnlyRootCause()
            throws HandlerRequestException, ExecutionException, InterruptedException {
        final Throwable rootCause = new RuntimeException("root cause");
        final long rootCauseTimestamp = System.currentTimeMillis();

        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(fromGlobalFailure(rootCause, rootCauseTimestamp));
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getRootException(), is(ExceptionUtils.stringifyException(rootCause)));
        assertThat(response.getRootTimestamp(), is(rootCauseTimestamp));
        assertFalse(response.isTruncated());
        assertThat(response.getAllExceptions(), empty());

        assertThat(
                response.getExceptionHistory().getEntries(),
                contains(historyContainsGlobalFailure(rootCause, rootCauseTimestamp)));
    }

    @Test
    public void testOnlyExceptionHistory()
            throws HandlerRequestException, ExecutionException, InterruptedException {
        final RuntimeException rootThrowable = new RuntimeException("exception #0");
        final long rootTimestamp = System.currentTimeMillis();
        final RootExceptionHistoryEntry rootEntry = fromGlobalFailure(rootThrowable, rootTimestamp);
        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfoWithoutFailureCause(rootEntry);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10);
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getRootException(), is(nullValue()));
        assertThat(response.getRootTimestamp(), is(nullValue()));

        assertThat(
                response.getExceptionHistory().getEntries(),
                contains(historyContainsGlobalFailure(rootThrowable, rootTimestamp)));
    }

    @Test
    public void testOnlyExceptionHistoryWithNoMatchingFailureLabel()
            throws HandlerRequestException {
        final RuntimeException rootThrowable = new RuntimeException("exception #0");
        final long rootTimestamp = System.currentTimeMillis();
        final RootExceptionHistoryEntry rootEntry = fromGlobalFailure(rootThrowable, rootTimestamp);
        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfoWithoutFailureCause(rootEntry);
        // rootEntry with EMPTY_FAILURE_LABELS so no match
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), 10, Arrays.asList("key:value"));
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getRootException(), is(nullValue()));
        assertThat(response.getRootTimestamp(), is(nullValue()));

        assertThat(response.getExceptionHistory().getEntries(), is(empty()));
    }

    @Test
    public void testWithExceptionHistory()
            throws HandlerRequestException, ExecutionException, InterruptedException {
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

        assertThat(
                response.getExceptionHistory().getEntries(),
                contains(
                        historyContainsGlobalFailure(
                                rootCause.getException(), rootCause.getTimestamp()),
                        historyContainsJobExceptionInfo(
                                otherFailure.getException(),
                                otherFailure.getTimestamp(),
                                otherFailure.getFailureLabelsFuture(),
                                otherFailure.getFailingTaskName(),
                                JobExceptionsHandler.toString(
                                        otherFailure.getTaskManagerLocation()),
                                JobExceptionsHandler.toTaskManagerId(
                                        otherFailure.getTaskManagerLocation()))));
        assertFalse(response.getExceptionHistory().isTruncated());
    }

    @Test
    public void testWithExceptionHistoryWithMatchingFailureLabel()
            throws HandlerRequestException, ExecutionException, InterruptedException {
        final RootExceptionHistoryEntry rootCause =
                fromGlobalFailure(new RuntimeException("exception #0"), System.currentTimeMillis());
        final RootExceptionHistoryEntry matchingFailure =
                new RootExceptionHistoryEntry(
                        new RuntimeException("exception #1"),
                        System.currentTimeMillis(),
                        CompletableFuture.completedFuture(
                                new HashMap<String, String>() {
                                    {
                                        put("key1", "value1");
                                        put("key3", "value3");
                                    }
                                }),
                        "task name",
                        new LocalTaskManagerLocation(),
                        Collections.emptySet());
        final RootExceptionHistoryEntry noMatchingFailure =
                new RootExceptionHistoryEntry(
                        new RuntimeException("exception #2"),
                        System.currentTimeMillis(),
                        CompletableFuture.completedFuture(
                                Collections.singletonMap("key2", "value2")),
                        "task name",
                        new LocalTaskManagerLocation(),
                        Collections.emptySet());
        final RootExceptionHistoryEntry anotherMatchingFailure =
                new RootExceptionHistoryEntry(
                        new RuntimeException("exception #3"),
                        System.currentTimeMillis(),
                        CompletableFuture.completedFuture(
                                new HashMap<String, String>() {
                                    {
                                        put("key1", "value1");
                                        put("key3", "value2");
                                    }
                                }),
                        "task name",
                        new LocalTaskManagerLocation(),
                        Collections.emptySet());

        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(
                        rootCause, matchingFailure, noMatchingFailure, anotherMatchingFailure);
        // Two matching historyEntries
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(
                        executionGraphInfo.getJobId(),
                        10,
                        Arrays.asList("key1:value1", "key3:value3"));
        final JobExceptionsInfoWithHistory response =
                testInstance.handleRequest(request, executionGraphInfo);

        assertThat(response.getExceptionHistory().getEntries().size(), is(1));
        assertThat(
                response.getExceptionHistory().getEntries(),
                contains(
                        historyContainsJobExceptionInfo(
                                matchingFailure.getException(),
                                matchingFailure.getTimestamp(),
                                matchingFailure.getFailureLabelsFuture(),
                                matchingFailure.getFailingTaskName(),
                                JobExceptionsHandler.toString(
                                        matchingFailure.getTaskManagerLocation()),
                                JobExceptionsHandler.toTaskManagerId(
                                        matchingFailure.getTaskManagerLocation()))));
        assertFalse(response.getExceptionHistory().isTruncated());
    }

    @Test
    public void testWithLocalExceptionHistoryEntryNotHavingATaskManagerInformationAvailable()
            throws HandlerRequestException, ExecutionException, InterruptedException {
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

        assertThat(
                response.getExceptionHistory().getEntries(),
                contains(
                        historyContainsJobExceptionInfo(
                                failure.getException(),
                                failure.getTimestamp(),
                                failure.getFailureLabelsFuture(),
                                failure.getFailingTaskName(),
                                JobExceptionsHandler.toString(failure.getTaskManagerLocation()),
                                JobExceptionsHandler.toTaskManagerId(
                                        failure.getTaskManagerLocation()))));
    }

    @Test
    public void testWithExceptionHistoryWithTruncationThroughParameter()
            throws HandlerRequestException, ExecutionException, InterruptedException {
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

        assertThat(
                response.getExceptionHistory().getEntries(),
                contains(
                        historyContainsGlobalFailure(
                                rootCause.getException(), rootCause.getTimestamp())));
        assertThat(response.getExceptionHistory().getEntries(), iterableWithSize(1));
        assertTrue(response.getExceptionHistory().isTruncated());
    }

    @Test
    public void testTaskManagerLocationFallbackHandling() {
        assertThat(JobExceptionsHandler.toString((TaskManagerLocation) null), is("(unassigned)"));
    }

    @Test
    public void testTaskManagerLocationHandling() {
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        assertThat(
                JobExceptionsHandler.toString(taskManagerLocation),
                is(
                        String.format(
                                "%s:%s",
                                taskManagerLocation.getFQDNHostname(),
                                taskManagerLocation.dataPort())));
    }

    @Test
    public void testArchivedTaskManagerLocationFallbackHandling() {
        assertThat(
                JobExceptionsHandler.toString(
                        (ExceptionHistoryEntry.ArchivedTaskManagerLocation) null),
                is(nullValue()));
    }

    @Test
    public void testArchivedTaskManagerLocationHandling() {
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        assertThat(
                JobExceptionsHandler.toString(taskManagerLocation),
                is(
                        String.format(
                                "%s:%s",
                                taskManagerLocation.getFQDNHostname(),
                                taskManagerLocation.dataPort())));
    }

    @Test
    public void testGetJobExceptionsInfo() throws HandlerRequestException {
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
        assertEquals(jobExceptionsInfo.getAllExceptions().size(), numReportedException);
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
        return createRequest(jobId, size, Collections.emptyList());
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(
            JobID jobId, int size, List<String> failureLabels) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());
        final Map<String, List<String>> queryParameters = new HashMap<>();
        queryParameters.put(UpperLimitExceptionParameter.KEY, Collections.singletonList("" + size));
        if (!failureLabels.isEmpty()) {
            queryParameters.put(FailureLabelFilterParameter.KEY, failureLabels);
        }

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

    // -------- factory methods for instantiating new Matchers --------

    @SafeVarargs
    private static Matcher<RootExceptionInfo> historyContainsJobExceptionInfo(
            Throwable expectedFailureCause,
            long expectedFailureTimestamp,
            CompletableFuture<Map<String, String>> expectedFailureLabels,
            String expectedTaskNameWithSubtaskId,
            String expectedTaskManagerLocation,
            String expectedTaskManagerId,
            Matcher<ExceptionInfo>... concurrentExceptionMatchers)
            throws ExecutionException, InterruptedException {
        return new RootExceptionInfoMatcher(
                matchesFailure(
                        expectedFailureCause,
                        expectedFailureTimestamp,
                        expectedFailureLabels,
                        expectedTaskNameWithSubtaskId,
                        expectedTaskManagerLocation,
                        expectedTaskManagerId),
                concurrentExceptionMatchers);
    }

    @SafeVarargs
    private static Matcher<RootExceptionInfo> historyContainsGlobalFailure(
            Throwable expectedFailureCause,
            long expectedFailureTimestamp,
            Matcher<ExceptionInfo>... concurrentExceptionMatchers)
            throws ExecutionException, InterruptedException {
        return new RootExceptionInfoMatcher(
                matchesFailure(
                        expectedFailureCause,
                        expectedFailureTimestamp,
                        FailureEnricherUtils.EMPTY_FAILURE_LABELS,
                        null,
                        null,
                        null),
                concurrentExceptionMatchers);
    }

    private static Matcher<ExceptionInfo> matchesFailure(
            Throwable expectedFailureCause,
            long expectedFailureTimestamp,
            CompletableFuture<Map<String, String>> expectedFailureLabels,
            String expectedTaskNameWithSubtaskId,
            String expectedTaskManagerLocation,
            String expectedTaskManagerId)
            throws ExecutionException, InterruptedException {
        return new ExceptionInfoMatcher(
                expectedFailureCause,
                expectedFailureTimestamp,
                expectedFailureLabels,
                expectedTaskNameWithSubtaskId,
                expectedTaskManagerLocation,
                expectedTaskManagerId);
    }

    // -------- Matcher implementations used in this test class --------

    /** Checks the given {@link RootExceptionInfo} instance. */
    private static class RootExceptionInfoMatcher
            extends TypeSafeDiagnosingMatcher<RootExceptionInfo> {

        private final Matcher<ExceptionInfo> rootCauseMatcher;
        private final Matcher<Iterable<? extends ExceptionInfo>> concurrentExceptionsMatcher;

        @SafeVarargs
        private RootExceptionInfoMatcher(
                Matcher<ExceptionInfo> rootCauseMatcher,
                Matcher<ExceptionInfo>... concurrentExceptionsMatchers) {
            this.rootCauseMatcher = rootCauseMatcher;
            this.concurrentExceptionsMatcher =
                    concurrentExceptionsMatchers.length == 0
                            ? IsEmptyIterable.emptyIterable()
                            : IsIterableContainingInOrder.contains(
                                    Arrays.asList(concurrentExceptionsMatchers));
        }

        @Override
        protected boolean matchesSafely(
                RootExceptionInfo rootExceptionInfo, Description description) {
            boolean match = true;
            if (!rootCauseMatcher.matches(rootExceptionInfo)) {
                rootCauseMatcher.describeMismatch(rootExceptionInfo, description);
                match = false;
            }

            if (!concurrentExceptionsMatcher.matches(rootExceptionInfo.getConcurrentExceptions())) {
                concurrentExceptionsMatcher.describeMismatch(
                        rootExceptionInfo.getConcurrentExceptions(), description);
                return false;
            }

            return match;
        }

        @Override
        public void describeTo(Description description) {
            rootCauseMatcher.describeTo(description);
            concurrentExceptionsMatcher.describeTo(description);
        }
    }

    /** Checks the given {@link ExceptionInfo} instance. */
    private static class ExceptionInfoMatcher extends TypeSafeDiagnosingMatcher<ExceptionInfo> {

        private final Throwable expectedException;
        private final long expectedTimestamp;
        private final Map<String, String> expectedFailureLabels;
        private final String expectedTaskName;
        private final String expectedLocation;
        private final String expectedTaskManagerId;

        private ExceptionInfoMatcher(
                Throwable expectedException,
                long expectedTimestamp,
                CompletableFuture<Map<String, String>> expectedFailureLabels,
                String expectedTaskName,
                String expectedLocation,
                String expectedTaskManagerId)
                throws ExecutionException, InterruptedException {
            this.expectedException = deserializeSerializedThrowable(expectedException);
            this.expectedTimestamp = expectedTimestamp;
            this.expectedFailureLabels = expectedFailureLabels.get();
            this.expectedTaskName = expectedTaskName;
            this.expectedLocation = expectedLocation;
            this.expectedTaskManagerId = expectedTaskManagerId;
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("exceptionName=")
                    .appendText(getExpectedExceptionName())
                    .appendText(", exceptionStacktrace=")
                    .appendText(getExpectedStacktrace())
                    .appendText(", timestamp=")
                    .appendText(String.valueOf(expectedTimestamp))
                    .appendText(", failureLabels=")
                    .appendText(String.valueOf(expectedFailureLabels))
                    .appendText(", taskName=")
                    .appendText(expectedTaskName)
                    .appendText(", location=")
                    .appendText(expectedLocation)
                    .appendText(", taskManagerId=")
                    .appendText(expectedTaskManagerId);
        }

        private String getExpectedExceptionName() {
            return expectedException.getClass().getName();
        }

        private String getExpectedStacktrace() {
            return ExceptionUtils.stringifyException(expectedException);
        }

        @Override
        protected boolean matchesSafely(ExceptionInfo info, Description description) {
            return matches(
                            info,
                            description,
                            ExceptionInfo::getExceptionName,
                            getExpectedExceptionName(),
                            "exceptionName")
                    && matches(
                            info,
                            description,
                            ExceptionInfo::getStacktrace,
                            ExceptionUtils.stringifyException(expectedException),
                            "stacktrace")
                    && matches(
                            info,
                            description,
                            ExceptionInfo::getTimestamp,
                            expectedTimestamp,
                            "timestamp")
                    && matches(
                            info,
                            description,
                            ExceptionInfo::getTaskName,
                            expectedTaskName,
                            "taskName")
                    && matches(
                            info,
                            description,
                            ExceptionInfo::getLocation,
                            expectedLocation,
                            "location")
                    && matches(
                            info,
                            description,
                            ExceptionInfo::getTaskManagerId,
                            expectedTaskManagerId,
                            "taskManagerId");
        }

        private <R> boolean matches(
                ExceptionInfo info,
                Description desc,
                Function<ExceptionInfo, R> extractor,
                R expectedValue,
                String attributeName) {
            final R actualValue = extractor.apply(info);
            if (actualValue == null) {
                return expectedValue == null;
            }

            final boolean match = actualValue.equals(expectedValue);
            if (!match) {
                desc.appendText(attributeName)
                        .appendText("=")
                        .appendText(String.valueOf(actualValue));
            }

            return match;
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
}
