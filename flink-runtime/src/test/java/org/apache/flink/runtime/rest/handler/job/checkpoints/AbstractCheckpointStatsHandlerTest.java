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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.StatsSummaryDto;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/** Test class for {@link AbstractCheckpointStatsHandler}. */
class AbstractCheckpointStatsHandlerTest {

    private static final Time TIMEOUT = Time.seconds(10);

    private static final JobID JOB_ID = new JobID();

    private static final CheckpointStatsTracker checkpointStatsTracker =
            new CheckpointStatsTracker(
                    10, UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup());

    @Test
    void testRetrieveSnapshotFromCache() throws Exception {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);
        CheckpointingStatistics checkpointingStatistics = getTestCheckpointingStatistics();
        CheckpointStatsSnapshot checkpointStatsSnapshot1 = getTestCheckpointStatsSnapshot();

        // Create a passthrough cache so the latest object will always be returned
        Cache<JobID, CompletableFuture<CheckpointStatsSnapshot>> cache =
                CacheBuilder.newBuilder().build();

        try (RecordingCheckpointStatsHandler checkpointStatsHandler =
                new RecordingCheckpointStatsHandler(
                        leaderRetriever,
                        TIMEOUT,
                        Collections.emptyMap(),
                        CheckpointingStatisticsHeaders.getInstance(),
                        cache,
                        Executors.directExecutor(),
                        checkpointingStatistics)) {
            RestfulGateway functioningRestfulGateway =
                    new TestingRestfulGateway.Builder()
                            .setRequestCheckpointStatsSnapshotFunction(
                                    jobID ->
                                            CompletableFuture.completedFuture(
                                                    checkpointStatsSnapshot1))
                            .build();
            HandlerRequest<EmptyRequestBody> request =
                    HandlerRequest.resolveParametersAndCreate(
                            EmptyRequestBody.getInstance(),
                            new JobMessageParameters(),
                            Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
                            Collections.emptyMap(),
                            Collections.emptyList());

            assertThat(
                            checkpointStatsHandler
                                    .handleRequest(request, functioningRestfulGateway)
                                    .get())
                    .usingRecursiveComparison()
                    .isEqualTo(checkpointingStatistics);
            assertThat(checkpointStatsHandler.getStoredCheckpointStats())
                    .isEqualTo(checkpointStatsSnapshot1);

            // Refresh the checkpoint stats data
            CheckpointStatsSnapshot checkpointStatsSnapshot2 = getTestCheckpointStatsSnapshot();
            RestfulGateway refreshedRestfulGateway =
                    new TestingRestfulGateway.Builder()
                            .setRequestCheckpointStatsSnapshotFunction(
                                    jobID ->
                                            CompletableFuture.completedFuture(
                                                    checkpointStatsSnapshot2))
                            .build();
            assertThat(checkpointStatsHandler.handleRequest(request, refreshedRestfulGateway).get())
                    .usingRecursiveComparison()
                    .isEqualTo(checkpointingStatistics);
            assertThat(checkpointStatsHandler.getStoredCheckpointStats())
                    .isEqualTo(checkpointStatsSnapshot2);
        }
    }

    @Test
    void testRestExceptionPassedThrough() throws Exception {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);
        CheckpointStatsSnapshot checkpointStatsSnapshot1 = getTestCheckpointStatsSnapshot();
        RestHandlerException restHandlerException =
                new RestHandlerException(
                        "some exception thrown", HttpResponseStatus.INTERNAL_SERVER_ERROR);

        try (ThrowingCheckpointStatsHandler checkpointStatsHandler =
                new ThrowingCheckpointStatsHandler(
                        leaderRetriever,
                        TIMEOUT,
                        Collections.emptyMap(),
                        CheckpointingStatisticsHeaders.getInstance(),
                        CacheBuilder.newBuilder().build(),
                        Executors.directExecutor(),
                        restHandlerException)) {
            RestfulGateway restfulGateway =
                    new TestingRestfulGateway.Builder()
                            .setRequestCheckpointStatsSnapshotFunction(
                                    jobID ->
                                            CompletableFuture.completedFuture(
                                                    checkpointStatsSnapshot1))
                            .build();
            HandlerRequest<EmptyRequestBody> request =
                    HandlerRequest.resolveParametersAndCreate(
                            EmptyRequestBody.getInstance(),
                            new JobMessageParameters(),
                            Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
                            Collections.emptyMap(),
                            Collections.emptyList());

            assertThatExceptionOfType(ExecutionException.class)
                    .isThrownBy(
                            () ->
                                    checkpointStatsHandler
                                            .handleRequest(request, restfulGateway)
                                            .get())
                    .withCause(restHandlerException);
        }
    }

    @Test
    void testFlinkJobNotFoundException() throws Exception {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);
        CheckpointStatsSnapshot checkpointStatsSnapshot1 = getTestCheckpointStatsSnapshot();
        CompletableFuture<CheckpointStatsSnapshot> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new FlinkJobNotFoundException(JOB_ID));

        try (RecordingCheckpointStatsHandler checkpointStatsHandler =
                new RecordingCheckpointStatsHandler(
                        leaderRetriever,
                        TIMEOUT,
                        Collections.emptyMap(),
                        CheckpointingStatisticsHeaders.getInstance(),
                        CacheBuilder.newBuilder().build(),
                        Executors.directExecutor(),
                        null)) {
            RestfulGateway restfulGateway =
                    new TestingRestfulGateway.Builder()
                            .setRequestCheckpointStatsSnapshotFunction(jobID -> failedFuture)
                            .build();
            HandlerRequest<EmptyRequestBody> request =
                    HandlerRequest.resolveParametersAndCreate(
                            EmptyRequestBody.getInstance(),
                            new JobMessageParameters(),
                            Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
                            Collections.emptyMap(),
                            Collections.emptyList());

            assertThat(checkpointStatsHandler.handleRequest(request, restfulGateway))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableOfType(ExecutionException.class)
                    .withStackTraceContaining("Job %s not found", JOB_ID);
        }
    }

    private static CheckpointStatsSnapshot getTestCheckpointStatsSnapshot() {
        return checkpointStatsTracker.createSnapshot();
    }

    private CheckpointingStatistics getTestCheckpointingStatistics() {
        final CheckpointingStatistics.Counts counts =
                new CheckpointingStatistics.Counts(1, 2, 3, 4, 5);
        final CheckpointingStatistics.Summary summary =
                new CheckpointingStatistics.Summary(
                        new StatsSummaryDto(1L, 1L, 1L, 0, 0, 0, 0, 0),
                        new StatsSummaryDto(1L, 1L, 1L, 0, 0, 0, 0, 0),
                        new StatsSummaryDto(2L, 2L, 2L, 0, 0, 0, 0, 0),
                        new StatsSummaryDto(3L, 3L, 3L, 0, 0, 0, 0, 0),
                        new StatsSummaryDto(4L, 4L, 4L, 0, 0, 0, 0, 0),
                        new StatsSummaryDto(5L, 5L, 5L, 0, 0, 0, 0, 0));
        return new CheckpointingStatistics(
                counts,
                summary,
                new CheckpointingStatistics.LatestCheckpoints(null, null, null, null),
                Collections.emptyList());
    }

    private static class RecordingCheckpointStatsHandler
            extends AbstractCheckpointStatsHandler<CheckpointingStatistics, JobMessageParameters> {

        private final CheckpointingStatistics returnValue;
        private CheckpointStatsSnapshot storedCheckpointStats;

        protected RecordingCheckpointStatsHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<EmptyRequestBody, CheckpointingStatistics, JobMessageParameters>
                        messageHeaders,
                Cache<JobID, CompletableFuture<CheckpointStatsSnapshot>>
                        checkpointStatsSnapshotCache,
                Executor executor,
                CheckpointingStatistics returnValue) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    messageHeaders,
                    checkpointStatsSnapshotCache,
                    executor);
            this.returnValue = returnValue;
        }

        @Override
        protected CheckpointingStatistics handleCheckpointStatsRequest(
                HandlerRequest<EmptyRequestBody> request,
                CheckpointStatsSnapshot checkpointStatsSnapshot)
                throws RestHandlerException {
            storedCheckpointStats = checkpointStatsSnapshot;
            return returnValue;
        }

        public CheckpointStatsSnapshot getStoredCheckpointStats() {
            return storedCheckpointStats;
        }
    }

    private static class ThrowingCheckpointStatsHandler
            extends AbstractCheckpointStatsHandler<CheckpointingStatistics, JobMessageParameters> {

        private final RestHandlerException exception;

        protected ThrowingCheckpointStatsHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<EmptyRequestBody, CheckpointingStatistics, JobMessageParameters>
                        messageHeaders,
                Cache<JobID, CompletableFuture<CheckpointStatsSnapshot>>
                        checkpointStatsSnapshotCache,
                Executor executor,
                RestHandlerException exception) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    messageHeaders,
                    checkpointStatsSnapshotCache,
                    executor);
            this.exception = exception;
        }

        @Override
        protected CheckpointingStatistics handleCheckpointStatsRequest(
                HandlerRequest<EmptyRequestBody> request,
                CheckpointStatsSnapshot checkpointStatsSnapshot)
                throws RestHandlerException {
            throw exception;
        }
    }
}
