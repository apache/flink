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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionConfigBuilder;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobDetailsHandler}. */
class JobDetailsHandlerTest {
    private JobDetailsHandler jobDetailsHandler;
    private HandlerRequest<EmptyRequestBody> handlerRequest;
    private AccessExecutionGraph archivedExecutionGraph;
    private final String expectedStreamGraphJson =
            "{\"pending_operators:2,\":\"nodes\":[{\"id\":\"1\",\"parallelism\":1,\"operator\":\"Source: Sequence Source\",\"description\":\"Source: Sequence Source\",\"inputs\":[]},{\"id\":\"2\",\"parallelism\":1,\"operator\":\"Sink: Print to Std. Out\",\"description\":\"Sink: Print to Std. Out\",\"inputs\":[{\"num\":0,\"id\":\"1\",\"ship_strategy\":\"FORWARD\",\"exchange\":\"UNDEFINED\"}]}]}";

    private static HandlerRequest<EmptyRequestBody> createRequest(JobID jobId)
            throws HandlerRequestException {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new TaskManagerMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @BeforeEach
    void setUp() throws HandlerRequestException {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);
        final RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(new Configuration());
        final MetricFetcher metricFetcher =
                new MetricFetcherImpl<>(
                        () -> null,
                        address -> null,
                        Executors.directExecutor(),
                        Duration.ofMillis(1000L),
                        MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue().toMillis());
        final ArchivedExecutionConfig archivedExecutionConfig =
                new ArchivedExecutionConfigBuilder().build();

        archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setArchivedExecutionConfig(archivedExecutionConfig)
                        .setStreamGraphJson(expectedStreamGraphJson)
                        .build();
        jobDetailsHandler =
                new JobDetailsHandler(
                        leaderRetriever,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobDetailsHeaders.getInstance(),
                        new DefaultExecutionGraphCache(
                                restHandlerConfiguration.getTimeout(),
                                Duration.ofMillis(restHandlerConfiguration.getRefreshInterval())),
                        Executors.directExecutor(),
                        metricFetcher);
        handlerRequest = createRequest(archivedExecutionGraph.getJobID());
    }

    @Test
    void testGetJobDetailsWithStreamGraphJson() throws RestHandlerException {
        JobDetailsInfo jobDetailsInfo =
                jobDetailsHandler.handleRequest(
                        handlerRequest,
                        new ExecutionGraphInfo((ArchivedExecutionGraph) archivedExecutionGraph));
        assertThat(jobDetailsInfo.getStreamGraphJson())
                .isEqualTo(new JobPlanInfo.RawJson(expectedStreamGraphJson).toString());
    }

    /**
     * Verifies that {@link JobDetailsHandler} aggregates the per-downstream-target {@code
     * numRecordsOut} map across subtasks of the same job vertex. Uses the terminal (archived) path,
     * which consumes {@link IOMetrics#getNumRecordsOutPerTarget()} directly without the {@link
     * MetricFetcher}.
     */
    @Test
    void testJobDetailsAggregatesPerTargetWriteRecords() throws Exception {
        final String targetA = "abcdef0123456789abcdef0123456789";
        final String targetB = "0123456789abcdef0123456789abcdef";

        final Map<String, Long> subtask0PerTarget = new HashMap<>();
        subtask0PerTarget.put(targetA, 3L);
        subtask0PerTarget.put(targetB, 1L);
        final IOMetrics ioMetrics0 =
                new IOMetrics(0L, 0L, 0L, 4L, 0L, 0.0, 0L, null, subtask0PerTarget);

        final Map<String, Long> subtask1PerTarget = new HashMap<>();
        subtask1PerTarget.put(targetA, 2L);
        subtask1PerTarget.put(targetB, 4L);
        final IOMetrics ioMetrics1 =
                new IOMetrics(0L, 0L, 0L, 6L, 0L, 0.0, 0L, null, subtask1PerTarget);

        final JobVertexID jobVertexId = new JobVertexID();
        final ArchivedExecutionJobVertex archivedJobVertex =
                buildJobVertexWithSubtaskIOMetrics(jobVertexId, ioMetrics0, ioMetrics1);

        final AccessExecutionGraph graphWithVertex =
                buildGraphWithSingleVertex(archivedJobVertex, jobVertexId);

        final HandlerRequest<EmptyRequestBody> request = createRequest(graphWithVertex.getJobID());
        final JobDetailsInfo jobDetailsInfo =
                jobDetailsHandler.handleRequest(
                        request, new ExecutionGraphInfo((ArchivedExecutionGraph) graphWithVertex));

        final JobDetailsInfo.JobVertexDetailsInfo vertexInfo =
                jobDetailsInfo.getJobVertexInfos().iterator().next();
        final IOMetricsInfo metrics = vertexInfo.getJobVertexMetrics();

        assertThat(metrics.getRecordsWritten()).isEqualTo(10L);
        assertThat(metrics.getRecordsWrittenPerTarget())
                .containsEntry(targetA, 5L)
                .containsEntry(targetB, 5L)
                .hasSize(2);
    }

    /**
     * Verifies that a job vertex whose subtasks report no per-target breakdown (legacy {@link
     * IOMetrics} constructors) surfaces an empty {@code write-records-per-target} map rather than
     * {@code null}.
     */
    @Test
    void testJobDetailsBackCompatNoPerTarget() throws Exception {
        final IOMetrics legacy0 = new IOMetrics(0L, 0L, 0L, 5L, 0L, 0.0, 0L);
        final IOMetrics legacy1 = new IOMetrics(0L, 0L, 0L, 7L, 0L, 0.0, 0L);

        final JobVertexID jobVertexId = new JobVertexID();
        final ArchivedExecutionJobVertex archivedJobVertex =
                buildJobVertexWithSubtaskIOMetrics(jobVertexId, legacy0, legacy1);

        final AccessExecutionGraph graphWithVertex =
                buildGraphWithSingleVertex(archivedJobVertex, jobVertexId);

        final HandlerRequest<EmptyRequestBody> request = createRequest(graphWithVertex.getJobID());
        final JobDetailsInfo jobDetailsInfo =
                jobDetailsHandler.handleRequest(
                        request, new ExecutionGraphInfo((ArchivedExecutionGraph) graphWithVertex));

        final IOMetricsInfo metrics =
                jobDetailsInfo.getJobVertexInfos().iterator().next().getJobVertexMetrics();

        assertThat(metrics.getRecordsWritten()).isEqualTo(12L);
        assertThat(metrics.getRecordsWrittenPerTarget()).isNotNull().isEmpty();
    }

    private static ArchivedExecutionJobVertex buildJobVertexWithSubtaskIOMetrics(
            JobVertexID jobVertexId, IOMetrics ioMetrics0, IOMetrics ioMetrics1) {
        final StringifiedAccumulatorResult[] emptyAccumulators =
                new StringifiedAccumulatorResult[0];
        final ArchivedExecutionVertex subtask0 =
                new ArchivedExecutionVertex(
                        0,
                        "subtask-0",
                        new ArchivedExecution(
                                emptyAccumulators,
                                ioMetrics0,
                                createExecutionAttemptId(jobVertexId, 0, 0),
                                ExecutionState.FINISHED,
                                null,
                                null,
                                null,
                                new long[ExecutionState.values().length],
                                new long[ExecutionState.values().length]),
                        new ExecutionHistory(0));
        final ArchivedExecutionVertex subtask1 =
                new ArchivedExecutionVertex(
                        1,
                        "subtask-1",
                        new ArchivedExecution(
                                emptyAccumulators,
                                ioMetrics1,
                                createExecutionAttemptId(jobVertexId, 1, 0),
                                ExecutionState.FINISHED,
                                null,
                                null,
                                null,
                                new long[ExecutionState.values().length],
                                new long[ExecutionState.values().length]),
                        new ExecutionHistory(0));
        return new ArchivedExecutionJobVertex(
                new ArchivedExecutionVertex[] {subtask0, subtask1},
                jobVertexId,
                "test-vertex",
                2,
                2,
                new SlotSharingGroup(),
                ResourceProfile.UNKNOWN,
                emptyAccumulators);
    }

    private AccessExecutionGraph buildGraphWithSingleVertex(
            ArchivedExecutionJobVertex archivedJobVertex, JobVertexID jobVertexId) {
        final Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
        tasks.put(jobVertexId, archivedJobVertex);
        final ArchivedExecutionConfig archivedExecutionConfig =
                new ArchivedExecutionConfigBuilder().build();
        return new ArchivedExecutionGraphBuilder()
                .setArchivedExecutionConfig(archivedExecutionConfig)
                .setTasks(tasks)
                .build();
    }
}
