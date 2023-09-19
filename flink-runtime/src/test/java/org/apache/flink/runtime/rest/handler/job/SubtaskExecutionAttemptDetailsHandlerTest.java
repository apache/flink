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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptMessageParameters;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link SubtaskExecutionAttemptDetailsHandler}. */
class SubtaskExecutionAttemptDetailsHandlerTest {

    @Test
    void testHandleRequest() throws Exception {

        final JobID jobID = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();

        // The testing subtask.
        final int subtaskIndex = 1;
        final ExecutionState expectedState = ExecutionState.FINISHED;
        final int attempt = 0;

        final StringifiedAccumulatorResult[] emptyAccumulators =
                new StringifiedAccumulatorResult[0];

        final long bytesIn = 1L;
        final long bytesOut = 10L;
        final long recordsIn = 20L;
        final long recordsOut = 30L;
        final long accumulateIdleTime = 40L;
        final long accumulateBusyTime = 50L;
        final long accumulateBackPressuredTime = 60L;

        final IOMetrics ioMetrics =
                new IOMetrics(
                        bytesIn,
                        bytesOut,
                        recordsIn,
                        recordsOut,
                        accumulateIdleTime,
                        accumulateBusyTime,
                        accumulateBackPressuredTime);

        final ArchivedExecutionJobVertex archivedExecutionJobVertex =
                new ArchivedExecutionJobVertex(
                        new ArchivedExecutionVertex[] {
                            null, // the first subtask won't be queried
                            new ArchivedExecutionVertex(
                                    subtaskIndex,
                                    "test task",
                                    new ArchivedExecution(
                                            emptyAccumulators,
                                            ioMetrics,
                                            createExecutionAttemptId(
                                                    jobVertexId, subtaskIndex, attempt),
                                            expectedState,
                                            null,
                                            null,
                                            null,
                                            new long[ExecutionState.values().length],
                                            new long[ExecutionState.values().length]),
                                    new ExecutionHistory(0))
                        },
                        jobVertexId,
                        "test",
                        1,
                        1,
                        ResourceProfile.UNKNOWN,
                        emptyAccumulators);

        // Change some fields so we can make it different from other sub tasks.
        final MetricFetcher metricFetcher =
                new MetricFetcherImpl<>(
                        () -> null,
                        address -> null,
                        Executors.directExecutor(),
                        Time.milliseconds(1000L),
                        MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue());

        // Instance the handler.
        final RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(new Configuration());

        final SubtaskExecutionAttemptDetailsHandler handler =
                new SubtaskExecutionAttemptDetailsHandler(
                        () -> null,
                        Time.milliseconds(100L),
                        Collections.emptyMap(),
                        SubtaskExecutionAttemptDetailsHeaders.getInstance(),
                        new DefaultExecutionGraphCache(
                                restHandlerConfiguration.getTimeout(),
                                Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
                        Executors.directExecutor(),
                        metricFetcher);

        final HashMap<String, String> receivedPathParameters = new HashMap<>(4);
        receivedPathParameters.put(JobIDPathParameter.KEY, jobID.toString());
        receivedPathParameters.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        receivedPathParameters.put(SubtaskIndexPathParameter.KEY, Integer.toString(subtaskIndex));
        receivedPathParameters.put(SubtaskAttemptPathParameter.KEY, Integer.toString(attempt));

        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        new SubtaskAttemptMessageParameters(),
                        receivedPathParameters,
                        Collections.emptyMap(),
                        Collections.emptyList());

        // Handle request.
        final SubtaskExecutionAttemptDetailsInfo detailsInfo =
                handler.handleRequest(request, archivedExecutionJobVertex);

        // Verify
        final IOMetricsInfo ioMetricsInfo =
                new IOMetricsInfo(
                        bytesIn,
                        true,
                        bytesOut,
                        true,
                        recordsIn,
                        true,
                        recordsOut,
                        true,
                        accumulateBackPressuredTime,
                        accumulateIdleTime,
                        accumulateBusyTime);

        final Map<ExecutionState, Long> statusDuration = new HashMap<>();
        statusDuration.put(ExecutionState.CREATED, -1L);
        statusDuration.put(ExecutionState.SCHEDULED, -1L);
        statusDuration.put(ExecutionState.DEPLOYING, -1L);
        statusDuration.put(ExecutionState.INITIALIZING, -1L);
        statusDuration.put(ExecutionState.RUNNING, -1L);

        final SubtaskExecutionAttemptDetailsInfo expectedDetailsInfo =
                new SubtaskExecutionAttemptDetailsInfo(
                        subtaskIndex,
                        expectedState,
                        attempt,
                        "(unassigned)",
                        -1L,
                        0L,
                        -1L,
                        ioMetricsInfo,
                        "(unassigned)",
                        statusDuration,
                        null);

        assertThat(detailsInfo).isEqualTo(expectedDetailsInfo);
    }
}
