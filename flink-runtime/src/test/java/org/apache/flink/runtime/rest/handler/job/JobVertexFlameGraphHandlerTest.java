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
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.FlameGraphTypeQueryParameter;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexFlameGraphParameters;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.SubtaskIndexQueryParameter;
import org.apache.flink.runtime.webmonitor.stats.VertexStatsTracker;
import org.apache.flink.runtime.webmonitor.threadinfo.VertexFlameGraph;
import org.apache.flink.runtime.webmonitor.threadinfo.VertexThreadInfoStats;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link JobVertexFlameGraphHandler}. */
class JobVertexFlameGraphHandlerTest {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

    private static VertexThreadInfoStats taskThreadInfoStatsDefaultSample;
    private static JobVertexFlameGraphHandler handler;

    @BeforeAll
    static void setUp() {
        taskThreadInfoStatsDefaultSample =
                new VertexThreadInfoStats(
                        8,
                        System.currentTimeMillis(),
                        System.currentTimeMillis() + 100,
                        Collections.emptyMap());

        final RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(new Configuration());
        handler =
                new JobVertexFlameGraphHandler(
                        () -> null,
                        Time.milliseconds(100L),
                        Collections.emptyMap(),
                        new DefaultExecutionGraphCache(
                                restHandlerConfiguration.getTimeout(),
                                Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
                        Executors.directExecutor(),
                        new TestThreadInfoTracker(taskThreadInfoStatsDefaultSample));
    }

    /** Some subtasks are finished, some subtasks are running. */
    @Test
    void testHandleMixedSubtasks() throws Exception {
        final ArchivedExecutionJobVertex archivedExecutionJobVertex =
                new ArchivedExecutionJobVertex(
                        new ArchivedExecutionVertex[] {
                            generateExecutionVertex(0, ExecutionState.FINISHED),
                            generateExecutionVertex(1, ExecutionState.RUNNING)
                        },
                        JOB_VERTEX_ID,
                        "test",
                        2,
                        2,
                        ResourceProfile.UNKNOWN,
                        new StringifiedAccumulatorResult[0]);

        // Check the finished subtask
        HandlerRequest<EmptyRequestBody> request = generateJobVertexFlameGraphParameters(0);
        VertexFlameGraph jobVertexFlameGraph =
                handler.handleRequest(request, archivedExecutionJobVertex);
        assertThat(jobVertexFlameGraph.getEndTime())
                .isEqualTo(VertexFlameGraph.terminated().getEndTime());

        // Check the running subtask
        request = generateJobVertexFlameGraphParameters(1);
        jobVertexFlameGraph = handler.handleRequest(request, archivedExecutionJobVertex);
        assertThat(jobVertexFlameGraph.getEndTime())
                .isEqualTo(taskThreadInfoStatsDefaultSample.getEndTime());

        // Check the job vertex
        request = generateJobVertexFlameGraphParameters(null);
        jobVertexFlameGraph = handler.handleRequest(request, archivedExecutionJobVertex);
        assertThat(jobVertexFlameGraph.getEndTime())
                .isEqualTo(taskThreadInfoStatsDefaultSample.getEndTime());
    }

    /** All subtasks are finished. */
    @Test
    void testHandleFinishedJobVertex() throws Exception {
        final ArchivedExecutionJobVertex archivedExecutionJobVertex =
                new ArchivedExecutionJobVertex(
                        new ArchivedExecutionVertex[] {
                            generateExecutionVertex(0, ExecutionState.FINISHED),
                            generateExecutionVertex(1, ExecutionState.FINISHED)
                        },
                        JOB_VERTEX_ID,
                        "test",
                        2,
                        2,
                        ResourceProfile.UNKNOWN,
                        new StringifiedAccumulatorResult[0]);

        HandlerRequest<EmptyRequestBody> request = generateJobVertexFlameGraphParameters(null);
        VertexFlameGraph jobVertexFlameGraph =
                handler.handleRequest(request, archivedExecutionJobVertex);
        assertThat(jobVertexFlameGraph.getEndTime())
                .isEqualTo(VertexFlameGraph.terminated().getEndTime());
    }

    private HandlerRequest<EmptyRequestBody> generateJobVertexFlameGraphParameters(
            Integer subtaskIndex) throws HandlerRequestException {
        final HashMap<String, String> receivedPathParameters = new HashMap<>(2);
        receivedPathParameters.put(JobIDPathParameter.KEY, JOB_ID.toString());
        receivedPathParameters.put(JobVertexIdPathParameter.KEY, JOB_VERTEX_ID.toString());

        Map<String, List<String>> queryParams = new HashMap<>(2);
        queryParams.put(
                FlameGraphTypeQueryParameter.KEY,
                Collections.singletonList(FlameGraphTypeQueryParameter.Type.FULL.name()));
        if (subtaskIndex != null) {
            queryParams.put(
                    SubtaskIndexQueryParameter.KEY,
                    Collections.singletonList(subtaskIndex.toString()));
        }

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JobVertexFlameGraphParameters(),
                receivedPathParameters,
                queryParams,
                Collections.emptyList());
    }

    private ArchivedExecutionVertex generateExecutionVertex(
            int subtaskIndex, ExecutionState executionState) {
        return new ArchivedExecutionVertex(
                subtaskIndex,
                "test task",
                new ArchivedExecution(
                        new StringifiedAccumulatorResult[0],
                        null,
                        createExecutionAttemptId(JOB_VERTEX_ID, subtaskIndex, 0),
                        executionState,
                        null,
                        null,
                        null,
                        new long[ExecutionState.values().length],
                        new long[ExecutionState.values().length]),
                new ExecutionHistory(0));
    }

    /** A {@link VertexStatsTracker} which returns the pre-generated thread info stats directly. */
    private static class TestThreadInfoTracker
            implements VertexStatsTracker<VertexThreadInfoStats> {

        private final VertexThreadInfoStats stats;

        public TestThreadInfoTracker(VertexThreadInfoStats stats) {
            this.stats = stats;
        }

        @Override
        public Optional<VertexThreadInfoStats> getJobVertexStats(
                JobID jobId, AccessExecutionJobVertex vertex) {
            return Optional.of(stats);
        }

        @Override
        public Optional<VertexThreadInfoStats> getExecutionVertexStats(
                JobID jobId, AccessExecutionJobVertex vertex, int subtaskIndex) {
            return Optional.of(stats);
        }

        @Override
        public void shutDown() throws FlinkException {}
    }
}
