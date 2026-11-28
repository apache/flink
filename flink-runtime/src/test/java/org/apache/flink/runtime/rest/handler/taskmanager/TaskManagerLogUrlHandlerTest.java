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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobTaskManagerMessageParameters;
import org.apache.flink.runtime.rest.messages.LogUrlResponse;
import org.apache.flink.runtime.rest.messages.TaskManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.util.EnvironmentInfoUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the {@link TaskManagerLogUrlHandler}. */
class TaskManagerLogUrlHandlerTest {

    private static final String JM_CONTAINER_ID = "container_jm";

    private static final String JM_NM_HOST = "jm-node.example.com";

    private static final String NM_PORT = "8042";

    private static final String USER = "user";

    private final TaskManagerLogUrlHandler testInstance =
            new TaskManagerLogUrlHandler(
                    () -> null,
                    TestingUtils.TIMEOUT,
                    Collections.emptyMap(),
                    TaskManagerLogUrlHeaders.getInstance(),
                    new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                    Executors.directExecutor(),
                    new EnvironmentInfoUtils.EnvironmentContext(
                            JM_CONTAINER_ID, JM_NM_HOST, NM_PORT, USER));

    @Test
    void testGenerateTaskManagerLogUrlUsesTaskManagerHost() {
        assertThat(testInstance.createTaskManagerUrl("container_tm", "tm-node.example.com"))
                .isEqualTo(
                        new LogUrlResponse(
                                "http://tm-node.example.com:8042/node/containerlogs/container_tm/user"));
    }

    @Test
    void testArchiveContainsCurrentAndHistoricalTaskManagers() throws Exception {
        final JobID jobId = new JobID();
        final JobVertexID vertexId = new JobVertexID();
        final TaskManagerLocation current = location("container_current", "current.example.com");
        final TaskManagerLocation previous = location("container_previous", "previous.example.com");

        final Collection<ArchivedJson> archived =
                testInstance.archiveJsonWithPath(
                        executionGraphInfo(
                                jobId, vertexId, execution(vertexId, current), history(previous)));

        assertThat(
                        archived.stream()
                                .collect(
                                        Collectors.toMap(
                                                ArchivedJson::getPath, ArchivedJson::getJson)))
                .hasSize(2)
                .containsEntry(
                        "/jobs/" + jobId + "/taskmanagers/container_current/log-url",
                        "{\"url\":\"http://current.example.com:8042/node/containerlogs/container_current/user\"}")
                .containsEntry(
                        "/jobs/" + jobId + "/taskmanagers/container_previous/log-url",
                        "{\"url\":\"http://previous.example.com:8042/node/containerlogs/container_previous/user\"}");
    }

    @Test
    void testArchiveDeduplicatesTaskManagersAcrossTasks() throws Exception {
        final JobID jobId = new JobID();
        final TaskManagerLocation shared = location("container_shared", "shared.example.com");
        final Map<JobVertexID, ArchivedExecutionJobVertex> vertices = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            final JobVertexID vertexId = new JobVertexID();
            vertices.put(vertexId, jobVertex(vertexId, execution(vertexId, shared), history()));
        }

        final Collection<ArchivedJson> archived =
                testInstance.archiveJsonWithPath(
                        new ExecutionGraphInfo(
                                new ArchivedExecutionGraphBuilder()
                                        .setJobID(jobId)
                                        .setTasks(vertices)
                                        .build()));

        assertThat(archived)
                .extracting(ArchivedJson::getPath)
                .containsExactly("/jobs/" + jobId + "/taskmanagers/container_shared/log-url");
    }

    @Test
    void testHandleRequestFindsCurrentAndHistoricalTaskManager() throws Exception {
        final JobVertexID vertexId = new JobVertexID();
        final ExecutionGraphInfo info =
                executionGraphInfo(
                        new JobID(),
                        vertexId,
                        execution(vertexId, location("container_current", "current.example.com")),
                        history(location("container_previous", "previous.example.com")));

        assertThat(handle(info, "container_current"))
                .isEqualTo(
                        new LogUrlResponse(
                                "http://current.example.com:8042/node/containerlogs/container_current/user"));
        assertThat(handle(info, "container_previous"))
                .isEqualTo(
                        new LogUrlResponse(
                                "http://previous.example.com:8042/node/containerlogs/container_previous/user"));
    }

    @Test
    void testHandleRequestFailsWithNotFoundForUnknownTaskManager() {
        final JobVertexID vertexId = new JobVertexID();
        final ExecutionGraphInfo info =
                executionGraphInfo(
                        new JobID(),
                        vertexId,
                        execution(vertexId, location("container_current", "current.example.com")),
                        history());

        assertThatThrownBy(() -> handle(info, "container_unknown"))
                .isInstanceOf(RestHandlerException.class)
                .hasMessageContaining("container_unknown");
    }

    private LogUrlResponse handle(ExecutionGraphInfo info, String taskManagerId) throws Exception {
        final JobTaskManagerMessageParameters parameters = new JobTaskManagerMessageParameters();
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, info.getJobId().toString());
        pathParameters.put(TaskManagerIdPathParameter.KEY, taskManagerId);
        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        parameters,
                        pathParameters,
                        Collections.emptyMap(),
                        Collections.emptyList());
        return testInstance.handleRequest(request, info.getArchivedExecutionGraph());
    }

    private static TaskManagerLocation location(String resourceId, String fqdnHost) {
        return new TaskManagerLocation(
                new ResourceID(resourceId),
                InetAddress.getLoopbackAddress(),
                1234,
                new TaskManagerLocation.HostNameSupplier() {
                    @Override
                    public String getHostName() {
                        return fqdnHost;
                    }

                    @Override
                    public String getFqdnHostName() {
                        return fqdnHost;
                    }
                },
                fqdnHost + ":5678");
    }

    private static ArchivedExecution execution(JobVertexID vertexId, TaskManagerLocation location) {
        return new ArchivedExecution(
                new StringifiedAccumulatorResult[0],
                null,
                createExecutionAttemptId(vertexId, 0, 0),
                ExecutionState.RUNNING,
                null,
                location,
                new AllocationID(),
                new long[ExecutionState.values().length],
                new long[ExecutionState.values().length]);
    }

    private static ExecutionHistory history(TaskManagerLocation... locations) {
        final JobVertexID vertexId = new JobVertexID();
        final List<ArchivedExecution> executions =
                Arrays.stream(locations)
                        .map(location -> execution(vertexId, location))
                        .collect(Collectors.toList());
        return new ExecutionHistory(Math.max(1, locations.length)) {
            @Override
            public Collection<ArchivedExecution> getHistoricalExecutions() {
                return executions;
            }
        };
    }

    private static ExecutionGraphInfo executionGraphInfo(
            JobID jobId,
            JobVertexID vertexId,
            ArchivedExecution current,
            ExecutionHistory history) {
        return new ExecutionGraphInfo(
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setTasks(
                                Collections.singletonMap(
                                        vertexId, jobVertex(vertexId, current, history)))
                        .build());
    }

    private static ArchivedExecutionJobVertex jobVertex(
            JobVertexID vertexId, ArchivedExecution current, ExecutionHistory history) {
        return new ArchivedExecutionJobVertex(
                new ArchivedExecutionVertex[] {
                    new ArchivedExecutionVertex(0, "test task", current, history)
                },
                vertexId,
                vertexId.toString(),
                1,
                1,
                new SlotSharingGroup(),
                ResourceProfile.UNKNOWN,
                new StringifiedAccumulatorResult[0]);
    }
}
