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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.LogUrlResponse;
import org.apache.flink.runtime.rest.messages.TaskManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.util.EnvironmentInfoUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for the {@link TaskManagerLogUrlHandler}. */
public class TaskManagerLogUrlHandlerTest {

    private TaskManagerLogUrlHandler testInstance;

    private static final String CONTAINER_ID = "container_abc";

    private static final String NM_HOST = "foo.bar";

    private static final String NM_PORT = "0000";

    private static final String USER = "user";

    private Configuration configuration;

    @BeforeEach
    void setup() {
        configuration = new Configuration();
        configuration.set(
                HistoryServerOptions
                        .HISTORY_SERVER_JOBMANAGER_TASKMANAGER_LOG_ENABLE_CUSTOM_HANDLERS,
                true);

        testInstance =
                new TaskManagerLogUrlHandler(
                        () -> null,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        TaskManagerLogUrlHeaders.getInstance(),
                        new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                        Executors.directExecutor(),
                        this.configuration);
    }

    @Test
    public void testGenerateMultipleTaskManagerLogUrls() throws IOException {
        int numTasks = 5;

        Collection<ArchivedJson> archivedTaskManagerUrls =
                testInstance.archiveJsonWithPath(createAccessExecutionGraph(numTasks, 0));

        assertEquals(archivedTaskManagerUrls.size(), numTasks);
    }

    @Test
    public void testNoDuplicateTaskManagerLogUrls() throws IOException {
        int numTasks = 5;
        int numTasksWithSameLocation = 3;
        Collection<ArchivedJson> archivedTaskManagerUrls =
                testInstance.archiveJsonWithPath(
                        createAccessExecutionGraph(numTasks, numTasksWithSameLocation));

        assertEquals(archivedTaskManagerUrls.size(), numTasks - numTasksWithSameLocation + 1);
    }

    @Test
    public void testGenerateTaskManagerLogUrl() {
        EnvironmentInfoUtils.EnvironmentContext environmentContext =
                new EnvironmentInfoUtils.EnvironmentContext(CONTAINER_ID, NM_HOST, NM_PORT, USER);

        LogUrlResponse actual =
                testInstance.createTaskManagerUrl(environmentContext, CONTAINER_ID, NM_HOST);

        LogUrlResponse expected =
                new LogUrlResponse(
                        String.format(
                                TaskManagerLogUrlHandler.TASK_MANAGER_LOG_URL_FORMAT,
                                NM_HOST,
                                NM_PORT,
                                CONTAINER_ID,
                                environmentContext.user));

        assertEquals(actual, expected);
    }

    private static ExecutionGraphInfo createAccessExecutionGraph(
            int numTasks, int numTasksWithSameLocation) {
        Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
        for (int i = 0; i < numTasks - numTasksWithSameLocation; i++) {
            final JobVertexID jobVertexId = new JobVertexID();
            final LocalTaskManagerLocation assignedResourceLocation =
                    new LocalTaskManagerLocation();
            tasks.put(
                    jobVertexId,
                    createArchivedExecutionJobVertexWithLocation(
                            jobVertexId, assignedResourceLocation));
        }

        final LocalTaskManagerLocation fixedResourceLocation = new LocalTaskManagerLocation();
        for (int i = 0; i < numTasksWithSameLocation; i++) {
            final JobVertexID jobVertexId = new JobVertexID();
            tasks.put(
                    jobVertexId,
                    createArchivedExecutionJobVertexWithLocation(
                            jobVertexId, fixedResourceLocation));
        }

        return new ExecutionGraphInfo(new ArchivedExecutionGraphBuilder().setTasks(tasks).build());
    }

    private static ArchivedExecutionJobVertex createArchivedExecutionJobVertexWithLocation(
            JobVertexID jobVertexID, LocalTaskManagerLocation assignedResourceLocation) {
        final StringifiedAccumulatorResult[] emptyAccumulators =
                new StringifiedAccumulatorResult[0];
        final long[] timestamps = new long[ExecutionState.values().length];
        final long[] endTimestamps = new long[ExecutionState.values().length];
        final ExecutionState expectedState = ExecutionState.FINISHED;

        return new ArchivedExecutionJobVertex(
                createArchiveExecutionVertices(
                        3,
                        jobVertexID,
                        expectedState,
                        assignedResourceLocation,
                        timestamps,
                        endTimestamps),
                jobVertexID,
                jobVertexID.toString(),
                1,
                1,
                new SlotSharingGroup(),
                ResourceProfile.UNKNOWN,
                emptyAccumulators);
    }

    private static ArchivedExecutionVertex[] createArchiveExecutionVertices(
            int numSubtasks,
            JobVertexID jobVertexID,
            ExecutionState expectedState,
            LocalTaskManagerLocation location,
            long[] timestamps,
            long[] endTimestamps) {

        ArchivedExecutionVertex[] vertices = new ArchivedExecutionVertex[numSubtasks];

        for (int i = 0; i < numSubtasks; i++) {
            ArchivedExecutionVertex vertex =
                    new ArchivedExecutionVertex(
                            i,
                            "test task",
                            new ArchivedExecution(
                                    new StringifiedAccumulatorResult[0],
                                    null,
                                    createExecutionAttemptId(jobVertexID, i, 1),
                                    expectedState,
                                    null,
                                    location,
                                    new AllocationID(),
                                    timestamps,
                                    endTimestamps),
                            new ExecutionHistory(0));

            vertices[i] = vertex;
        }
        return vertices;
    }
}
