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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.UpperLimitExceptionParameter;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Test for the {@link JobExceptionsHandler}. */
public class JobExceptionsHandlerTest extends TestLogger {

    @Test
    public void testGetJobExceptionsInfo() throws HandlerRequestException {
        final JobExceptionsHandler jobExceptionsHandler =
                new JobExceptionsHandler(
                        () -> null,
                        TestingUtils.TIMEOUT(),
                        Collections.emptyMap(),
                        JobExceptionsHeaders.getInstance(),
                        new DefaultExecutionGraphCache(
                                TestingUtils.TIMEOUT(), TestingUtils.TIMEOUT()),
                        TestingUtils.defaultExecutor());
        final int numExceptions = 20;
        final AccessExecutionGraph archivedExecutionGraph =
                createAccessExecutionGraph(numExceptions);
        checkExceptionLimit(jobExceptionsHandler, archivedExecutionGraph, numExceptions, 10);
        checkExceptionLimit(
                jobExceptionsHandler, archivedExecutionGraph, numExceptions, numExceptions);
        checkExceptionLimit(jobExceptionsHandler, archivedExecutionGraph, numExceptions, 30);
    }

    private static void checkExceptionLimit(
            JobExceptionsHandler jobExceptionsHandler,
            AccessExecutionGraph graph,
            int maxNumExceptions,
            int numExpectedException)
            throws HandlerRequestException {
        final HandlerRequest<EmptyRequestBody, JobExceptionsMessageParameters> handlerRequest =
                createRequest(graph.getJobID(), numExpectedException);
        final JobExceptionsInfo jobExceptionsInfo =
                jobExceptionsHandler.handleRequest(handlerRequest, graph);
        final int numReportedException =
                maxNumExceptions >= numExpectedException ? numExpectedException : maxNumExceptions;
        assertEquals(jobExceptionsInfo.getAllExceptions().size(), numReportedException);
    }

    private static AccessExecutionGraph createAccessExecutionGraph(int numTasks) {
        Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
        for (int i = 0; i < numTasks; i++) {
            final JobVertexID jobVertexId = new JobVertexID();
            tasks.put(jobVertexId, createArchivedExecutionJobVertex(jobVertexId));
        }
        return new ArchivedExecutionGraphBuilder().setTasks(tasks).build();
    }

    private static ArchivedExecutionJobVertex createArchivedExecutionJobVertex(
            JobVertexID jobVertexID) {
        final StringifiedAccumulatorResult[] emptyAccumulators =
                new StringifiedAccumulatorResult[0];
        final long[] timestamps = new long[ExecutionState.values().length];
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
                                    new ExecutionAttemptID(),
                                    attempt,
                                    expectedState,
                                    "error",
                                    assignedResourceLocation,
                                    allocationID,
                                    subtaskIndex,
                                    timestamps),
                            new EvictingBoundedList<>(0))
                },
                jobVertexID,
                jobVertexID.toString(),
                1,
                1,
                ResourceProfile.UNKNOWN,
                emptyAccumulators);
    }

    private static HandlerRequest<EmptyRequestBody, JobExceptionsMessageParameters> createRequest(
            JobID jobId, int size) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());
        final Map<String, List<String>> queryParameters = new HashMap<>();
        queryParameters.put(UpperLimitExceptionParameter.KEY, Collections.singletonList("" + size));

        return new HandlerRequest<>(
                EmptyRequestBody.getInstance(),
                new JobExceptionsMessageParameters(),
                pathParameters,
                queryParameters);
    }
}
