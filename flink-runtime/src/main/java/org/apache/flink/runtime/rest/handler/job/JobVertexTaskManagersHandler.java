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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.AggregatedTaskDetailsInfo;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.JobVertexTaskManagersInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.StatusDurationUtils;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.OnlyExecutionGraphJsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * A request handler that provides the details of a job vertex, including id, name, and the runtime
 * and metrics of all its subtasks aggregated by TaskManager.
 */
public class JobVertexTaskManagersHandler
        extends AbstractAccessExecutionGraphHandler<
                JobVertexTaskManagersInfo, JobVertexMessageParameters>
        implements OnlyExecutionGraphJsonArchivist {
    private MetricFetcher metricFetcher;

    public JobVertexTaskManagersHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobVertexTaskManagersInfo, JobVertexMessageParameters>
                    messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            MetricFetcher metricFetcher) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executionGraphCache,
                executor);
        this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
    }

    @Override
    protected JobVertexTaskManagersInfo handleRequest(
            HandlerRequest<EmptyRequestBody> request, AccessExecutionGraph executionGraph)
            throws RestHandlerException {
        JobID jobID = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
        AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

        if (jobVertex == null) {
            throw new NotFoundException(String.format("JobVertex %s not found", jobVertexID));
        }

        return createJobVertexTaskManagersInfo(jobVertex, jobID, metricFetcher);
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph)
            throws IOException {
        Collection<? extends AccessExecutionJobVertex> vertices = graph.getAllVertices().values();
        List<ArchivedJson> archive = new ArrayList<>(vertices.size());
        for (AccessExecutionJobVertex task : vertices) {
            ResponseBody json = createJobVertexTaskManagersInfo(task, graph.getJobID(), null);
            String path =
                    getMessageHeaders()
                            .getTargetRestEndpointURL()
                            .replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
                            .replace(
                                    ':' + JobVertexIdPathParameter.KEY,
                                    task.getJobVertexId().toString());
            archive.add(new ArchivedJson(path, json));
        }
        return archive;
    }

    private static JobVertexTaskManagersInfo createJobVertexTaskManagersInfo(
            AccessExecutionJobVertex jobVertex,
            JobID jobID,
            @Nullable MetricFetcher metricFetcher) {
        // Build a map that groups task executions by TaskManager
        Map<String, String> taskManagerId2Host = new HashMap<>();
        Map<String, List<AccessExecution>> taskManagerExecutions = new HashMap<>();
        Set<AccessExecution> representativeExecutions = new HashSet<>();
        for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
            AccessExecution representativeAttempt = vertex.getCurrentExecutionAttempt();
            representativeExecutions.add(representativeAttempt);

            for (AccessExecution execution : vertex.getCurrentExecutions()) {
                TaskManagerLocation location = execution.getAssignedResourceLocation();
                String taskManagerHost =
                        location == null
                                ? "(unassigned)"
                                : location.getHostname() + ':' + location.dataPort();
                String taskmanagerId =
                        location == null ? "(unassigned)" : location.getResourceID().toString();
                taskManagerId2Host.put(taskmanagerId, taskManagerHost);
                List<AccessExecution> executions =
                        taskManagerExecutions.computeIfAbsent(
                                taskmanagerId, ignored -> new ArrayList<>());
                executions.add(execution);
            }
        }

        final long now = System.currentTimeMillis();

        List<JobVertexTaskManagersInfo.TaskManagersInfo> taskManagersInfoList = new ArrayList<>(4);
        for (Map.Entry<String, List<AccessExecution>> entry : taskManagerExecutions.entrySet()) {
            String taskmanagerId = entry.getKey();
            String host = taskManagerId2Host.get(taskmanagerId);
            List<AccessExecution> executions = entry.getValue();

            List<IOMetricsInfo> ioMetricsInfos = new ArrayList<>();
            List<Map<ExecutionState, Long>> status =
                    executions.stream()
                            .map(StatusDurationUtils::getExecutionStateDuration)
                            .collect(Collectors.toList());

            // executionsPerState counts attempts of a subtask separately
            int[] executionsPerState = new int[ExecutionState.values().length];
            // tasksPerState counts only the representative attempts, and is used to aggregate the
            // task manager state
            int[] tasksPerState = new int[ExecutionState.values().length];

            long startTime = Long.MAX_VALUE;
            long endTime = 0;
            boolean allFinished = true;

            MutableIOMetrics counts = new MutableIOMetrics();

            int representativeAttemptsCount = 0;
            for (AccessExecution execution : executions) {
                final ExecutionState state = execution.getState();
                executionsPerState[state.ordinal()]++;
                if (representativeExecutions.contains(execution)) {
                    tasksPerState[state.ordinal()]++;
                    representativeAttemptsCount++;
                }

                // take the earliest start time
                long started = execution.getStateTimestamp(ExecutionState.DEPLOYING);
                if (started > 0) {
                    startTime = Math.min(startTime, started);
                }

                allFinished &= state.isTerminal();
                endTime = Math.max(endTime, execution.getStateTimestamp(state));

                counts.addIOMetrics(
                        execution,
                        metricFetcher,
                        jobID.toString(),
                        jobVertex.getJobVertexId().toString());
                MutableIOMetrics current = new MutableIOMetrics();
                current.addIOMetrics(
                        execution,
                        metricFetcher,
                        jobID.toString(),
                        jobVertex.getJobVertexId().toString());
                ioMetricsInfos.add(
                        new IOMetricsInfo(
                                current.getNumBytesIn(),
                                current.isNumBytesInComplete(),
                                current.getNumBytesOut(),
                                current.isNumBytesOutComplete(),
                                current.getNumRecordsIn(),
                                current.isNumRecordsInComplete(),
                                current.getNumRecordsOut(),
                                current.isNumRecordsOutComplete(),
                                current.getAccumulateBackPressuredTime(),
                                current.getAccumulateIdleTime(),
                                current.getAccumulateBusyTime()));
            }

            long duration;
            if (startTime < Long.MAX_VALUE) {
                if (allFinished) {
                    duration = endTime - startTime;
                } else {
                    endTime = -1L;
                    duration = now - startTime;
                }
            } else {
                startTime = -1L;
                endTime = -1L;
                duration = -1L;
            }

            // Safe when tasksPerState are all zero and representativeAttemptsCount is zero
            ExecutionState jobVertexState =
                    ExecutionJobVertex.getAggregateJobVertexState(
                            tasksPerState, representativeAttemptsCount);

            final IOMetricsInfo jobVertexMetrics =
                    new IOMetricsInfo(
                            counts.getNumBytesIn(),
                            counts.isNumBytesInComplete(),
                            counts.getNumBytesOut(),
                            counts.isNumBytesOutComplete(),
                            counts.getNumRecordsIn(),
                            counts.isNumRecordsInComplete(),
                            counts.getNumRecordsOut(),
                            counts.isNumRecordsOutComplete(),
                            counts.getAccumulateBackPressuredTime(),
                            counts.getAccumulateIdleTime(),
                            counts.getAccumulateBusyTime());

            Map<ExecutionState, Integer> statusCounts =
                    CollectionUtil.newHashMapWithExpectedSize(ExecutionState.values().length);
            for (ExecutionState state : ExecutionState.values()) {
                statusCounts.put(state, executionsPerState[state.ordinal()]);
            }
            taskManagersInfoList.add(
                    new JobVertexTaskManagersInfo.TaskManagersInfo(
                            host,
                            jobVertexState,
                            startTime,
                            endTime,
                            duration,
                            jobVertexMetrics,
                            statusCounts,
                            taskmanagerId,
                            AggregatedTaskDetailsInfo.create(ioMetricsInfos, status)));
        }

        return new JobVertexTaskManagersInfo(
                jobVertex.getJobVertexId(), jobVertex.getName(), now, taskManagersInfoList);
    }
}
