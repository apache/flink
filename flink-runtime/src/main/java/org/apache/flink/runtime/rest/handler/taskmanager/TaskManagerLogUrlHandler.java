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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractAccessExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobTaskManagerMessageParameters;
import org.apache.flink.runtime.rest.messages.LogUrlResponse;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TaskManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.util.EnvironmentInfoUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/** Request handler for retrieving the task manager log url. */
@Internal
public class TaskManagerLogUrlHandler
        extends AbstractAccessExecutionGraphHandler<LogUrlResponse, JobTaskManagerMessageParameters>
        implements JsonArchivist {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerLogUrlHandler.class);

    private final EnvironmentInfoUtils.EnvironmentContext environmentContext;

    public TaskManagerLogUrlHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, LogUrlResponse, JobTaskManagerMessageParameters>
                    messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            EnvironmentInfoUtils.EnvironmentContext environmentContext) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executionGraphCache,
                executor);
        this.environmentContext = Preconditions.checkNotNull(environmentContext);
    }

    /**
     * Looks up the task manager with the requested resource id in the current and historical
     * executions. Fails with 404 if no execution ran on that task manager, e.g. because its
     * container has not been allocated yet.
     */
    @Override
    protected LogUrlResponse handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, AccessExecutionGraph executionGraph)
            throws RestHandlerException {
        ResourceID containerId = request.getPathParameter(TaskManagerIdPathParameter.class);
        Map<JobVertexID, ? extends AccessExecutionJobVertex> jobVertices =
                executionGraph.getAllVertices();

        for (AccessExecutionJobVertex jobVertex : jobVertices.values()) {
            for (AccessExecutionVertex task : jobVertex.getTaskVertices()) {

                TaskManagerLocation currentLocation = task.getCurrentAssignedResourceLocation();

                if (currentLocation != null
                        && currentLocation.getResourceID().equals(containerId)) {
                    return createTaskManagerUrl(
                            containerId.toString(), currentLocation.getFQDNHostname());
                } else {
                    ExecutionHistory executionHistory = task.getExecutionHistory();
                    for (ArchivedExecution execution : executionHistory.getHistoricalExecutions()) {
                        TaskManagerLocation location = execution.getAssignedResourceLocation();
                        if (location != null && location.getResourceID().equals(containerId)) {
                            return createTaskManagerUrl(
                                    containerId.toString(), location.getFQDNHostname());
                        }
                    }
                }
            }
        }

        // If we are unable to find the hostname of the task manager for the container in the
        // execution graph, throw an error.
        throw new RestHandlerException(
                "Unable to find hostname for containerId: "
                        + containerId
                        + " and job id: "
                        + executionGraph.getJobID(),
                HttpResponseStatus.NOT_FOUND);
    }

    /**
     * Archives one log url per task manager that executed a task of the job, covering both the
     * current and the historical executions.
     */
    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(ExecutionGraphInfo executionGraphInfo)
            throws IOException {
        Collection<? extends AccessExecutionJobVertex> vertices =
                executionGraphInfo.getArchivedExecutionGraph().getAllVertices().values();
        Set<ArchivedJson> archive = new HashSet<>();
        for (AccessExecutionJobVertex jobVertex : vertices) {
            for (AccessExecutionVertex task : jobVertex.getTaskVertices()) {
                Set<ArchivedJson> vertexArchivedJson =
                        processAllTaskManagerExecutionsPerVertex(
                                executionGraphInfo.getJobId().toString(), task);
                archive.addAll(vertexArchivedJson);
            }
        }
        return archive;
    }

    private Set<ArchivedJson> processAllTaskManagerExecutionsPerVertex(
            String jobId, AccessExecutionVertex vertex) throws IOException {
        Set<ArchivedJson> archivedJsons = new HashSet<>();

        // The current execution is not part of the execution history, so handle it separately.
        AccessExecution currentExecution = vertex.getCurrentExecutionAttempt();
        if (currentExecution != null && currentExecution.getAssignedResourceLocation() != null) {
            archivedJsons.add(
                    buildTaskManagerJson(currentExecution.getAssignedResourceLocation(), jobId));
        }
        ExecutionHistory executionHistory = vertex.getExecutionHistory();
        for (ArchivedExecution execution : executionHistory.getHistoricalExecutions()) {
            final TaskManagerLocation location = execution.getAssignedResourceLocation();
            if (location == null) {
                LOG.warn(
                        "Historical execution {} of job {} has no assigned resource location, skipping its log url.",
                        execution.getAttemptId(),
                        jobId);
                continue;
            }
            archivedJsons.add(buildTaskManagerJson(location, jobId));
        }
        return archivedJsons;
    }

    private ArchivedJson buildTaskManagerJson(TaskManagerLocation location, String jobId)
            throws IOException {
        String containerId = location.getResourceID().toString();
        String path =
                TaskManagerLogUrlHeaders.getInstance()
                        .getTargetRestEndpointURL()
                        .replace(':' + JobIDPathParameter.KEY, jobId)
                        .replace(':' + TaskManagerIdPathParameter.KEY, containerId);
        ResponseBody taskManagerUrl = createTaskManagerUrl(containerId, location.getFQDNHostname());
        return new ArchivedJson(path, taskManagerUrl);
    }

    /**
     * Generates a URL that will link to the location of the task manager logs. The format of the
     * URL will be: http://TM-NM-HOST:NM-HTTP-PORT/node/containerlogs/CONTAINER-ID/USER
     *
     * <p>The NodeManager HTTP port and the user are taken from the JobManager's container, so this
     * assumes that all NodeManagers of the cluster use the same web port.
     */
    @VisibleForTesting
    LogUrlResponse createTaskManagerUrl(String containerId, String nodeManagerHost) {
        return new LogUrlResponse(
                EnvironmentInfoUtils.createNodeManagerLogUrl(
                        nodeManagerHost,
                        environmentContext.nodeManagerHttpPort,
                        containerId,
                        environmentContext.user));
    }
}
