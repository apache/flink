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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
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
public class TaskManagerLogUrlHandler
        extends AbstractAccessExecutionGraphHandler<LogUrlResponse, JobTaskManagerMessageParameters>
        implements JsonArchivist {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerLogUrlHandler.class);
    public static final String TASK_MANAGER_LOG_URL_FORMAT =
            "http://%s:%s/node/containerlogs/%s/%s";

    private final Configuration config;

    public TaskManagerLogUrlHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, LogUrlResponse, JobTaskManagerMessageParameters>
                    messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            Configuration configuration) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executionGraphCache,
                executor);
        this.config = configuration;
    }

    /**
     * When the endpoint is hit for a live application, it may not return a log-url if the container
     * has not yet been allocated.
     */
    @Override
    protected LogUrlResponse handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, AccessExecutionGraph executionGraph)
            throws RestHandlerException {
        EnvironmentInfoUtils.EnvironmentContext environmentContext =
                EnvironmentInfoUtils.getEnvironmentContext();
        ResourceID containerId = request.getPathParameter(TaskManagerIdPathParameter.class);
        Map<JobVertexID, ? extends AccessExecutionJobVertex> jobVertices =
                executionGraph.getAllVertices();

        for (AccessExecutionJobVertex jobVertex : jobVertices.values()) {
            for (AccessExecutionVertex task : jobVertex.getTaskVertices()) {

                TaskManagerLocation currentLocation = task.getCurrentAssignedResourceLocation();

                if (currentLocation != null
                        && currentLocation.getResourceID().equals(containerId)) {
                    return createTaskManagerUrl(
                            environmentContext,
                            containerId.toString(),
                            currentLocation.getFQDNHostname());
                } else {
                    ExecutionHistory executionHistory = task.getExecutionHistory();
                    for (ArchivedExecution execution : executionHistory.getHistoricalExecutions()) {
                        TaskManagerLocation location = execution.getAssignedResourceLocation();
                        if (location != null && location.getResourceID().equals(containerId)) {
                            return createTaskManagerUrl(
                                    environmentContext,
                                    containerId.toString(),
                                    location.getFQDNHostname());
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
     * Generates a URL that will link to the location of the task manager logs. The format of the
     * URL will be:
     * Job-History-base-URL/CONTAINER-NM-HOST:CONTAINER-NM-PORT/CONTAINER-ID/CONTAINER-ID/USER/
     */
    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(ExecutionGraphInfo executionGraphInfo)
            throws IOException {
        EnvironmentInfoUtils.EnvironmentContext environmentContext =
                EnvironmentInfoUtils.getEnvironmentContext();
        Collection<? extends AccessExecutionJobVertex> vertices =
                executionGraphInfo.getArchivedExecutionGraph().getAllVertices().values();
        Set<ArchivedJson> archive = new HashSet<>();
        for (AccessExecutionJobVertex jobVertex : vertices) {
            for (AccessExecutionVertex task : jobVertex.getTaskVertices()) {
                Set<ArchivedJson> vertexArchivedJson =
                        processAllTaskManagerExecutionsPerVertex(
                                executionGraphInfo.getJobId().toString(), task, environmentContext);
                archive.addAll(vertexArchivedJson);
            }
        }
        return archive;
    }

    private Set<ArchivedJson> processAllTaskManagerExecutionsPerVertex(
            String jobId,
            AccessExecutionVertex vertex,
            EnvironmentInfoUtils.EnvironmentContext environmentContext)
            throws IOException {
        Set<ArchivedJson> archivedJsons = new HashSet<>();

        // The current execution is not included in the execution history so it is
        // fetched separately
        AccessExecution currentExecution = vertex.getCurrentExecutionAttempt();
        ArchivedJson currentJson = null;
        if (isValidExecutionResourceLocation(currentExecution)) {
            currentJson =
                    buildTaskManagerJson(
                            currentExecution.getAssignedResourceLocation(),
                            jobId,
                            environmentContext);
            archivedJsons.add(currentJson);
        }
        ExecutionHistory executionHistory = vertex.getExecutionHistory();
        for (ArchivedExecution execution : executionHistory.getHistoricalExecutions()) {
            // In case the task manager location is not available, skip the execution
            if (!isValidExecutionResourceLocation(execution)) {
                LOG.warn(
                        "Archived execution with jobId: {} has no assigned resource location, skipping building json",
                        jobId);
                continue;
            }
            TaskManagerLocation location = execution.getAssignedResourceLocation();
            ArchivedJson json = buildTaskManagerJson(location, jobId, environmentContext);
            archivedJsons.add(json);
        }
        return archivedJsons;
    }

    private boolean isValidExecutionResourceLocation(AccessExecution execution) {
        return execution != null
                && execution.getAssignedResourceLocation() != null
                && execution.getAssignedResourceLocation().getResourceID() != null;
    }

    private ArchivedJson buildTaskManagerJson(
            TaskManagerLocation location,
            String jobId,
            EnvironmentInfoUtils.EnvironmentContext environmentContext)
            throws IOException {
        String containerId = location.getResourceID().toString();
        String path =
                TaskManagerLogUrlHeaders.getInstance()
                        .getTargetRestEndpointURL()
                        .replace(':' + JobIDPathParameter.KEY, jobId)
                        .replace(':' + TaskManagerIdPathParameter.KEY, containerId);
        ResponseBody taskManagerUrl =
                createTaskManagerUrl(environmentContext, containerId, location.getFQDNHostname());
        return new ArchivedJson(path, taskManagerUrl);
    }

    /**
     * Generates a URL that will link to the location of the task manager logs. The format of the
     * URL will be: CONTAINER-NM-HOST:CONTAINER-NM-HTTP-PORT/node/containerlogs/CONTAINER-ID/USER/
     */
    @VisibleForTesting
    public LogUrlResponse createTaskManagerUrl(
            EnvironmentInfoUtils.EnvironmentContext environmentContext,
            String containerId,
            String nodeManagerHttpHostname) {
        return new LogUrlResponse(
                String.format(
                        TASK_MANAGER_LOG_URL_FORMAT,
                        nodeManagerHttpHostname,
                        environmentContext.nodeManagerHttpPort,
                        containerId,
                        environmentContext.user));
    }
}
