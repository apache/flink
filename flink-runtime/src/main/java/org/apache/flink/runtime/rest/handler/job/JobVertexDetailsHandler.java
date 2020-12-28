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
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/** Request handler for the job vertex details. */
public class JobVertexDetailsHandler
        extends AbstractExecutionGraphHandler<JobVertexDetailsInfo, JobVertexMessageParameters>
        implements JsonArchivist {
    private final MetricFetcher metricFetcher;

    public JobVertexDetailsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobVertexDetailsInfo, JobVertexMessageParameters>
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
        this.metricFetcher = metricFetcher;
    }

    @Override
    protected JobVertexDetailsInfo handleRequest(
            HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
            AccessExecutionGraph executionGraph)
            throws NotFoundException {
        JobID jobID = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
        AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

        if (jobVertex == null) {
            throw new NotFoundException(String.format("JobVertex %s not found", jobVertexID));
        }

        return createJobVertexDetailsInfo(jobVertex, jobID, metricFetcher);
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph)
            throws IOException {
        Collection<? extends AccessExecutionJobVertex> vertices = graph.getAllVertices().values();
        List<ArchivedJson> archive = new ArrayList<>(vertices.size());
        for (AccessExecutionJobVertex task : vertices) {
            ResponseBody json = createJobVertexDetailsInfo(task, graph.getJobID(), null);
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

    private static JobVertexDetailsInfo createJobVertexDetailsInfo(
            AccessExecutionJobVertex jobVertex,
            JobID jobID,
            @Nullable MetricFetcher metricFetcher) {
        List<SubtaskExecutionAttemptDetailsInfo> subtasks = new ArrayList<>();
        final long now = System.currentTimeMillis();
        for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
            final AccessExecution execution = vertex.getCurrentExecutionAttempt();
            final JobVertexID jobVertexID = jobVertex.getJobVertexId();
            subtasks.add(
                    SubtaskExecutionAttemptDetailsInfo.create(
                            execution, metricFetcher, jobID, jobVertexID));
        }

        return new JobVertexDetailsInfo(
                jobVertex.getJobVertexId(),
                jobVertex.getName(),
                jobVertex.getParallelism(),
                now,
                subtasks);
    }
}
