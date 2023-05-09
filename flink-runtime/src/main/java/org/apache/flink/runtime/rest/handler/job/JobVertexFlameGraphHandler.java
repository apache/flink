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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.FlameGraphTypeQueryParameter;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexFlameGraphHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexFlameGraphParameters;
import org.apache.flink.runtime.rest.messages.SubtaskIndexQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.stats.VertexStatsTracker;
import org.apache.flink.runtime.webmonitor.threadinfo.VertexFlameGraph;
import org.apache.flink.runtime.webmonitor.threadinfo.VertexFlameGraphFactory;
import org.apache.flink.runtime.webmonitor.threadinfo.VertexThreadInfoStats;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Request handler for the job vertex Flame Graph. */
public class JobVertexFlameGraphHandler
        extends AbstractJobVertexHandler<VertexFlameGraph, JobVertexFlameGraphParameters> {

    private final VertexStatsTracker<VertexThreadInfoStats> threadInfoOperatorTracker;

    public JobVertexFlameGraphHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            VertexStatsTracker<VertexThreadInfoStats> threadInfoOperatorTracker) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                JobVertexFlameGraphHeaders.getInstance(),
                executionGraphCache,
                executor);
        this.threadInfoOperatorTracker = threadInfoOperatorTracker;
    }

    @Override
    protected VertexFlameGraph handleRequest(
            HandlerRequest<EmptyRequestBody> request, AccessExecutionJobVertex jobVertex)
            throws RestHandlerException {

        @Nullable Integer subtaskIndex = getSubtaskIndex(request, jobVertex);
        if (isTerminated(jobVertex, subtaskIndex)) {
            return VertexFlameGraph.terminated();
        }

        final Optional<VertexThreadInfoStats> threadInfoSample;
        if (subtaskIndex == null) {
            threadInfoSample =
                    threadInfoOperatorTracker.getJobVertexStats(
                            request.getPathParameter(JobIDPathParameter.class), jobVertex);
        } else {
            threadInfoSample =
                    threadInfoOperatorTracker.getExecutionVertexStats(
                            request.getPathParameter(JobIDPathParameter.class),
                            jobVertex,
                            subtaskIndex);
        }

        final FlameGraphTypeQueryParameter.Type flameGraphType = getFlameGraphType(request);

        final Optional<VertexFlameGraph> operatorFlameGraph;

        switch (flameGraphType) {
            case FULL:
                operatorFlameGraph =
                        threadInfoSample.map(VertexFlameGraphFactory::createFullFlameGraphFrom);
                break;
            case ON_CPU:
                operatorFlameGraph =
                        threadInfoSample.map(VertexFlameGraphFactory::createOnCpuFlameGraph);
                break;
            case OFF_CPU:
                operatorFlameGraph =
                        threadInfoSample.map(VertexFlameGraphFactory::createOffCpuFlameGraph);
                break;
            default:
                throw new RestHandlerException(
                        "Unknown Flame Graph type " + flameGraphType + '.',
                        HttpResponseStatus.BAD_REQUEST);
        }

        return operatorFlameGraph.orElse(VertexFlameGraph.waiting());
    }

    private boolean isTerminated(
            AccessExecutionJobVertex jobVertex, @Nullable Integer subtaskIndex) {
        if (subtaskIndex == null) {
            return jobVertex.getAggregateState().isTerminal();
        }
        AccessExecutionVertex executionVertex = jobVertex.getTaskVertices()[subtaskIndex];
        return executionVertex.getExecutionState().isTerminal();
    }

    private static FlameGraphTypeQueryParameter.Type getFlameGraphType(HandlerRequest<?> request) {
        final List<FlameGraphTypeQueryParameter.Type> flameGraphTypeParameter =
                request.getQueryParameter(FlameGraphTypeQueryParameter.class);

        if (flameGraphTypeParameter.isEmpty()) {
            return FlameGraphTypeQueryParameter.Type.FULL;
        }
        return flameGraphTypeParameter.get(0);
    }

    @Nullable
    private static Integer getSubtaskIndex(
            HandlerRequest<?> request, AccessExecutionJobVertex jobVertex)
            throws RestHandlerException {
        final List<Integer> subtaskIndexParameter =
                request.getQueryParameter(SubtaskIndexQueryParameter.class);

        if (subtaskIndexParameter.isEmpty()) {
            return null;
        }
        int subtaskIndex = subtaskIndexParameter.get(0);
        if (subtaskIndex >= jobVertex.getTaskVertices().length || subtaskIndex < 0) {
            throw new RestHandlerException(
                    "Invalid subtask index for vertex " + jobVertex.getJobVertexId(),
                    HttpResponseStatus.NOT_FOUND);
        }
        return subtaskIndex;
    }

    @Override
    public void close() throws Exception {
        threadInfoOperatorTracker.shutDown();
    }

    public static AbstractRestHandler<?, ?, ?, ?> disabledHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders) {
        return new DisabledJobVertexFlameGraphHandler(leaderRetriever, timeout, responseHeaders);
    }

    private static class DisabledJobVertexFlameGraphHandler
            extends AbstractRestHandler<
                    RestfulGateway,
                    EmptyRequestBody,
                    VertexFlameGraph,
                    JobVertexFlameGraphParameters> {
        protected DisabledJobVertexFlameGraphHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    JobVertexFlameGraphHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<VertexFlameGraph> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            return CompletableFuture.completedFuture(VertexFlameGraph.disabled());
        }
    }
}
