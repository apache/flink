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
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.FlameGraphTypeQueryParameter;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexFlameGraphHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexFlameGraphParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.threadinfo.JobVertexFlameGraph;
import org.apache.flink.runtime.webmonitor.threadinfo.JobVertexFlameGraphFactory;
import org.apache.flink.runtime.webmonitor.threadinfo.JobVertexThreadInfoStats;
import org.apache.flink.runtime.webmonitor.threadinfo.JobVertexThreadInfoTracker;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Request handler for the job vertex Flame Graph. */
public class JobVertexFlameGraphHandler
        extends AbstractJobVertexHandler<JobVertexFlameGraph, JobVertexFlameGraphParameters> {

    private final JobVertexThreadInfoTracker<JobVertexThreadInfoStats> threadInfoOperatorTracker;

    public JobVertexFlameGraphHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            JobVertexThreadInfoTracker<JobVertexThreadInfoStats> threadInfoOperatorTracker) {
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
    protected JobVertexFlameGraph handleRequest(
            HandlerRequest<EmptyRequestBody, JobVertexFlameGraphParameters> request,
            AccessExecutionJobVertex jobVertex)
            throws RestHandlerException {

        if (jobVertex.getAggregateState().isTerminal()) {
            return JobVertexFlameGraph.terminated();
        }

        final Optional<JobVertexThreadInfoStats> threadInfoSample =
                threadInfoOperatorTracker.getVertexStats(
                        request.getPathParameter(JobIDPathParameter.class), jobVertex);

        final FlameGraphTypeQueryParameter.Type flameGraphType = getFlameGraphType(request);

        final Optional<JobVertexFlameGraph> operatorFlameGraph;

        switch (flameGraphType) {
            case FULL:
                operatorFlameGraph =
                        threadInfoSample.map(JobVertexFlameGraphFactory::createFullFlameGraphFrom);
                break;
            case ON_CPU:
                operatorFlameGraph =
                        threadInfoSample.map(JobVertexFlameGraphFactory::createOnCpuFlameGraph);
                break;
            case OFF_CPU:
                operatorFlameGraph =
                        threadInfoSample.map(JobVertexFlameGraphFactory::createOffCpuFlameGraph);
                break;
            default:
                throw new RestHandlerException(
                        "Unknown Flame Graph type " + flameGraphType + '.',
                        HttpResponseStatus.BAD_REQUEST);
        }

        return operatorFlameGraph.orElse(JobVertexFlameGraph.waiting());
    }

    private static FlameGraphTypeQueryParameter.Type getFlameGraphType(
            HandlerRequest<?, JobVertexFlameGraphParameters> request) {
        final List<FlameGraphTypeQueryParameter.Type> flameGraphTypeParameter =
                request.getQueryParameter(FlameGraphTypeQueryParameter.class);

        if (flameGraphTypeParameter.isEmpty()) {
            return FlameGraphTypeQueryParameter.Type.FULL;
        } else {
            return flameGraphTypeParameter.get(0);
        }
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
                    JobVertexFlameGraph,
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
        protected CompletableFuture<JobVertexFlameGraph> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody, JobVertexFlameGraphParameters> request,
                @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            return CompletableFuture.completedFuture(JobVertexFlameGraph.disabled());
        }
    }
}
