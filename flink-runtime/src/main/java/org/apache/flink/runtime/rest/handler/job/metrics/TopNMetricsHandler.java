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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

/** Handler for Top N metrics requests. */
public class TopNMetricsHandler
        extends AbstractExecutionGraphHandler<TopNMetricsResponseBody, TopNMetricsParameters> {

    public TopNMetricsHandler(
            final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            final Duration timeout,
            final Map<String, String> responseHeaders,
            final TopNMetricsHeaders headers,
            final ExecutionGraphCache executionGraphCache,
            final Executor executor) {
        super(leaderRetriever, timeout, responseHeaders, headers, executionGraphCache, executor);
    }

    @Override
    protected TopNMetricsResponseBody handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request,
            @Nonnull ExecutionGraphInfo executionGraphInfo)
            throws RestHandlerException {
        // TODO: Implement Top N metrics retrieval logic
        // For now, return empty lists to make compilation pass
        return new TopNMetricsResponseBody(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
}
