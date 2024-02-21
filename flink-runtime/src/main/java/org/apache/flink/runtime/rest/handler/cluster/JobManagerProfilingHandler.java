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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.cluster.ProfilingRequestBody;
import org.apache.flink.runtime.util.profiler.ProfilingService;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Rest handler which serves the profiling info from the JobManager. */
public class JobManagerProfilingHandler
        extends AbstractRestHandler<
                RestfulGateway, ProfilingRequestBody, ProfilingInfo, EmptyMessageParameters> {
    private final long maxDurationInSeconds;
    private final ProfilingService profilingService;

    public JobManagerProfilingHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<ProfilingRequestBody, ProfilingInfo, EmptyMessageParameters>
                    messageHeaders,
            final Configuration configuration) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.maxDurationInSeconds =
                configuration.get(RestOptions.MAX_PROFILING_DURATION).getSeconds();
        this.profilingService = ProfilingService.getInstance(configuration);
    }

    @Override
    protected CompletableFuture<ProfilingInfo> handleRequest(
            @Nonnull HandlerRequest<ProfilingRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        ProfilingRequestBody profilingRequest = request.getRequestBody();
        int duration = profilingRequest.getDuration();
        if (duration <= 0 || duration > maxDurationInSeconds) {
            return FutureUtils.completedExceptionally(
                    new IllegalArgumentException(
                            String.format(
                                    "`duration` must be set between (0s, %ds].",
                                    maxDurationInSeconds)));
        }
        return profilingService.requestProfiling(
                "JobManager", duration, profilingRequest.getMode());
    }
}
