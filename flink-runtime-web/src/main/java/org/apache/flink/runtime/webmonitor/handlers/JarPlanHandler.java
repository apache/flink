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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.handlers.utils.JarHandlerUtils.JarHandlerContext;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/** This handler handles requests to fetch the plan for a jar. */
public class JarPlanHandler
        extends AbstractRestHandler<
                RestfulGateway, JarPlanRequestBody, JobPlanInfo, JarPlanMessageParameters> {

    private final Path jarDir;

    private final Configuration configuration;

    private final Executor executor;

    private final Function<JobGraph, JobPlanInfo> planGenerator;

    public JarPlanHandler(
            final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            final Time timeout,
            final Map<String, String> responseHeaders,
            final MessageHeaders<JarPlanRequestBody, JobPlanInfo, JarPlanMessageParameters>
                    messageHeaders,
            final Path jarDir,
            final Configuration configuration,
            final Executor executor) {
        this(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                jarDir,
                configuration,
                executor,
                jobGraph -> new JobPlanInfo(JsonPlanGenerator.generatePlan(jobGraph)));
    }

    public JarPlanHandler(
            final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            final Time timeout,
            final Map<String, String> responseHeaders,
            final MessageHeaders<JarPlanRequestBody, JobPlanInfo, JarPlanMessageParameters>
                    messageHeaders,
            final Path jarDir,
            final Configuration configuration,
            final Executor executor,
            final Function<JobGraph, JobPlanInfo> planGenerator) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.jarDir = requireNonNull(jarDir);
        this.configuration = requireNonNull(configuration);
        this.executor = requireNonNull(executor);
        this.planGenerator = planGenerator;
    }

    @Override
    protected CompletableFuture<JobPlanInfo> handleRequest(
            @Nonnull final HandlerRequest<JarPlanRequestBody, JarPlanMessageParameters> request,
            @Nonnull final RestfulGateway gateway)
            throws RestHandlerException {
        final JarHandlerContext context = JarHandlerContext.fromRequest(request, jarDir, log);

        return CompletableFuture.supplyAsync(
                () -> {
                    try (PackagedProgram packagedProgram =
                            context.toPackagedProgram(configuration)) {
                        final JobGraph jobGraph =
                                context.toJobGraph(packagedProgram, configuration, true);
                        return planGenerator.apply(jobGraph);
                    }
                },
                executor);
    }
}
