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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.handlers.utils.ArtifactHandlerUtils;
import org.apache.flink.runtime.webmonitor.handlers.utils.ArtifactHandlerUtils.ArtifactHandlerContext;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * This handler handles requests to fetch the plan for an artifact.
 */
public class ArtifactPlanHandler
		extends AbstractRestHandler<RestfulGateway, ArtifactPlanRequestBody, JobPlanInfo, ArtifactPlanMessageParameters> {

	private final Path artifactDir;

	private final Configuration configuration;

	private final Executor executor;

	private final Function<JobGraph, JobPlanInfo> planGenerator;

	public ArtifactPlanHandler(
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<ArtifactPlanRequestBody, JobPlanInfo, ArtifactPlanMessageParameters> messageHeaders,
			final Path artifactDir,
			final Configuration configuration,
			final Executor executor) {
		this(
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			artifactDir,
			configuration,
			executor,
			jobGraph -> new JobPlanInfo(JsonPlanGenerator.generatePlan(jobGraph)));
	}

	public ArtifactPlanHandler(
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<ArtifactPlanRequestBody, JobPlanInfo, ArtifactPlanMessageParameters> messageHeaders,
			final Path artifactDir,
			final Configuration configuration,
			final Executor executor,
			final Function<JobGraph, JobPlanInfo> planGenerator) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.artifactDir = requireNonNull(artifactDir);
		this.configuration = requireNonNull(configuration);
		this.executor = requireNonNull(executor);
		this.planGenerator = planGenerator;
	}

	@Override
	protected CompletableFuture<JobPlanInfo> handleRequest(
			@Nonnull final HandlerRequest<ArtifactPlanRequestBody, ArtifactPlanMessageParameters> request,
			@Nonnull final RestfulGateway gateway) throws RestHandlerException {
		final ArtifactHandlerUtils.ArtifactHandlerContext
			context = ArtifactHandlerContext.fromRequest(request, artifactDir, log);

		return CompletableFuture.supplyAsync(() -> {
			final JobGraph jobGraph = context.toJobGraph(configuration);
			return planGenerator.apply(jobGraph);
		}, executor);
	}
}
