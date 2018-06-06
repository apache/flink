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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.webmonitor.handlers.utils.JarHandlerUtils.tokenizeArguments;
import static org.apache.flink.shaded.guava18.com.google.common.base.Strings.emptyToNull;

/**
 * This handler handles requests to fetch the plan for a jar.
 */
public class JarPlanHandler
		extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, JobPlanInfo, JarPlanMessageParameters> {

	private final Path jarDir;

	private final Configuration configuration;

	private final Executor executor;

	public JarPlanHandler(
			final CompletableFuture<String> localRestAddress,
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<EmptyRequestBody, JobPlanInfo, JarPlanMessageParameters> messageHeaders,
			final Path jarDir,
			final Configuration configuration,
			final Executor executor) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.jarDir = requireNonNull(jarDir);
		this.configuration = requireNonNull(configuration);
		this.executor = requireNonNull(executor);
	}

	@Override
	protected CompletableFuture<JobPlanInfo> handleRequest(
			@Nonnull final HandlerRequest<EmptyRequestBody, JarPlanMessageParameters> request,
			@Nonnull final RestfulGateway gateway) throws RestHandlerException {

		final String jarId = request.getPathParameter(JarIdPathParameter.class);
		final String entryClass = emptyToNull(HandlerRequestUtils.getQueryParameter(request, EntryClassQueryParameter.class));
		final Integer parallelism = HandlerRequestUtils.getQueryParameter(request, ParallelismQueryParameter.class, ExecutionConfig.PARALLELISM_DEFAULT);
		final List<String> programArgs = tokenizeArguments(HandlerRequestUtils.getQueryParameter(request, ProgramArgsQueryParameter.class));
		final Path jarFile = jarDir.resolve(jarId);

		return CompletableFuture.supplyAsync(() -> {
			final JobGraph jobGraph;
			try {
				final PackagedProgram packagedProgram = new PackagedProgram(
					jarFile.toFile(),
					entryClass,
					programArgs.toArray(new String[programArgs.size()]));
				jobGraph = PackagedProgramUtils.createJobGraph(packagedProgram, configuration, parallelism);
			} catch (final ProgramInvocationException e) {
				throw new CompletionException(new RestHandlerException(
					e.getMessage(),
					HttpResponseStatus.INTERNAL_SERVER_ERROR,
					e));
			}
			return new JobPlanInfo(JsonPlanGenerator.generatePlan(jobGraph));
		}, executor);
	}
}
