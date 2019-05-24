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
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.handlers.utils.ArtifactHandlerUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.fromRequestBodyOrQueryParameter;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.getQueryParameter;
import static org.apache.flink.shaded.guava18.com.google.common.base.Strings.emptyToNull;

/**
 * Handler to submit jobs uploaded via the Web UI.
 */
public class ArtifactRunHandler extends
		AbstractRestHandler<DispatcherGateway, ArtifactRunRequestBody, ArtifactRunResponseBody, ArtifactRunMessageParameters> {

	private final Path artifactDir;

	private final Configuration configuration;

	private final Executor executor;

	public ArtifactRunHandler(
			final GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<ArtifactRunRequestBody, ArtifactRunResponseBody, ArtifactRunMessageParameters> messageHeaders,
			final Path artifactDir,
			final Configuration configuration,
			final Executor executor) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.artifactDir = requireNonNull(artifactDir);
		this.configuration = requireNonNull(configuration);
		this.executor = requireNonNull(executor);
	}

	@Override
	protected CompletableFuture<ArtifactRunResponseBody> handleRequest(
			@Nonnull final HandlerRequest<ArtifactRunRequestBody, ArtifactRunMessageParameters> request,
			@Nonnull final DispatcherGateway gateway) throws RestHandlerException {
		final ArtifactHandlerUtils.ArtifactHandlerContext
			context = ArtifactHandlerUtils.ArtifactHandlerContext.fromRequest(request, artifactDir, log);

		final SavepointRestoreSettings savepointRestoreSettings = getSavepointRestoreSettings(request);

		final CompletableFuture<JobGraph> jobGraphFuture = getJobGraphAsync(context, savepointRestoreSettings);

		CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);

		CompletableFuture<JobGraph> jarUploadFuture = jobGraphFuture.thenCombine(blobServerPortFuture, (jobGraph, blobServerPort) -> {
			final InetSocketAddress address = new InetSocketAddress(gateway.getHostname(), blobServerPort);
			try {
				ClientUtils.extractAndUploadJobGraphFiles(jobGraph, () -> new BlobClient(address, configuration));
			} catch (FlinkException e) {
				throw new CompletionException(e);
			}

			return jobGraph;
		});

		CompletableFuture<Acknowledge> jobSubmissionFuture = jarUploadFuture.thenCompose(jobGraph -> {
			// we have to enable queued scheduling because slots will be allocated lazily
			jobGraph.setAllowQueuedScheduling(true);
			return gateway.submitJob(jobGraph, timeout);
		});

		return jobSubmissionFuture
			.thenCombine(jarUploadFuture, (ack, jobGraph) -> new ArtifactRunResponseBody(jobGraph.getJobID()));
	}

	private SavepointRestoreSettings getSavepointRestoreSettings(
			final @Nonnull HandlerRequest<ArtifactRunRequestBody, ArtifactRunMessageParameters> request)
				throws RestHandlerException {

		final ArtifactRunRequestBody requestBody = request.getRequestBody();

		final boolean allowNonRestoredState = fromRequestBodyOrQueryParameter(
			requestBody.getAllowNonRestoredState(),
			() -> getQueryParameter(request, AllowNonRestoredStateQueryParameter.class),
			false,
			log);
		final String savepointPath = fromRequestBodyOrQueryParameter(
			emptyToNull(requestBody.getSavepointPath()),
			() -> emptyToNull(getQueryParameter(request, SavepointPathQueryParameter.class)),
			null,
			log);
		final SavepointRestoreSettings savepointRestoreSettings;
		if (savepointPath != null) {
			savepointRestoreSettings = SavepointRestoreSettings.forPath(
				savepointPath,
				allowNonRestoredState);
		} else {
			savepointRestoreSettings = SavepointRestoreSettings.none();
		}
		return savepointRestoreSettings;
	}

	private CompletableFuture<JobGraph> getJobGraphAsync(
			ArtifactHandlerUtils.ArtifactHandlerContext context,
			final SavepointRestoreSettings savepointRestoreSettings) {
		return CompletableFuture.supplyAsync(() -> {
			final JobGraph jobGraph = context.toJobGraph(configuration);
			jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);
			return jobGraph;
		}, executor);
	}
}
