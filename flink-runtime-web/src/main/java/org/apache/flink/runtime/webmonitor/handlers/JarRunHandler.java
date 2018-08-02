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
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.getQueryParameter;
import static org.apache.flink.runtime.webmonitor.handlers.utils.JarHandlerUtils.tokenizeArguments;
import static org.apache.flink.shaded.guava18.com.google.common.base.Strings.emptyToNull;

/**
 * Handler to submit jobs uploaded via the Web UI.
 */
public class JarRunHandler extends
		AbstractRestHandler<DispatcherGateway, JarRunRequestBody, JarRunResponseBody, JarRunMessageParameters> {

	private final Path jarDir;

	private final Configuration configuration;

	private final Executor executor;

	public JarRunHandler(
			final CompletableFuture<String> localRestAddress,
			final GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<JarRunRequestBody, JarRunResponseBody, JarRunMessageParameters> messageHeaders,
			final Path jarDir,
			final Configuration configuration,
			final Executor executor) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.jarDir = requireNonNull(jarDir);
		this.configuration = requireNonNull(configuration);
		this.executor = requireNonNull(executor);
	}

	@Override
	protected CompletableFuture<JarRunResponseBody> handleRequest(
			@Nonnull final HandlerRequest<JarRunRequestBody, JarRunMessageParameters> request,
			@Nonnull final DispatcherGateway gateway) throws RestHandlerException {

		final JarRunRequestBody requestBody = request.getRequestBody();

		final String pathParameter = request.getPathParameter(JarIdPathParameter.class);
		final Path jarFile = jarDir.resolve(pathParameter);

		final String entryClass = fromRequestBodyOrQueryParameter(
			emptyToNull(requestBody.getEntryClassName()),
			() -> emptyToNull(getQueryParameter(request, EntryClassQueryParameter.class)),
			null,
			log);

		final List<String> programArgs = tokenizeArguments(
			fromRequestBodyOrQueryParameter(
				emptyToNull(requestBody.getProgramArguments()),
				() -> getQueryParameter(request, ProgramArgsQueryParameter.class),
				null,
				log));

		final int parallelism = fromRequestBodyOrQueryParameter(
			requestBody.getParallelism(),
			() -> getQueryParameter(request, ParallelismQueryParameter.class),
			ExecutionConfig.PARALLELISM_DEFAULT,
			log);

		final SavepointRestoreSettings savepointRestoreSettings = getSavepointRestoreSettings(request);

		final CompletableFuture<JobGraph> jobGraphFuture = getJobGraphAsync(
			jarFile,
			entryClass,
			programArgs,
			savepointRestoreSettings,
			parallelism);

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
			.thenCombine(jarUploadFuture, (ack, jobGraph) -> new JarRunResponseBody(jobGraph.getJobID()))
			.exceptionally(throwable -> {
				throw new CompletionException(new RestHandlerException(
					throwable.getMessage(),
					HttpResponseStatus.INTERNAL_SERVER_ERROR,
					throwable));
			});
	}

	private SavepointRestoreSettings getSavepointRestoreSettings(
			final @Nonnull HandlerRequest<JarRunRequestBody, JarRunMessageParameters> request)
				throws RestHandlerException {

		final JarRunRequestBody requestBody = request.getRequestBody();

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

	/**
	 * Returns {@code requestValue} if it is not null, otherwise returns the query parameter value
	 * if it is not null, otherwise returns the default value.
	 */
	private static <T> T fromRequestBodyOrQueryParameter(
			T requestValue,
			SupplierWithException<T, RestHandlerException> queryParameterExtractor,
			T defaultValue,
			Logger log) throws RestHandlerException {
		if (requestValue != null) {
			return requestValue;
		} else {
			T queryParameterValue = queryParameterExtractor.get();
			if (queryParameterValue != null) {
				log.warn("Configuring the job submission via query parameters is deprecated." +
					" Please migrate to submitting a JSON request instead.");
				return queryParameterValue;
			} else {
				return defaultValue;
			}
		}
	}

	private CompletableFuture<JobGraph> getJobGraphAsync(
			final Path jarFile,
			@Nullable final String entryClass,
			final List<String> programArgs,
			final SavepointRestoreSettings savepointRestoreSettings,
			final int parallelism) {

		return CompletableFuture.supplyAsync(() -> {
			if (!Files.exists(jarFile)) {
				throw new CompletionException(new RestHandlerException(
					String.format("Jar file %s does not exist", jarFile), HttpResponseStatus.BAD_REQUEST));
			}

			final JobGraph jobGraph;
			try {
				final PackagedProgram packagedProgram = new PackagedProgram(
					jarFile.toFile(),
					entryClass,
					programArgs.toArray(new String[programArgs.size()]));
				jobGraph = PackagedProgramUtils.createJobGraph(packagedProgram, configuration, parallelism);
			} catch (final ProgramInvocationException e) {
				throw new CompletionException(e);
			}
			jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);
			return jobGraph;
		}, executor);
	}
}
