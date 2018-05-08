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
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.util.ScalaUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import akka.actor.AddressFromURIString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
		AbstractRestHandler<DispatcherGateway, EmptyRequestBody, JarRunResponseBody, JarRunMessageParameters> {

	private final Path jarDir;

	private final Configuration configuration;

	private final Executor executor;

	public JarRunHandler(
			final CompletableFuture<String> localRestAddress,
			final GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<EmptyRequestBody, JarRunResponseBody, JarRunMessageParameters> messageHeaders,
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
			@Nonnull final HandlerRequest<EmptyRequestBody, JarRunMessageParameters> request,
			@Nonnull final DispatcherGateway gateway) throws RestHandlerException {

		final String pathParameter = request.getPathParameter(JarIdPathParameter.class);
		final Path jarFile = jarDir.resolve(pathParameter);

		final String entryClass = emptyToNull(getQueryParameter(request, EntryClassQueryParameter.class));
		final List<String> programArgs = tokenizeArguments(getQueryParameter(request, ProgramArgsQueryParameter.class));
		final int parallelism = getQueryParameter(request, ParallelismQueryParameter.class, ExecutionConfig.PARALLELISM_DEFAULT);
		final SavepointRestoreSettings savepointRestoreSettings = getSavepointRestoreSettings(request);

		final CompletableFuture<JobGraph> jobGraphFuture = getJobGraphAsync(
			jarFile,
			entryClass,
			programArgs,
			savepointRestoreSettings,
			parallelism);

		CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);

		CompletableFuture<JobGraph> jarUploadFuture = jobGraphFuture.thenCombine(blobServerPortFuture, (jobGraph, blobServerPort) -> {
			final InetSocketAddress address = new InetSocketAddress(getDispatcherHost(gateway), blobServerPort);
			final List<PermanentBlobKey> keys;
			try {
				keys = BlobClient.uploadFiles(address, configuration, jobGraph.getJobID(), jobGraph.getUserJars());
			} catch (IOException ioe) {
				throw new CompletionException(new FlinkException("Could not upload job jar files.", ioe));
			}

			for (PermanentBlobKey key : keys) {
				jobGraph.addUserJarBlobKey(key);
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

	private static SavepointRestoreSettings getSavepointRestoreSettings(
			final @Nonnull HandlerRequest<EmptyRequestBody, JarRunMessageParameters> request)
				throws RestHandlerException {

		final boolean allowNonRestoredState = getQueryParameter(request, AllowNonRestoredStateQueryParameter.class, false);
		final String savepointPath = getQueryParameter(request, SavepointPathQueryParameter.class);
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

	private static String getDispatcherHost(DispatcherGateway gateway) {
		String dispatcherAddress = gateway.getAddress();
		final Optional<String> host = ScalaUtils.toJava(AddressFromURIString.parse(dispatcherAddress).host());

		return host.orElseGet(() -> {
			// if the dispatcher address does not contain a host part, then assume it's running
			// on the same machine as the handler
			return "localhost";
		});
	}
}
