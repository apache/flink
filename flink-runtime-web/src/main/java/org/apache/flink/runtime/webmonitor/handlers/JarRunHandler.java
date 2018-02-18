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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.shaded.guava18.com.google.common.base.Strings.emptyToNull;

/**
 * Handler to submit jobs uploaded via the Web UI.
 */
public class JarRunHandler extends
		AbstractRestHandler<RestfulGateway, EmptyRequestBody, JarRunResponseBody, JarRunMessageParameters> {

	private static final Pattern ARGUMENTS_TOKENIZE_PATTERN = Pattern.compile("([^\"\']\\S*|\".+?\"|\'.+?\')\\s*");

	private final Path jarDir;

	private final Configuration configuration;

	private final Executor executor;

	private final RestClusterClient<?> restClusterClient;

	public JarRunHandler(
			final CompletableFuture<String> localRestAddress,
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<EmptyRequestBody, JarRunResponseBody, JarRunMessageParameters> messageHeaders,
			final Path jarDir,
			final Configuration configuration,
			final Executor executor,
			final RestClusterClient<?> restClusterClient) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.jarDir = requireNonNull(jarDir);
		this.configuration = requireNonNull(configuration);
		this.executor = requireNonNull(executor);
		this.restClusterClient = requireNonNull(restClusterClient);
	}

	@Override
	protected CompletableFuture<JarRunResponseBody> handleRequest(
			@Nonnull final HandlerRequest<EmptyRequestBody, JarRunMessageParameters> request,
			@Nonnull final RestfulGateway gateway) throws RestHandlerException {

		final String pathParameter = request.getPathParameter(JarIdPathParameter.class);
		final Path jarFile = jarDir.resolve(pathParameter);

		final String entryClass = emptyToNull(getQueryParameter(request, EntryClassQueryParameter.class));
		final List<String> programArgs = tokenizeArguments(getQueryParameter(request, ProgramArgsQueryParameter.class));
		final int parallelism = getQueryParameter(request, ParallelismQueryParameter.class, -1);
		final SavepointRestoreSettings savepointRestoreSettings = getSavepointRestoreSettings(request);

		final CompletableFuture<JobGraph> jobGraphFuture = getJobGraphAsync(
			jarFile,
			entryClass,
			programArgs,
			savepointRestoreSettings,
			parallelism);

		return jobGraphFuture.thenCompose(jobGraph -> restClusterClient
			.submitJob(jobGraph)
			.thenApply((jobSubmitResponseBody -> new JarRunResponseBody(jobGraph.getJobID()))))
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

	/**
	 * Takes program arguments as a single string, and splits them into a list of string.
	 *
	 * <pre>
	 * tokenizeArguments("--foo bar")            = ["--foo" "bar"]
	 * tokenizeArguments("--foo \"bar baz\"")    = ["--foo" "bar baz"]
	 * tokenizeArguments("--foo 'bar baz'")      = ["--foo" "bar baz"]
	 * </pre>
	 *
	 * <strong>WARNING: </strong>This method does not respect escaped quotes.
	 */
	@VisibleForTesting
	static List<String> tokenizeArguments(@Nullable final String args) {
		if (args == null) {
			return Collections.emptyList();
		}
		final Matcher matcher = ARGUMENTS_TOKENIZE_PATTERN.matcher(args);
		final List<String> tokens = new ArrayList<>();
		while (matcher.find()) {
			tokens.add(matcher.group()
				.trim()
				.replace("\"", "")
				.replace("\'", ""));
		}
		return tokens;
	}

	/**
	 * Returns the value of a query parameter, or {@code null} if the query parameter is not set.
	 * @throws RestHandlerException If the query parameter is repeated.
	 */
	@VisibleForTesting
	static <X, P extends MessageQueryParameter<X>> X getQueryParameter(
			final HandlerRequest<EmptyRequestBody, JarRunMessageParameters> request,
			final Class<P> queryParameterClass) throws RestHandlerException {
		return getQueryParameter(request, queryParameterClass, null);
	}

	@VisibleForTesting
	static <X, P extends MessageQueryParameter<X>> X getQueryParameter(
			final HandlerRequest<EmptyRequestBody, JarRunMessageParameters> request,
			final Class<P> queryParameterClass,
			final X defaultValue) throws RestHandlerException {

		final List<X> values = request.getQueryParameter(queryParameterClass);
		final X value;
		if (values.size() > 1) {
			throw new RestHandlerException(
				String.format("Expected only one value %s.", values),
				HttpResponseStatus.BAD_REQUEST);
		} else if (values.size() == 1) {
			value = values.get(0);
		} else {
			value = defaultValue;
		}
		return value;
	}
}
