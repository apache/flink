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

package org.apache.flink.runtime.webmonitor.handlers.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.EntryClassQueryParameter;
import org.apache.flink.runtime.webmonitor.handlers.JarIdPathParameter;
import org.apache.flink.runtime.webmonitor.handlers.JarRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.ParallelismQueryParameter;
import org.apache.flink.runtime.webmonitor.handlers.ProgramArgQueryParameter;
import org.apache.flink.runtime.webmonitor.handlers.ProgramArgsQueryParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.fromRequestBodyOrQueryParameter;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.getQueryParameter;
import static org.apache.flink.shaded.guava18.com.google.common.base.Strings.emptyToNull;

/**
 * Utils for jar handlers.
 *
 * @see org.apache.flink.runtime.webmonitor.handlers.JarRunHandler
 * @see org.apache.flink.runtime.webmonitor.handlers.JarPlanHandler
 */
public class JarHandlerUtils {

	/** Standard jar handler parameters parsed from request. */
	public static class JarHandlerContext {
		private final Path jarFile;
		private final String entryClass;
		private final List<String> programArgs;
		private final int parallelism;
		private final JobID jobId;

		private JarHandlerContext(Path jarFile, String entryClass, List<String> programArgs, int parallelism, JobID jobId) {
			this.jarFile = jarFile;
			this.entryClass = entryClass;
			this.programArgs = programArgs;
			this.parallelism = parallelism;
			this.jobId = jobId;
		}

		public static <R extends JarRequestBody> JarHandlerContext fromRequest(
				@Nonnull final HandlerRequest<R, ?> request,
				@Nonnull final Path jarDir,
				@Nonnull final Logger log) throws RestHandlerException {
			final JarRequestBody requestBody = request.getRequestBody();

			final String pathParameter = request.getPathParameter(JarIdPathParameter.class);
			Path jarFile = jarDir.resolve(pathParameter);

			String entryClass = fromRequestBodyOrQueryParameter(
				emptyToNull(requestBody.getEntryClassName()),
				() -> emptyToNull(getQueryParameter(request, EntryClassQueryParameter.class)),
				null,
				log);

			List<String> programArgs = JarHandlerUtils.getProgramArgs(request, log);

			int parallelism = fromRequestBodyOrQueryParameter(
				requestBody.getParallelism(),
				() -> getQueryParameter(request, ParallelismQueryParameter.class),
				ExecutionConfig.PARALLELISM_DEFAULT,
				log);

			JobID jobId = fromRequestBodyOrQueryParameter(
				requestBody.getJobId(),
				() -> null, // No support via query parameter
				null, // Delegate default job ID to actual JobGraph generation
				log);

			return new JarHandlerContext(jarFile, entryClass, programArgs, parallelism, jobId);
		}

		public JobGraph toJobGraph(Configuration configuration) {
			if (!Files.exists(jarFile)) {
				throw new CompletionException(new RestHandlerException(
					String.format("Jar file %s does not exist", jarFile), HttpResponseStatus.BAD_REQUEST));
			}

			try {
				final PackagedProgram packagedProgram = PackagedProgram.newBuilder()
					.setJarFile(jarFile.toFile())
					.setEntryPointClassName(entryClass)
					.setConfiguration(configuration)
					.setArguments(programArgs.toArray(new String[0]))
					.build();
				return PackagedProgramUtils.createJobGraph(packagedProgram, configuration, parallelism, jobId);
			} catch (final ProgramInvocationException e) {
				throw new CompletionException(e);
			}
		}
	}

	/** Parse program arguments in jar run or plan request. */
	private static <R extends JarRequestBody, M extends MessageParameters> List<String> getProgramArgs(
			HandlerRequest<R, M> request, Logger log) throws RestHandlerException {
		JarRequestBody requestBody = request.getRequestBody();
		@SuppressWarnings("deprecation")
		List<String> programArgs = tokenizeArguments(
			fromRequestBodyOrQueryParameter(
				emptyToNull(requestBody.getProgramArguments()),
				() -> getQueryParameter(request, ProgramArgsQueryParameter.class),
				null,
				log));
		List<String> programArgsList =
			fromRequestBodyOrQueryParameter(
				requestBody.getProgramArgumentsList(),
				() -> request.getQueryParameter(ProgramArgQueryParameter.class),
				null,
				log);
		if (!programArgsList.isEmpty()) {
			if (!programArgs.isEmpty()) {
				throw new RestHandlerException(
					"Confusing request: programArgs and programArgsList are specified, please, use only programArgsList",
					HttpResponseStatus.BAD_REQUEST);
			}
			return programArgsList;
		} else {
			return programArgs;
		}
	}

	private static final Pattern ARGUMENTS_TOKENIZE_PATTERN = Pattern.compile("([^\"\']\\S*|\".+?\"|\'.+?\')\\s*");

	/**
	 * Takes program arguments as a single string, and splits them into a list of string.
	 *
	 * <pre>
	 * tokenizeArguments("--foo bar")            = ["--foo" "bar"]
	 * tokenizeArguments("--foo \"bar baz\"")    = ["--foo" "bar baz"]
	 * tokenizeArguments("--foo 'bar baz'")      = ["--foo" "bar baz"]
	 * tokenizeArguments(null)                   = []
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
}
