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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactIdPathParameter;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.EntryClassQueryParameter;
import org.apache.flink.runtime.webmonitor.handlers.ParallelismQueryParameter;
import org.apache.flink.runtime.webmonitor.handlers.ProgramArgQueryParameter;
import org.apache.flink.runtime.webmonitor.handlers.ProgramArgsQueryParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.fromRequestBodyOrQueryParameter;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.getQueryParameter;
import static org.apache.flink.shaded.guava18.com.google.common.base.Strings.emptyToNull;

/**
 * Utils for artifact handlers.
 *
 * @see org.apache.flink.runtime.webmonitor.handlers.ArtifactRunHandler
 * @see org.apache.flink.runtime.webmonitor.handlers.ArtifactPlanHandler
 */
public class ArtifactHandlerUtils {

	/** Standard artifact handler parameters parsed from request. */
	public static class ArtifactHandlerContext {
		private final Path artifactFile;
		private final String entryClass;
		private final List<String> programArgs;
		private final int parallelism;
		private final JobID jobId;
		private File dependentJarFile;

		private ArtifactHandlerContext(Path artifactFile, String entryClass, File dependentJarFile, List<String> programArgs, int parallelism, JobID jobId) {
			this.artifactFile = artifactFile;
			this.entryClass = entryClass;
			this.dependentJarFile = dependentJarFile;
			this.programArgs = programArgs;
			this.parallelism = parallelism;
			this.jobId = jobId;
		}

		public static <R extends ArtifactRequestBody> ArtifactHandlerContext fromRequest(
				@Nonnull final HandlerRequest<R, ?> request,
				@Nonnull final Path artifactDir,
				@Nonnull final Logger log) throws RestHandlerException {
			final ArtifactRequestBody requestBody = request.getRequestBody();

			final String pathParameter = request.getPathParameter(ArtifactIdPathParameter.class);
			Path artifactFile = artifactDir.resolve(pathParameter);

			String entryClass = fromRequestBodyOrQueryParameter(
				emptyToNull(requestBody.getEntryClassName()),
				() -> emptyToNull(getQueryParameter(request, EntryClassQueryParameter.class)),
				null,
				log);

			List<String> programArgs = ArtifactHandlerUtils.getProgramArgs(request, log);

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

			File dependentJarFile = null;
			final String artifactFileName = artifactFile.getFileName().toString();
			if (artifactFileName.endsWith(".py") ||
				artifactFileName.endsWith(".zip") ||
				artifactFileName.endsWith(".egg")) {
				final List<String> list = new ArrayList<>();
				if (artifactFileName.endsWith(".py")) {
					list.add("-py");
					list.add(artifactFile.toFile().getAbsolutePath());
				} else {
					if (entryClass == null) {
						throw new CompletionException(new RestHandlerException(
							"Entry class is missing", HttpResponseStatus.BAD_REQUEST));
					}

					list.add("-pym");
					list.add(entryClass);
					list.add("-pyfs");

					final StringBuilder pyFiles = new StringBuilder();
					pyFiles.append(artifactFile.toFile().getAbsolutePath());
					// extract the contained libraries under directory lib of the zip file
					if (artifactFileName.endsWith(".zip")) {
						Tuple2<File, List<File>> containedLibraries =
							extractContainedLibraries(artifactFile.toFile().getAbsoluteFile().toURI());
						dependentJarFile = containedLibraries.f0;

						if (!containedLibraries.f1.isEmpty()) {
							pyFiles.append(",");
							pyFiles.append(containedLibraries.f1.stream()
								.map(File::getAbsolutePath).collect(Collectors.joining(",")));
						}
					}
					list.add(pyFiles.toString());
				}
				list.addAll(programArgs);

				// sets the entry class to PythonDriver for Python jobs
				entryClass = "org.apache.flink.python.client.PythonDriver";
				programArgs = list;
			}

			return new ArtifactHandlerContext(artifactFile, entryClass, dependentJarFile, programArgs, parallelism, jobId);
		}

		public JobGraph toJobGraph(Configuration configuration) {
			if (!Files.exists(artifactFile)) {
				throw new CompletionException(new RestHandlerException(
					String.format("Artifact file %s does not exist", artifactFile), HttpResponseStatus.BAD_REQUEST));
			}

			File jarFile = null;
			if (artifactFile.getFileName().toString().endsWith(".jar")) {
				// the artifact is Java
				jarFile = artifactFile.toFile();
			} else if (dependentJarFile != null) {
				// the artifact is Python
				jarFile = dependentJarFile;
			}

			try {
				final PackagedProgram packagedProgram = new PackagedProgram(
					jarFile,
					entryClass,
					programArgs.toArray(new String[0]));
				return PackagedProgramUtils.createJobGraph(packagedProgram, configuration, parallelism, jobId);
			} catch (final ProgramInvocationException e) {
				throw new CompletionException(e);
			}
		}
	}

	/** Parse program arguments in artifact run or plan request. */
	private static <R extends ArtifactRequestBody, M extends MessageParameters> List<String> getProgramArgs(
			HandlerRequest<R, M> request, Logger log) throws RestHandlerException {
		ArtifactRequestBody requestBody = request.getRequestBody();
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

	@VisibleForTesting
	protected static Tuple2<File, List<File>> extractContainedLibraries(URI artifactFile) {
		try (ZipFile zipFile = new ZipFile(new File(artifactFile))) {
			ZipEntry containedJarFileEntry = null;
			final List<ZipEntry> containedPythonFileEntries = new ArrayList<>();

			Enumeration<? extends ZipEntry> entries = zipFile.entries();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				String name = entry.getName();

				if (name.length() > 8 && name.startsWith("lib/") && name.endsWith(".jar")) {
					if (containedJarFileEntry == null) {
						containedJarFileEntry = entry;
					} else {
						throw new RuntimeException(
							String.format("Artifact file %s contains multiple jar files", artifactFile));
					}
				}

				if (name.length() > 7 && name.startsWith("lib/") &&
					(name.endsWith(".py") || name.endsWith(".zip") || name.endsWith(".egg"))) {
					containedPythonFileEntries.add(entry);
				}
			}

			File extractedTempJar = null;
			final List<File> extractedTempLibraries = new ArrayList<>(containedPythonFileEntries.size());
			boolean incomplete = true;
			try {
				if (containedJarFileEntry != null) {
					extractedTempJar = extractZipEntry(zipFile, containedJarFileEntry);
				}

				for (ZipEntry containedPythonFileEntry : containedPythonFileEntries) {
					extractedTempLibraries.add(extractZipEntry(zipFile, containedPythonFileEntry));
				}

				incomplete = false;
			} finally {
				if (incomplete) {
					if (extractedTempJar != null) {
						extractedTempJar.delete();
					}
					for (File f : extractedTempLibraries) {
						f.delete();
					}
				}
			}

			return Tuple2.of(extractedTempJar, extractedTempLibraries);
		} catch (Throwable t) {
			throw new CompletionException(new RestHandlerException(
				"Unknown I/O error while extracting contained library files.",
				HttpResponseStatus.BAD_REQUEST,
				t));
		}
	}

	private static File extractZipEntry(final ZipFile zipFile, final ZipEntry zipEntry) {
		final Random rnd = new Random();
		final byte[] buffer = new byte[4096];
		String name = zipEntry.getName();
		// '/' as in case of zip, jar
		// java.util.zip.ZipEntry#isDirectory always looks only for '/' not for File.separator
		name = name.replace('/', '_');

		File tempFile;
		try {
			tempFile = File.createTempFile(rnd.nextInt(Integer.MAX_VALUE) + "_", name);
			tempFile.deleteOnExit();
		} catch (IOException e) {
			throw new RuntimeException(
				String.format("An I/O error occurred while creating temporary file to extract nested " +
					"library '%s'.", zipEntry.getName()), e);
		}

		// copy the temp file contents to a temporary File
		try (
			OutputStream out = new FileOutputStream(tempFile);
			InputStream in = new BufferedInputStream(zipFile.getInputStream(zipEntry))) {

			int numRead;
			while ((numRead = in.read(buffer)) != -1) {
				out.write(buffer, 0, numRead);
			}
		} catch (IOException e) {
			throw new RuntimeException(
				String.format(
					"An I/O error occurred while extracting nested library '%s' to " +
					"temporary file '%s'.", zipEntry.getName(), tempFile.getAbsolutePath()), e);
		}

		return tempFile;
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
