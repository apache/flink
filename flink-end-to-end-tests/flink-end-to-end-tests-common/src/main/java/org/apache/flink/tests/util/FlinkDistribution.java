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

package org.apache.flink.tests.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper around a Flink distribution.
 */
public final class FlinkDistribution extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkDistribution.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final Path FLINK_CONF_YAML = Paths.get("flink-conf.yaml");
	private static final Path FLINK_CONF_YAML_BACKUP = Paths.get("flink-conf.yaml.bak");

	private final List<AutoClosablePath> filesToDelete = new ArrayList<>(4);

	private final Path opt;
	private final Path lib;
	private final Path conf;
	private final Path log;
	private final Path bin;
	private final Path examples;

	private Configuration defaultConfig;

	public FlinkDistribution() {
		final String distDirProperty = System.getProperty("distDir");
		if (distDirProperty == null) {
			Assert.fail("The distDir property was not set. You can set it when running maven via -DdistDir=<path> .");
		}
		final Path flinkDir = Paths.get(distDirProperty);
		bin = flinkDir.resolve("bin");
		opt = flinkDir.resolve("opt");
		lib = flinkDir.resolve("lib");
		conf = flinkDir.resolve("conf");
		log = flinkDir.resolve("log");
		examples = flinkDir.resolve("examples");
	}

	@Override
	protected void before() throws IOException {
		defaultConfig = new UnmodifiableConfiguration(GlobalConfiguration.loadConfiguration(conf.toAbsolutePath().toString()));
		final Path originalConfig = conf.resolve(FLINK_CONF_YAML);
		final Path backupConfig = conf.resolve(FLINK_CONF_YAML_BACKUP);
		Files.copy(originalConfig, backupConfig);
		filesToDelete.add(new AutoClosablePath(backupConfig));
	}

	@Override
	protected void after() {
		try {
			stopFlinkCluster();
		} catch (IOException e) {
			LOG.error("Failure while shutting down Flink cluster.", e);
		}

		final Path originalConfig = conf.resolve(FLINK_CONF_YAML);
		final Path backupConfig = conf.resolve(FLINK_CONF_YAML_BACKUP);

		try {
			Files.move(backupConfig, originalConfig, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			LOG.error("Failed to restore flink-conf.yaml", e);
		}

		for (AutoCloseable fileToDelete : filesToDelete) {
			try {
				fileToDelete.close();
			} catch (Exception e) {
				LOG.error("Failure while cleaning up file.", e);
			}
		}
	}

	public void startFlinkCluster() throws IOException {
		AutoClosableProcess.runBlocking("Start Flink cluster", bin.resolve("start-cluster.sh").toAbsolutePath().toString());

		final OkHttpClient client = new OkHttpClient();

		final Request request = new Request.Builder()
			.get()
			.url("http://localhost:8081/taskmanagers")
			.build();

		Exception reportedException = null;
		for (int retryAttempt = 0; retryAttempt < 30; retryAttempt++) {
			try (Response response = client.newCall(request).execute()) {
				if (response.isSuccessful()) {
					final String json = response.body().string();
					final JsonNode taskManagerList = OBJECT_MAPPER.readTree(json)
						.get("taskmanagers");

					if (taskManagerList != null && taskManagerList.size() > 0) {
						LOG.info("Dispatcher REST endpoint is up.");
						return;
					}
				}
			} catch (IOException ioe) {
				reportedException = ExceptionUtils.firstOrSuppressed(ioe, reportedException);
			}

			LOG.info("Waiting for dispatcher REST endpoint to come up...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				reportedException = ExceptionUtils.firstOrSuppressed(e, reportedException);
			}
		}
		throw new AssertionError("Dispatcher REST endpoint did not start in time.", reportedException);
	}

	public void stopFlinkCluster() throws IOException {
		AutoClosableProcess.runBlocking("Stop Flink Cluster", bin.resolve("stop-cluster.sh").toAbsolutePath().toString());
	}

	public JobRunBuilder prepareExampleJob(final Path relativePath) {
		final Path absolutePath = examples.resolve(relativePath);
		return prepareJob(absolutePath);
	}

	public JobRunBuilder prepareJob(final Path jar) {
		return new JobRunBuilder(bin.resolve("flink"), jar);
	}

	/**
	 * Wrapper around the "run" command of the "flink" executable.
	 */
	public static final class JobRunBuilder {

		private final Path jar;
		private final Path flinkBinary;
		private int parallelism = 0;
		private List<String> arguments = new ArrayList<>(2);
		private boolean detached = false;

		JobRunBuilder(final Path flinkBinary, final Path jar) {
			Preconditions.checkNotNull(flinkBinary);
			Preconditions.checkArgument(flinkBinary.isAbsolute(), "Binary path must be absolute.");
			Preconditions.checkNotNull(jar);
			Preconditions.checkArgument(jar.isAbsolute(), "Jar path must be absolute.");
			this.flinkBinary = flinkBinary;
			this.jar = jar;
		}

		public JobRunBuilder setParallelism(final int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public JobRunBuilder setDetached(final boolean detached) {
			this.detached = detached;
			return this;
		}

		public JobRunBuilder addArgument(final String argument) {
			Preconditions.checkNotNull(argument);
			this.arguments.add(argument);
			return this;
		}

		public JobRunBuilder addArgument(final String key, final String value) {
			Preconditions.checkNotNull(key);
			Preconditions.checkNotNull(value);
			this.arguments.add(key);
			this.arguments.add(value);
			return this;
		}

		public JobID run() throws IOException {
			List<String> commands = new ArrayList<>(4);
			commands.add(flinkBinary.toString());
			commands.add("run");
			if (detached) {
				commands.add("-d");
			}
			if (parallelism > 0) {
				commands.add("-p");
				commands.add(String.valueOf(parallelism));
			}
			commands.add(jar.toAbsolutePath().toString());
			commands.addAll(arguments);

			LOG.info("Running {}.", commands.stream().collect(Collectors.joining(" ")));

			try (AutoClosableProcess flink = new AutoClosableProcess(new ProcessBuilder()
				.command(commands)
				.start())) {

				final Pattern pattern = detached
					? Pattern.compile("Job has been submitted with JobID (.*)")
					: Pattern.compile("Job with JobID (.*) has finished.");
				try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(flink.getProcess().getInputStream(), StandardCharsets.UTF_8))) {
					final Optional<String> jobId = bufferedReader.lines()
						.peek(LOG::info)
						.map(pattern::matcher)
						.filter(Matcher::matches)
						.map(matcher -> matcher.group(1))
						.findAny();
					if (!jobId.isPresent()) {
						throw new IOException("Could not determine Job ID.");
					} else {
						return JobID.fromHexString(jobId.get());
					}
				}
			}
		}
	}

	public void copyOptJarsToLib(String jarNamePrefix) throws FileNotFoundException, IOException {
		final Optional<Path> reporterJarOptional;
		try (Stream<Path> logFiles = Files.walk(opt)) {
			reporterJarOptional = logFiles
				.filter(path -> path.getFileName().toString().startsWith(jarNamePrefix))
				.findFirst();
		}
		if (reporterJarOptional.isPresent()) {
			final Path optReporterJar = reporterJarOptional.get();
			final Path libReporterJar = lib.resolve(optReporterJar.getFileName());
			Files.copy(optReporterJar, libReporterJar);
			filesToDelete.add(new AutoClosablePath(libReporterJar));
		} else {
			throw new FileNotFoundException("No jar could be found matching the pattern " + jarNamePrefix + ".");
		}
	}

	public void appendConfiguration(Configuration config) throws IOException {
		final Configuration mergedConfig = new Configuration();
		mergedConfig.addAll(defaultConfig);
		mergedConfig.addAll(config);

		final List<String> configurationLines = mergedConfig.toMap().entrySet().stream()
			.map(entry -> entry.getKey() + ": " + entry.getValue())
			.collect(Collectors.toList());

		Files.write(conf.resolve("flink-conf.yaml"), configurationLines);
	}

	public Stream<String> searchAllLogs(Pattern pattern, Function<Matcher, String> matchProcessor) throws IOException {
		final List<String> matches = new ArrayList<>(2);

		try (Stream<Path> logFilesStream = Files.list(log)) {
			final Iterator<Path> logFiles = logFilesStream.iterator();
			while (logFiles.hasNext()) {
				final Path logFile = logFiles.next();
				if (!logFile.getFileName().toString().endsWith(".log")) {
					// ignore logs for previous runs that have a number suffix
					continue;
				}
				try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(logFile.toFile()), StandardCharsets.UTF_8))) {
					String line;
					while ((line = br.readLine()) != null) {
						Matcher matcher = pattern.matcher(line);
						if (matcher.matches()) {
							matches.add(matchProcessor.apply(matcher));
						}
					}
				}
			}
		}
		return matches.stream();
	}
}
