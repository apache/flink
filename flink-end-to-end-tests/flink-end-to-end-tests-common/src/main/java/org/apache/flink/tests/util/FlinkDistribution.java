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
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExternalResource;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper around a Flink distribution.
 */
public final class FlinkDistribution implements ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkDistribution.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final Path logBackupDir;

	private final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private final Path originalFlinkDir;
	private Path opt;
	private Path lib;
	private Path conf;
	private Path log;
	private Path bin;

	private Configuration defaultConfig;

	public FlinkDistribution() {
		final String distDirProperty = System.getProperty("distDir");
		if (distDirProperty == null) {
			Assert.fail("The distDir property was not set. You can set it when running maven via -DdistDir=<path> .");
		}
		final String backupDirProperty = System.getProperty("logBackupDir");
		logBackupDir = backupDirProperty == null ? null : Paths.get(backupDirProperty);
		originalFlinkDir = Paths.get(distDirProperty);
	}

	@Override
	public void before() throws IOException {
		temporaryFolder.create();

		final Path flinkDir = temporaryFolder.newFolder().toPath();

		LOG.info("Copying distribution to {}.", flinkDir);
		TestUtils.copyDirectory(originalFlinkDir, flinkDir);

		bin = flinkDir.resolve("bin");
		opt = flinkDir.resolve("opt");
		lib = flinkDir.resolve("lib");
		conf = flinkDir.resolve("conf");
		log = flinkDir.resolve("log");

		defaultConfig = new UnmodifiableConfiguration(GlobalConfiguration.loadConfiguration(conf.toAbsolutePath().toString()));
	}

	@Override
	public void afterTestSuccess() {
		try {
			stopFlinkCluster();
		} catch (IOException e) {
			LOG.error("Failure while shutting down Flink cluster.", e);
		}

		temporaryFolder.delete();
	}

	@Override
	public void afterTestFailure() {
		if (logBackupDir != null) {
			final UUID id = UUID.randomUUID();
			LOG.info("Backing up logs to {}/{}.", logBackupDir, id);
			try {
				Files.createDirectories(logBackupDir);
				TestUtils.copyDirectory(log, logBackupDir.resolve(id.toString()));
			} catch (IOException e) {
				LOG.warn("An error occurred while backing up logs.", e);
			}
		}

		afterTestSuccess();
	}

	public void startJobManager() throws IOException {
		LOG.info("Starting Flink JobManager.");
		AutoClosableProcess.runBlocking(bin.resolve("jobmanager.sh").toAbsolutePath().toString(), "start");
	}

	public void startTaskManager() throws IOException {
		LOG.info("Starting Flink TaskManager.");
		AutoClosableProcess.runBlocking(bin.resolve("taskmanager.sh").toAbsolutePath().toString(), "start");
	}

	public void startFlinkCluster() throws IOException {
		LOG.info("Starting Flink cluster.");
		AutoClosableProcess.runBlocking(bin.resolve("start-cluster.sh").toAbsolutePath().toString());

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
		LOG.info("Stopping Flink cluster.");
		AutoClosableProcess.runBlocking(bin.resolve("stop-cluster.sh").toAbsolutePath().toString());
	}

	public JobID submitJob(final JobSubmission jobSubmission) throws IOException {
		final List<String> commands = new ArrayList<>(4);
		commands.add(bin.resolve("flink").toString());
		commands.add("run");
		if (jobSubmission.isDetached()) {
			commands.add("-d");
		}
		if (jobSubmission.getParallelism() > 0) {
			commands.add("-p");
			commands.add(String.valueOf(jobSubmission.getParallelism()));
		}
		commands.add(jobSubmission.getJar().toAbsolutePath().toString());
		commands.addAll(jobSubmission.getArguments());

		LOG.info("Running {}.", commands.stream().collect(Collectors.joining(" ")));

		try (AutoClosableProcess flink = new AutoClosableProcess(new ProcessBuilder()
			.command(commands)
			.start())) {

			final Pattern pattern = jobSubmission.isDetached()
				? Pattern.compile("Job has been submitted with JobID (.*)")
				: Pattern.compile("Job with JobID (.*) has finished.");

			if (jobSubmission.isDetached()) {
				try {
					flink.getProcess().waitFor();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}

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
