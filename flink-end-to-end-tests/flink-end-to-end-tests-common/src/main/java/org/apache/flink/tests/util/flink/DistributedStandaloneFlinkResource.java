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

package org.apache.flink.tests.util.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.ssh.SshTool;
import org.apache.flink.util.ConfigurationException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Flink resource that start distributed standalone clusters.
 */
public class DistributedStandaloneFlinkResource implements FlinkResource {

	private static final Logger LOG = LoggerFactory.getLogger(DistributedStandaloneFlinkResource.class);

	private final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private final Path distributionDirectory;
	@Nullable
	private final Path logBackupDirectory;
	private final List<String> jmHosts;
	private final List<String> tmHosts;
	private final SshTool sshTool;
	private final FlinkResourceSetup setup;

	private FlinkDistribution distribution;
	private Collection<RemoteFlinkDistribution> remoteDistributions;

	public DistributedStandaloneFlinkResource(Path distributionDirectory, Optional<Path> logBackupDirectory,
	                                          List<String> jmHosts, List<String> tmHosts, SshTool sshTool,
	                                          FlinkResourceSetup setup) {
		this.distributionDirectory = distributionDirectory;
		this.logBackupDirectory = logBackupDirectory.orElse(null);
		this.jmHosts = jmHosts;
		this.tmHosts = tmHosts;
		this.sshTool = sshTool;
		this.setup = setup;
	}

	@Override
	public void before() throws Exception {
		temporaryFolder.create();
		Path tmp = temporaryFolder.newFolder().toPath();
		LOG.info("Copying distribution to {}.", tmp);
		TestUtils.copyDirectory(distributionDirectory, tmp);

		distribution = new FlinkDistribution(tmp);
		for (JarMove jarMove : setup.getJarMoveOperations()) {
			distribution.moveJar(jarMove);
		}
		if (setup.getConfig().isPresent()) {
			distribution.appendConfiguration(setup.getConfig().get());
		}

		remoteDistributions = new ArrayList<>();
		for (String taskExecutorHost : tmHosts) {
			// TODO: decide on remote path
			Path remoteDistributionDir = null;
			sshTool.copyDirectoryTo(tmp, remoteDistributionDir, taskExecutorHost);
			remoteDistributions.add(new RemoteFlinkDistribution(taskExecutorHost, remoteDistributionDir, sshTool));
		}
	}

	@Override
	public void afterTestSuccess() {
		shutdownCluster();
		temporaryFolder.delete();
	}

	@Override
	public void afterTestFailure() {
		if (distribution != null) {
			shutdownCluster();
			if (logBackupDirectory != null) {
				final Path targetDirectory = logBackupDirectory.resolve(UUID.randomUUID().toString());
				try {
					distribution.copyLogsTo(targetDirectory);
					LOG.info("Backed up logs to {}.", targetDirectory);
				} catch (IOException e) {
					LOG.warn("An error has occurred while backing up logs to {}.", targetDirectory, e);
				}
				for (RemoteFlinkDistribution remoteDistribution : remoteDistributions) {
					try {
						remoteDistribution.copyLogsTo(targetDirectory);
						LOG.info("Backed up logs from host {} to {}.", remoteDistribution.getHost(), targetDirectory);
					} catch (IOException e) {
						LOG.warn("An error has occurred while backing up logs from host {} to {}.",
							remoteDistribution.getHost(), targetDirectory, e);
					}
				}
			}
		}
		temporaryFolder.delete();
	}

	private void shutdownCluster() {
		try {
			distribution.stopFlinkCluster();
		} catch (IOException e) {
			LOG.warn("Error while shutting down Flink cluster.", e);
		}
		for (RemoteFlinkDistribution remoteDistribution : remoteDistributions) {
			try {
				remoteDistribution.stopFlinkCluster();
			} catch (IOException e) {
				LOG.warn("Error while shutting down Flink cluster on host {}.", remoteDistribution.getHost(), e);
			}
		}
	}

	@Override
	// TODO: modify this method also also accept number of job masters
	public ClusterController startCluster(int numTaskManagers) throws IOException {
		// TODO: set TM hosts according to required number of task executors and configured hosts
		// TODO: may imply duplicating some entries (if numHosts < numTaskManagers> or ignoring others (if numHosts >
		//  numTaskManagers>
		//distribution.setTaskExecutorHosts(IntStream.range(0, numTaskManagers).mapToObj(i -> "localhost").collect
		// (Collectors.toList()));
		distribution.startFlinkCluster();

		// TODO: deduplicate with LocalStandaloneResource#startCluster
		try (final RestClient restClient = new RestClient(
			RestClientConfiguration.fromConfiguration(new Configuration()), Executors.directExecutor())) {
			for (int retryAttempt = 0; retryAttempt < 30; retryAttempt++) {
				final CompletableFuture<TaskManagersInfo> localhost = restClient.sendRequest(
					// TODO: either ensure that at least one JM runs on the local machine, or use one of the
					//  configured JM hosts
					"localhost",
					8081,
					TaskManagersHeaders.getInstance(),
					EmptyMessageParameters.getInstance(),
					EmptyRequestBody.getInstance());

				try {
					final TaskManagersInfo taskManagersInfo = localhost.get(1, TimeUnit.SECONDS);

					final int numRunningTaskManagers = taskManagersInfo.getTaskManagerInfos().size();
					if (numRunningTaskManagers == numTaskManagers) {
						return new StandaloneClusterController(distribution);
					} else {
						LOG.info("Waiting for task managers to come up. {}/{} are currently running.",
							numRunningTaskManagers, numTaskManagers);
					}
				} catch (InterruptedException e) {
					LOG.info("Waiting for dispatcher REST endpoint to come up...");
					Thread.currentThread().interrupt();
				} catch (TimeoutException | ExecutionException e) {
					// ExecutionExceptions may occur if leader election is still going on
					LOG.info("Waiting for dispatcher REST endpoint to come up...");
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		} catch (ConfigurationException e) {
			throw new RuntimeException("Could not create RestClient.", e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		throw new RuntimeException("Cluster did not start in expected time-frame.");
	}

	@Override
	public Stream<String> searchAllLogs(Pattern pattern, Function<Matcher, String> matchProcessor) throws IOException {
		Stream<String> logStream = distribution.searchAllLogs(pattern, matchProcessor);
		for (RemoteFlinkDistribution remoteDistribution : remoteDistributions) {
			try {
				logStream = Stream.concat(logStream, remoteDistribution.searchAllLogs(pattern, matchProcessor));
			} catch (IOException e) {
				LOG.warn("Error while shutting down Flink cluster on host {}.", remoteDistribution.getHost(), e);
			}
		}
		return logStream;
	}
}
