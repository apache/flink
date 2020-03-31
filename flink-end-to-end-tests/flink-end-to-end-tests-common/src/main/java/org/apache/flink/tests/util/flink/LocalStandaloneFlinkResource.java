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

import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.util.ConfigurationException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Flink resource that start local standalone clusters.
 */
public class LocalStandaloneFlinkResource implements FlinkResource {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneFlinkResource.class);

	private final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private final Path distributionDirectory;
	@Nullable
	private final Path logBackupDirectory;
	private final FlinkResourceSetup setup;

	private FlinkDistribution distribution;

	LocalStandaloneFlinkResource(Path distributionDirectory, Optional<Path> logBackupDirectory,
	                             FlinkResourceSetup setup) {
		this.distributionDirectory = distributionDirectory;
		this.logBackupDirectory = logBackupDirectory.orElse(null);
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
	}

	@Override
	public ClusterController startCluster(int numTaskManagers) throws IOException {
		distribution.setTaskExecutorHosts(IntStream.range(0, numTaskManagers).mapToObj(i -> "localhost")
			                                  .collect(Collectors.toList()));
		distribution.startFlinkCluster();
		try {
			if (distribution.checkFlinkCluster(numTaskManagers)) {
				return new StandaloneClusterController(distribution);
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
		return distribution.searchAllLogs(pattern, matchProcessor);
	}
}
