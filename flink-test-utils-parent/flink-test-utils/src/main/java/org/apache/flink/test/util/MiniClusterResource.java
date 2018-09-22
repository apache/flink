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

package org.apache.flink.test.util;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.minicluster.JobExecutorService;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Starts a Flink mini cluster as a resource and registers the respective
 * ExecutionEnvironment and StreamExecutionEnvironment.
 */
public class MiniClusterResource extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(MiniClusterResource.class);

	private final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private final MiniClusterResourceConfiguration miniClusterResourceConfiguration;

	private JobExecutorService jobExecutorService;

	private ClusterClient<?> clusterClient;

	private Configuration restClusterClientConfig;

	private int numberSlots = -1;

	private TestEnvironment executionEnvironment;

	private int webUIPort = -1;

	public MiniClusterResource(final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
		this.miniClusterResourceConfiguration = Preconditions.checkNotNull(miniClusterResourceConfiguration);
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public ClusterClient<?> getClusterClient() {
		return clusterClient;
	}

	public Configuration getClientConfiguration() {
		return restClusterClientConfig;
	}

	public TestEnvironment getTestEnvironment() {
		return executionEnvironment;
	}

	public int getWebUIPort() {
		return webUIPort;
	}

	@Override
	public void before() throws Exception {
		temporaryFolder.create();

		startMiniCluster();

		numberSlots = miniClusterResourceConfiguration.getNumberSlotsPerTaskManager() * miniClusterResourceConfiguration.getNumberTaskManagers();

		executionEnvironment = new TestEnvironment(jobExecutorService, numberSlots, false);
		executionEnvironment.setAsContext();
		TestStreamEnvironment.setAsContext(jobExecutorService, numberSlots);
	}

	@Override
	public void after() {
		temporaryFolder.delete();

		TestStreamEnvironment.unsetAsContext();
		TestEnvironment.unsetAsContext();

		Exception exception = null;

		if (clusterClient != null) {
			try {
				clusterClient.shutdown();
			} catch (Exception e) {
				exception = e;
			}
		}

		clusterClient = null;

		if (jobExecutorService != null) {
			final CompletableFuture<?> terminationFuture = jobExecutorService.closeAsync();

			try {
				terminationFuture.get(
					miniClusterResourceConfiguration.getShutdownTimeout().toMilliseconds(),
					TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			jobExecutorService = null;
		}

		if (exception != null) {
			LOG.warn("Could not properly shut down the MiniClusterResource.", exception);
		}
	}

	private void startMiniCluster() throws Exception {
		final Configuration configuration = miniClusterResourceConfiguration.getConfiguration();
		configuration.setString(CoreOptions.TMP_DIRS, temporaryFolder.newFolder().getAbsolutePath());

		// we need to set this since a lot of test expect this because TestBaseUtils.startCluster()
		// enabled this by default
		if (!configuration.contains(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE)) {
			configuration.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
		}

		if (!configuration.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE)) {
			configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, TestBaseUtils.TASK_MANAGER_MEMORY_SIZE);
		}

		// set rest port to 0 to avoid clashes with concurrent MiniClusters
		configuration.setInteger(RestOptions.PORT, 0);

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(miniClusterResourceConfiguration.getNumberTaskManagers())
			.setNumSlotsPerTaskManager(miniClusterResourceConfiguration.getNumberSlotsPerTaskManager())
			.build();

		final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);

		miniCluster.start();

		// update the port of the rest endpoint
		configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().getPort());

		jobExecutorService = miniCluster;
		clusterClient = new MiniClusterClient(configuration, miniCluster);

		Configuration restClientConfig = new Configuration();
		restClientConfig.setString(JobManagerOptions.ADDRESS, miniCluster.getRestAddress().getHost());
		restClientConfig.setInteger(RestOptions.PORT, miniCluster.getRestAddress().getPort());
		this.restClusterClientConfig = new UnmodifiableConfiguration(restClientConfig);

		webUIPort = miniCluster.getRestAddress().getPort();
	}
}
