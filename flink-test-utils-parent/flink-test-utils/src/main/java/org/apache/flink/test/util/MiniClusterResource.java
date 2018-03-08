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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.minicluster.JobExecutorService;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Starts a Flink mini cluster as a resource and registers the respective
 * ExecutionEnvironment and StreamExecutionEnvironment.
 */
public class MiniClusterResource extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(MiniClusterResource.class);

	private static final String CODEBASE_KEY = "codebase";

	private static final String FLIP6_CODEBASE = "flip6";

	private final MiniClusterResourceConfiguration miniClusterResourceConfiguration;

	private final MiniClusterType miniClusterType;

	private JobExecutorService jobExecutorService;

	private final boolean enableClusterClient;

	private ClusterClient<?> clusterClient;

	private int numberSlots = -1;

	private TestEnvironment executionEnvironment;

	public MiniClusterResource(final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
		this(miniClusterResourceConfiguration, false);
	}

	public MiniClusterResource(
			final MiniClusterResourceConfiguration miniClusterResourceConfiguration,
			final boolean enableClusterClient) {
		this(
			miniClusterResourceConfiguration,
			Objects.equals(FLIP6_CODEBASE, System.getProperty(CODEBASE_KEY)) ? MiniClusterType.FLIP6 : MiniClusterType.OLD,
			enableClusterClient);
	}

	private MiniClusterResource(
			final MiniClusterResourceConfiguration miniClusterResourceConfiguration,
			final MiniClusterType miniClusterType,
			final boolean enableClusterClient) {
		this.miniClusterResourceConfiguration = Preconditions.checkNotNull(miniClusterResourceConfiguration);
		this.miniClusterType = Preconditions.checkNotNull(miniClusterType);
		this.enableClusterClient = enableClusterClient;
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public ClusterClient<?> getClusterClient() {
		if (!enableClusterClient) {
			// this check is technically only necessary for legacy clusters
			// we still fail here for flip6 to keep the behaviors in sync
			throw new IllegalStateException("To use the client you must enable it with the constructor.");
		}

		return clusterClient;
	}

	public TestEnvironment getTestEnvironment() {
		return executionEnvironment;
	}

	@Override
	public void before() throws Exception {

		startJobExecutorService(miniClusterType);

		numberSlots = miniClusterResourceConfiguration.getNumberSlotsPerTaskManager() * miniClusterResourceConfiguration.getNumberTaskManagers();

		executionEnvironment = new TestEnvironment(jobExecutorService, numberSlots, false);
		executionEnvironment.setAsContext();
		TestStreamEnvironment.setAsContext(jobExecutorService, numberSlots);
	}

	@Override
	public void after() {

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

		final CompletableFuture<?> terminationFuture = jobExecutorService.closeAsync();

		try {
			terminationFuture.get(
				miniClusterResourceConfiguration.getShutdownTimeout().toMilliseconds(),
				TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		jobExecutorService = null;

		if (exception != null) {
			LOG.warn("Could not properly shut down the MiniClusterResource.", exception);
		}
	}

	private void startJobExecutorService(MiniClusterType miniClusterType) throws Exception {
		switch (miniClusterType) {
			case OLD:
				startOldMiniCluster();
				break;
			case FLIP6:
				startFlip6MiniCluster();
				break;
			default:
				throw new FlinkRuntimeException("Unknown MiniClusterType "  + miniClusterType + '.');
		}
	}

	private void startOldMiniCluster() throws Exception {
		final Configuration configuration = new Configuration(miniClusterResourceConfiguration.getConfiguration());
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, miniClusterResourceConfiguration.getNumberTaskManagers());
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, miniClusterResourceConfiguration.getNumberSlotsPerTaskManager());

		final LocalFlinkMiniCluster flinkMiniCluster = TestBaseUtils.startCluster(
			configuration,
			!enableClusterClient); // the cluster client only works if separate actor systems are used

		jobExecutorService = flinkMiniCluster;
		if (enableClusterClient) {
			clusterClient = new StandaloneClusterClient(configuration, flinkMiniCluster.highAvailabilityServices(), true);
		}
	}

	private void startFlip6MiniCluster() throws Exception {
		final Configuration configuration = miniClusterResourceConfiguration.getConfiguration();

		// we need to set this since a lot of test expect this because TestBaseUtils.startCluster()
		// enabled this by default
		configuration.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);

		// set rest port to 0 to avoid clashes with concurrent MiniClusters
		configuration.setInteger(RestOptions.REST_PORT, 0);

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(miniClusterResourceConfiguration.getNumberTaskManagers())
			.setNumSlotsPerTaskManager(miniClusterResourceConfiguration.getNumberSlotsPerTaskManager())
			.build();

		final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);

		miniCluster.start();

		// update the port of the rest endpoint
		configuration.setInteger(RestOptions.REST_PORT, miniCluster.getRestAddress().getPort());

		jobExecutorService = miniCluster;
		if (enableClusterClient) {
			clusterClient = new MiniClusterClient(configuration, miniCluster);
		}
	}

	/**
	 * Mini cluster resource configuration object.
	 */
	public static class MiniClusterResourceConfiguration {
		private final Configuration configuration;

		private final int numberTaskManagers;

		private final int numberSlotsPerTaskManager;

		private final Time shutdownTimeout;

		public MiniClusterResourceConfiguration(
				Configuration configuration,
				int numberTaskManagers,
				int numberSlotsPerTaskManager) {
			this(
				configuration,
				numberTaskManagers,
				numberSlotsPerTaskManager,
				AkkaUtils.getTimeoutAsTime(configuration));
		}

		public MiniClusterResourceConfiguration(
				Configuration configuration,
				int numberTaskManagers,
				int numberSlotsPerTaskManager,
				Time shutdownTimeout) {
			this.configuration = Preconditions.checkNotNull(configuration);
			this.numberTaskManagers = numberTaskManagers;
			this.numberSlotsPerTaskManager = numberSlotsPerTaskManager;
			this.shutdownTimeout = Preconditions.checkNotNull(shutdownTimeout);
		}

		public Configuration getConfiguration() {
			return configuration;
		}

		public int getNumberTaskManagers() {
			return numberTaskManagers;
		}

		public int getNumberSlotsPerTaskManager() {
			return numberSlotsPerTaskManager;
		}

		public Time getShutdownTimeout() {
			return shutdownTimeout;
		}
	}

	// ---------------------------------------------
	// Enum definitions
	// ---------------------------------------------

	/**
	 * Type of the mini cluster to start.
	 */
	public enum MiniClusterType {
		OLD,
		FLIP6
	}
}
