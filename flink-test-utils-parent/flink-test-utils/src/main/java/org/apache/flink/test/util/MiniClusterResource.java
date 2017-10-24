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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Preconditions;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a Flink mini cluster as a resource and registers the respective
 * ExecutionEnvironment and StreamExecutionEnvironment.
 */
public class MiniClusterResource extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(MiniClusterResource.class);

	private final MiniClusterResourceConfiguration miniClusterResourceConfiguration;

	private LocalFlinkMiniCluster localFlinkMiniCluster;

	private int numberSlots = -1;

	private TestEnvironment executionEnvironment;

	public MiniClusterResource(final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
		this.miniClusterResourceConfiguration = Preconditions.checkNotNull(miniClusterResourceConfiguration);
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public TestEnvironment getTestEnvironment() {
		return executionEnvironment;
	}

	@Override
	public void before() throws Exception {
		final Configuration configuration = new Configuration(miniClusterResourceConfiguration.getConfiguration());

		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, miniClusterResourceConfiguration.getNumberTaskManagers());
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, miniClusterResourceConfiguration.numberSlotsPerTaskManager);

		localFlinkMiniCluster = TestBaseUtils.startCluster(
			configuration,
			true);

		numberSlots = miniClusterResourceConfiguration.getNumberSlotsPerTaskManager() * miniClusterResourceConfiguration.getNumberTaskManagers();

		executionEnvironment = new TestEnvironment(localFlinkMiniCluster, numberSlots, false);
		executionEnvironment.setAsContext();
		TestStreamEnvironment.setAsContext(localFlinkMiniCluster, numberSlots);
	}

	@Override
	public void after() {
		if (localFlinkMiniCluster != null) {
			try {
				TestBaseUtils.stopCluster(
					localFlinkMiniCluster,
					FutureUtils.toFiniteDuration(miniClusterResourceConfiguration.getShutdownTimeout()));
			} catch (Exception e) {
				LOG.warn("Could not properly shut down the Flink mini cluster.", e);
			}

			TestStreamEnvironment.unsetAsContext();
			TestEnvironment.unsetAsContext();
			localFlinkMiniCluster = null;
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
}
