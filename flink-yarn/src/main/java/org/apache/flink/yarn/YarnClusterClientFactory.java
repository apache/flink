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

package org.apache.flink.yarn;

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ClusterClientFactory} for a YARN cluster.
 */
public class YarnClusterClientFactory implements ClusterClientFactory<ApplicationId> {

	public static final String ID = "yarn-cluster";

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		checkNotNull(configuration);
		return ID.equals(configuration.getString(DeploymentOptions.TARGET));
	}

	@Override
	public YarnClusterDescriptor createClusterDescriptor(Configuration configuration) {
		checkNotNull(configuration);
		return getClusterDescriptor(configuration);
	}

	@Nullable
	@Override
	public ApplicationId getClusterId(Configuration configuration) {
		checkNotNull(configuration);
		final String clusterId = configuration.getString(YarnConfigOptions.APPLICATION_ID);
		return clusterId != null ? ConverterUtils.toApplicationId(clusterId) : null;
	}

	@Override
	public ClusterSpecification getClusterSpecification(Configuration configuration) {
		checkNotNull(configuration);

		// JobManager Memory
		final int jobManagerMemoryMB = ConfigurationUtils.getJobManagerHeapMemory(configuration).getMebiBytes();

		// Task Managers memory
		final int taskManagerMemoryMB = TaskExecutorResourceUtils
			.resourceSpecFromConfig(configuration)
			.getTotalProcessMemorySize()
			.getMebiBytes();

		int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

		return new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(jobManagerMemoryMB)
				.setTaskManagerMemoryMB(taskManagerMemoryMB)
				.setSlotsPerTaskManager(slotsPerTaskManager)
				.createClusterSpecification();
	}

	private YarnClusterDescriptor getClusterDescriptor(Configuration configuration) {
		final YarnClient yarnClient = YarnClient.createYarnClient();
		final YarnConfiguration yarnConfiguration = new YarnConfiguration();

		yarnClient.init(yarnConfiguration);
		yarnClient.start();

		return new YarnClusterDescriptor(
				configuration,
				yarnConfiguration,
				yarnClient,
				false);
	}
}
