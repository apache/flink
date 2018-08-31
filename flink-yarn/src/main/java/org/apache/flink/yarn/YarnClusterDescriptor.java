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

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Implementation of {@link AbstractYarnClusterDescriptor} which is used to start the
 * application master.
 */
public class YarnClusterDescriptor extends AbstractYarnClusterDescriptor {

	public YarnClusterDescriptor(
			Configuration flinkConfiguration,
			YarnConfiguration yarnConfiguration,
			String configurationDirectory,
			YarnClient yarnClient,
			boolean sharedYarnClient) {
		super(
			flinkConfiguration,
			yarnConfiguration,
			configurationDirectory,
			yarnClient,
			sharedYarnClient);
	}

	@Override
	protected String getYarnSessionClusterEntrypoint() {
		return YarnSessionClusterEntrypoint.class.getName();
	}

	@Override
	protected String getYarnJobClusterEntrypoint() {
		return YarnJobClusterEntrypoint.class.getName();
	}

	@Override
	public ClusterClient<ApplicationId> deployJobCluster(
		ClusterSpecification clusterSpecification,
		JobGraph jobGraph,
		boolean detached) throws ClusterDeploymentException {

		// this is required because the slots are allocated lazily
		jobGraph.setAllowQueuedScheduling(true);

		try {
			return deployInternal(
				clusterSpecification,
				"Flink per-job cluster",
				getYarnJobClusterEntrypoint(),
				jobGraph,
				detached);
		} catch (Exception e) {
			throw new ClusterDeploymentException("Could not deploy Yarn job cluster.", e);
		}
	}

	@Override
	protected ClusterClient<ApplicationId> createYarnClusterClient(
			AbstractYarnClusterDescriptor descriptor,
			int numberTaskManagers,
			int slotsPerTaskManager,
			ApplicationReport report,
			Configuration flinkConfiguration,
			boolean perJobCluster) throws Exception {
		return new RestClusterClient<>(
			flinkConfiguration,
			report.getApplicationId());
	}
}
