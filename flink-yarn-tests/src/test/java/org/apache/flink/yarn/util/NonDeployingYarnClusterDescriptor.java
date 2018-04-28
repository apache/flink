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

package org.apache.flink.yarn.util;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Dummy {@link AbstractYarnClusterDescriptor} without an actual deployment for tests.
 */
public class NonDeployingYarnClusterDescriptor extends AbstractYarnClusterDescriptor {

	private final ClusterClient<ApplicationId> clusterClient;

	public NonDeployingYarnClusterDescriptor(
			Configuration flinkConfiguration,
			YarnConfiguration yarnConfiguration,
			String configurationDirectory,
			YarnClient yarnClient,
			ClusterClient<ApplicationId> clusterClient) {
		super(flinkConfiguration, yarnConfiguration, configurationDirectory, yarnClient, true);

		//noinspection unchecked
		this.clusterClient = Preconditions.checkNotNull(clusterClient);
	}

	@Override
	public String getClusterDescription() {
		// return parent.getClusterDescription();
		return "NonDeployingYarnClusterDescriptor";
	}

	@Override
	protected ClusterClient<ApplicationId> createYarnClusterClient(
			AbstractYarnClusterDescriptor descriptor,
			int numberTaskManagers,
			int slotsPerTaskManager,
			ApplicationReport report,
			Configuration flinkConfiguration,
			boolean perJobCluster) {
		return clusterClient;
	}

	@Override
	public ClusterClient<ApplicationId> retrieve(ApplicationId clusterId) {
		return clusterClient;
	}

	@Override
	public ClusterClient<ApplicationId> deploySessionCluster(ClusterSpecification clusterSpecification) {
		return clusterClient;
	}

	@Override
	public ClusterClient<ApplicationId> deployJobCluster(
			ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) {
		return clusterClient;
	}

	@Override
	public void killCluster(ApplicationId clusterId) {
	}

	@Override
	protected String getYarnSessionClusterEntrypoint() {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	@Override
	protected String getYarnJobClusterEntrypoint() {
		throw new UnsupportedOperationException("Not needed in test.");
	}
}
