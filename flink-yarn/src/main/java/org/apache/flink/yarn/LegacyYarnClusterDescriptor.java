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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Legacy implementation of {@link AbstractYarnClusterDescriptor} which starts an {@link YarnApplicationMasterRunner}.
 */
public class LegacyYarnClusterDescriptor extends AbstractYarnClusterDescriptor {

	public LegacyYarnClusterDescriptor(
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
		return YarnApplicationMasterRunner.class.getName();
	}

	@Override
	protected String getYarnJobClusterEntrypoint() {
		throw new UnsupportedOperationException("The old Yarn descriptor does not support proper per-job mode.");
	}

	@Override
	public YarnClusterClient deployJobCluster(
			ClusterSpecification clusterSpecification,
			JobGraph jobGraph,
			boolean detached) {
		throw new UnsupportedOperationException("Cannot deploy a per-job yarn cluster yet.");
	}

	@Override
	protected ClusterClient<ApplicationId> createYarnClusterClient(AbstractYarnClusterDescriptor descriptor, int numberTaskManagers, int slotsPerTaskManager, ApplicationReport report, Configuration flinkConfiguration, boolean perJobCluster) throws Exception {
		return new YarnClusterClient(
			descriptor,
			numberTaskManagers,
			slotsPerTaskManager,
			report,
			flinkConfiguration,
			perJobCluster);
	}
}
