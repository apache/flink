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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;

/**
 * Implementation of {@link org.apache.flink.yarn.AbstractYarnClusterDescriptor} which is used to start the new application master for a job under flip-6.
 * This implementation is now however tricky, since YarnClusterDescriptorV2 is related YarnClusterClientV2, but AbstractYarnClusterDescriptor is related
 * to YarnClusterClient. We should let YarnClusterDescriptorV2 implements ClusterDescriptor&lt;YarnClusterClientV2&gt;.
 * However, in order to use the code in AbstractYarnClusterDescriptor for setting environments and so on, we make YarnClusterDescriptorV2 as now.
 */
public class YarnClusterDescriptorV2 extends AbstractYarnClusterDescriptor {

	public YarnClusterDescriptorV2(Configuration flinkConfiguration, String configurationDirectory) {
		super(flinkConfiguration, configurationDirectory);
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
	public YarnClusterClient deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph) {
		throw new UnsupportedOperationException("Cannot yet deploy a per-job yarn cluster.");
	}
}
