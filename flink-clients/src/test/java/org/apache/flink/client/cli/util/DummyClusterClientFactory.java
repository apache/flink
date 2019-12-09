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

package org.apache.flink.client.cli.util;

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ClusterClientFactory} used for testing.
 * @param <ClusterID> The type of the id of the cluster.
 */
public class DummyClusterClientFactory<ClusterID> implements ClusterClientFactory {

	public static final String ID = "dummy-client-factory";

	private final ClusterClient<ClusterID> clusterClient;

	public DummyClusterClientFactory(ClusterClient<ClusterID> clusterClient) {
		this.clusterClient = checkNotNull(clusterClient);
	}

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		return ID.equals(configuration.getString(DeploymentOptions.TARGET));
	}

	@Override
	public ClusterDescriptor<ClusterID> createClusterDescriptor(Configuration configuration) {
		return new DummyClusterDescriptor<>(checkNotNull(clusterClient));
	}

	@Override
	@Nullable
	public String getClusterId(Configuration configuration) {
		return "dummy";
	}

	@Override
	public ClusterSpecification getClusterSpecification(Configuration configuration) {
		return new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
	}
}
