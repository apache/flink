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

package org.apache.flink.kubernetes.cluster;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.KubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkService;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.UUID;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterDescriptor.class);

	private static final String CLUSTER_ID_PREFIX = "flink-session-cluster-";

	private static final String CLUSTER_DESCRIPTION = "Kubernetes cluster";

	private FlinkKubernetesOptions options;

	private KubeClient client;

	public KubernetesClusterDescriptor(@Nonnull FlinkKubernetesOptions options) {
		this(options, KubeClientFactory.fromConfiguration(options));
	}

	public KubernetesClusterDescriptor(@Nonnull FlinkKubernetesOptions options, @Nonnull KubeClient client) {
		this.options = options;
		this.client = client;
		this.client.initialize();
	}

	private String generateClusterId() {
		return CLUSTER_ID_PREFIX + UUID.randomUUID();
	}

	@Override
	public String getClusterDescription() {
		return CLUSTER_DESCRIPTION;
	}

	private ClusterClient<String> createClusterClient(FlinkService clusterService, String clusterId) throws Exception {

		Configuration configuration = new Configuration(this.options.getConfiguration());

		//now update the access info of new service: JM.addr, JM.port, rest.port
		Map<ConfigOption<Integer>, Endpoint> endpointMappings = this.client.extractEndpoints(clusterService);

		if (endpointMappings.containsKey(RestOptions.PORT)) {
			configuration.setString(RestOptions.ADDRESS, endpointMappings.get(RestOptions.PORT).getAddress());
			configuration.setInteger(RestOptions.PORT, endpointMappings.get(RestOptions.PORT).getPort());
		}
		if (endpointMappings.containsKey(JobManagerOptions.PORT)) {
			configuration.setString(JobManagerOptions.ADDRESS, endpointMappings.get(JobManagerOptions.PORT).getAddress());
			configuration.setInteger(JobManagerOptions.PORT, endpointMappings.get(JobManagerOptions.PORT).getPort());
		}

		return new RestClusterClient<>(configuration, clusterId);
	}

	@Override
	public ClusterClient<String> retrieve(String clusterId) throws ClusterRetrieveException {
		try {
			FlinkService clusterService = this.client.getFlinkService(clusterId);
			return this.createClusterClient(clusterService, clusterId);
		} catch (Exception e) {
			this.client.logException(e);
			throw new ClusterRetrieveException("Could not create the RestClusterClient.", e);
		}
	}

	@Override
	public ClusterClient<String> deploySessionCluster(ClusterSpecification clusterSpecification)
		throws ClusterDeploymentException {

		String clusterId = options.getClusterId() != null ? options.getClusterId() : generateClusterId();
		return this.deployClusterInternal(clusterId);
	}

	@Override
	public ClusterClient<String> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) {
		throw new NotImplementedException();
	}

	@Nonnull
	private ClusterClient<String> deployClusterInternal(String clusterId) throws ClusterDeploymentException {
		try {
			FlinkService clusterService = this.client.createClusterService(clusterId).get();
			this.client.createClusterPod();
			return this.createClusterClient(clusterService, clusterId);
		} catch (Exception e) {
			this.client.logException(e);
			this.tryKillCluster(clusterId);
			throw new ClusterDeploymentException("Could not create Kubernetes cluster " + clusterId, e);
		}
	}

	/**
	 * Try to kill cluster without throw exception.
	 */
	private void tryKillCluster(String clusterId) {
		try {
			this.killCluster(clusterId);
		} catch (Exception e) {
			this.client.logException(e);
		}
	}

	@Override
	public void killCluster(String clusterId) throws FlinkException {
		try {
			this.client.stopAndCleanupCluster(clusterId);
		} catch (Exception e) {
			this.client.logException(e);
			throw new FlinkException("Could not create Kubernetes cluster " + clusterId);
		}
	}

	@Override
	public void close() {
		try {
			this.client.close();
		} catch (Exception e) {
			this.client.logException(e);
			LOG.error("failed to close client, exception {}", e.toString());
		}
	}
}
