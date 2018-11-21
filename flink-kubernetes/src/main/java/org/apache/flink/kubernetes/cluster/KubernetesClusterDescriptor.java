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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.client.Endpoint;
import org.apache.flink.kubernetes.client.KubernetesClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterDescriptor.class);

	private static final String CLUSTER_ID_PREFIX = "flink-session-cluster-";

	private static final String CLUSTER_DESCRIPTION = "Kubernetes cluster";

	private FlinkKubernetesOptions options;

	private KubernetesClient client;

	public KubernetesClusterDescriptor(@Nonnull FlinkKubernetesOptions options, @Nonnull KubernetesClient client) {
		this.options = options;
		this.client = client;
	}

	private String generateClusterId() {
		return CLUSTER_ID_PREFIX + UUID.randomUUID();
	}

	@Override
	public String getClusterDescription() {
		return CLUSTER_DESCRIPTION;
	}

	private ClusterClient<String> createClusterEndpoint(Endpoint clusterEndpoint, String clusterId) throws Exception {

		Configuration configuration = new Configuration(this.options.getConfiguration());
		configuration.setString(JobManagerOptions.ADDRESS, clusterEndpoint.getAddress());
		configuration.setInteger(JobManagerOptions.PORT, clusterEndpoint.getPort());
		return new RestClusterClient<>(configuration, clusterId);
	}

	@Override
	public ClusterClient<String> retrieve(String clusterId) throws ClusterRetrieveException {
		try {
			Endpoint clusterEndpoint = this.client.getResetEndpoint(clusterId);
			return this.createClusterEndpoint(clusterEndpoint, clusterId);
		} catch (Exception e) {
			this.client.logException(e);
			throw new ClusterRetrieveException("Could not create the RestClusterClient.", e);
		}
	}

	@Override
	public ClusterClient<String> deploySessionCluster(ClusterSpecification clusterSpecification)
		throws ClusterDeploymentException {

		String clusterId = this.generateClusterId();

		//TODO: add arguments
		final List<String> args = Arrays.asList();

		return this.deployClusterInternal(clusterId, args);
	}

	@Override
	public ClusterClient<String> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) {
		throw new NotImplementedException();
	}

	@Nonnull
	private ClusterClient<String> deployClusterInternal(String clusterId, List<String> args) throws ClusterDeploymentException {
		try {
			Endpoint clusterEndpoint = this.client.createClusterService();
			this.client.createClusterPod();
			return this.createClusterEndpoint(clusterEndpoint, clusterId);
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
