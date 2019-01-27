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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.deploy.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.deploy.KubernetesClusterId;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest cluster client for kubernetes, need to override shutDownCluster().
 * In kubernetes cluster, we need to kill cluster after shutDownCluster
 */
public class KubernetesRestClusterClient<T> extends RestClusterClient<T> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesRestClusterClient.class);

	private final T clusterId;

	private final KubernetesClusterDescriptor kubernetesClusterDescriptor;

	public KubernetesRestClusterClient(KubernetesClusterDescriptor descriptor,
			Configuration config, T clusterId) throws Exception {
		super(config, clusterId);
		this.clusterId = clusterId;
		Preconditions.checkNotNull(descriptor);
		this.kubernetesClusterDescriptor = descriptor;
	}

	@Override
	public void shutDownCluster() {
		super.shutDownCluster();
		LOG.info("Killing cluster after shutdown.");
		kubernetesClusterDescriptor.killCluster(KubernetesClusterId.fromString(clusterId.toString()));
	}
}
