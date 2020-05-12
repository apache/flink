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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesResourceManager;
import org.apache.flink.kubernetes.KubernetesWorkerNode;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerConfiguration;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;

import javax.annotation.Nullable;

/**
 * {@link ResourceManagerFactory} implementation which creates a {@link KubernetesResourceManager}.
 */
public class KubernetesResourceManagerFactory extends ActiveResourceManagerFactory<KubernetesWorkerNode> {

	private static final KubernetesResourceManagerFactory INSTANCE = new KubernetesResourceManagerFactory();

	private static final Time POD_CREATION_RETRY_INTERVAL = Time.seconds(3L);

	private KubernetesResourceManagerFactory() {}

	public static KubernetesResourceManagerFactory getInstance() {
		return INSTANCE;
	}

	@Override
	public ResourceManager<KubernetesWorkerNode> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			ResourceManagerRuntimeServices resourceManagerRuntimeServices) {

		final KubernetesResourceManagerConfiguration kubernetesResourceManagerConfiguration =
			new KubernetesResourceManagerConfiguration(
				configuration.getString(KubernetesConfigOptions.CLUSTER_ID),
				POD_CREATION_RETRY_INTERVAL);

		return new KubernetesResourceManager(
			rpcService,
			resourceId,
			configuration,
			highAvailabilityServices,
			heartbeatServices,
			resourceManagerRuntimeServices.getSlotManager(),
			ResourceManagerPartitionTrackerImpl::new,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup,
			KubeClientFactory.fromConfiguration(configuration),
			kubernetesResourceManagerConfiguration);
	}

	@Override
	protected ResourceManagerRuntimeServicesConfiguration createResourceManagerRuntimeServicesConfiguration(
		Configuration configuration) throws ConfigurationException {
		return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration, KubernetesWorkerResourceSpecFactory.INSTANCE);
	}
}
