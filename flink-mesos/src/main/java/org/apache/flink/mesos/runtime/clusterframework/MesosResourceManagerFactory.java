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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link ResourceManagerFactory} which creates a {@link MesosResourceManager}.
 */
public class MesosResourceManagerFactory implements ResourceManagerFactory<RegisteredMesosWorkerNode> {

	@Nonnull
	private final MesosServices mesosServices;

	@Nonnull
	private final MesosConfiguration schedulerConfiguration;

	@Nonnull
	private final MesosTaskManagerParameters taskManagerParameters;

	@Nonnull
	private final ContainerSpecification taskManagerContainerSpec;

	public MesosResourceManagerFactory(@Nonnull MesosServices mesosServices, @Nonnull MesosConfiguration schedulerConfiguration, @Nonnull MesosTaskManagerParameters taskManagerParameters, @Nonnull ContainerSpecification taskManagerContainerSpec) {
		this.mesosServices = mesosServices;
		this.schedulerConfiguration = schedulerConfiguration;
		this.taskManagerParameters = taskManagerParameters;
		this.taskManagerContainerSpec = taskManagerContainerSpec;
	}

	@Override
	public ResourceManager<RegisteredMesosWorkerNode> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			JobManagerMetricGroup jobManagerMetricGroup) throws Exception {
		final ResourceManagerRuntimeServicesConfiguration rmServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServices rmRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			rmServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		return new MesosResourceManager(
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			rmRuntimeServices.getSlotManager(),
			metricRegistry,
			rmRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			configuration,
			mesosServices,
			schedulerConfiguration,
			taskManagerParameters,
			taskManagerContainerSpec,
			webInterfaceUrl,
			jobManagerMetricGroup);
	}
}
