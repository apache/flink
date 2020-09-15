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
import org.apache.flink.mesos.util.MesosUtils;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.active.LegacyActiveResourceManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/**
 * {@link ResourceManagerFactory} which creates a {@link MesosResourceManager}.
 */
public class MesosResourceManagerFactory extends LegacyActiveResourceManagerFactory<RegisteredMesosWorkerNode> {

	private static final Logger LOG = LoggerFactory.getLogger(MesosResourceManagerFactory.class);

	@Nonnull
	private final MesosServices mesosServices;

	@Nonnull
	private final MesosConfiguration schedulerConfiguration;

	public MesosResourceManagerFactory(@Nonnull MesosServices mesosServices, @Nonnull MesosConfiguration schedulerConfiguration) {
		this.mesosServices = mesosServices;
		this.schedulerConfiguration = schedulerConfiguration;
	}

	@Override
	public ResourceManager<RegisteredMesosWorkerNode> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			ResourceManagerRuntimeServices resourceManagerRuntimeServices,
			Executor ioExecutor) throws Exception {

		final MesosTaskManagerParameters taskManagerParameters = MesosUtils.createTmParameters(configuration, LOG);
		final ContainerSpecification taskManagerContainerSpec = MesosUtils.createContainerSpec(configuration);

		return new MesosResourceManager(
			rpcService,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			resourceManagerRuntimeServices.getSlotManager(),
			ResourceManagerPartitionTrackerImpl::new,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			configuration,
			mesosServices,
			schedulerConfiguration,
			taskManagerParameters,
			taskManagerContainerSpec,
			webInterfaceUrl,
			resourceManagerMetricGroup,
			ioExecutor);
	}

	@Override
	protected ResourceManagerRuntimeServicesConfiguration createResourceManagerRuntimeServicesConfiguration(
		Configuration configuration) throws ConfigurationException {
		return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration, MesosWorkerResourceSpecFactory.INSTANCE);
	}
}
