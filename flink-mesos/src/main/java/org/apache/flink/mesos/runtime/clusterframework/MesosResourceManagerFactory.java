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
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.active.ActiveResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.util.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link ActiveResourceManagerFactory} implementation which creates {@link ActiveResourceManager} with {@link MesosResourceManagerDriver}.
 */
public class MesosResourceManagerFactory extends ActiveResourceManagerFactory<RegisteredMesosWorkerNode> {

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
	protected ResourceManagerDriver<RegisteredMesosWorkerNode> createResourceManagerDriver(
			Configuration configuration, @Nullable String webInterfaceUrl, String rpcAddress) throws Exception {
		final MesosTaskManagerParameters taskManagerParameters = MesosUtils.createTmParameters(configuration, LOG);
		final ContainerSpecification taskManagerContainerSpec = MesosUtils.createContainerSpec(configuration);

		return new MesosResourceManagerDriver(
				configuration,
				mesosServices,
				schedulerConfiguration,
				taskManagerParameters,
				taskManagerContainerSpec,
				webInterfaceUrl);
	}

	@Override
	protected ResourceManagerRuntimeServicesConfiguration createResourceManagerRuntimeServicesConfiguration(
		Configuration configuration) throws ConfigurationException {
		return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration, MesosWorkerResourceSpecFactory.INSTANCE);
	}
}
