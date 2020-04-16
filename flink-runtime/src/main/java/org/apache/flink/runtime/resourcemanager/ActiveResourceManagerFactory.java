/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nullable;

/**
 * Resource manager factory which creates active {@link ResourceManager} implementations.
 *
 * <p>The default implementation will call {@link #createActiveResourceManagerConfiguration}
 * to create a new configuration which is configured with active resource manager relevant
 * configuration options.
 *
 * @param <T> type of the {@link ResourceIDRetrievable}
 */
public abstract class ActiveResourceManagerFactory<T extends ResourceIDRetrievable> implements ResourceManagerFactory<T> {

	@Override
	public ResourceManager<T> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup) throws Exception {
		return createActiveResourceManager(
			createActiveResourceManagerConfiguration(configuration),
			resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			fatalErrorHandler,
			clusterInformation,
			webInterfaceUrl,
			resourceManagerMetricGroup);
	}

	private static Configuration createActiveResourceManagerConfiguration(Configuration originalConfiguration) {
		return TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
			originalConfiguration, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
	}

	protected abstract ResourceManager<T> createActiveResourceManager(
		Configuration configuration,
		ResourceID resourceId,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		FatalErrorHandler fatalErrorHandler,
		ClusterInformation clusterInformation,
		@Nullable String webInterfaceUrl,
		ResourceManagerMetricGroup resourceManagerMetricGroup) throws Exception;
}
