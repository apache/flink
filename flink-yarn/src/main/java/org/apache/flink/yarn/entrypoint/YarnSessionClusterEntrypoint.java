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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.YarnResourceManager;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * Entry point for Yarn session clusters.
 */
public class YarnSessionClusterEntrypoint extends SessionClusterEntrypoint {

	private final String workingDirectory;

	public YarnSessionClusterEntrypoint(
			Configuration configuration,
			String workingDirectory) {
		super(configuration);
		this.workingDirectory = Preconditions.checkNotNull(workingDirectory);
	}

	@Override
	protected SecurityContext installSecurityContext(Configuration configuration) throws Exception {
		return YarnEntrypointUtils.installSecurityContext(configuration, workingDirectory);
	}

	@Override
	protected String getRPCPortRange(Configuration configuration) {
		return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
	}

	@Override
	protected ResourceManager<?> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl) throws Exception {
		final ResourceManagerRuntimeServicesConfiguration rmServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServices rmRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			rmServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		return new YarnResourceManager(
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME,
			resourceId,
			configuration,
			System.getenv(),
			highAvailabilityServices,
			heartbeatServices,
			rmRuntimeServices.getSlotManager(),
			metricRegistry,
			rmRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			webInterfaceUrl);
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnSessionClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Map<String, String> env = System.getenv();

		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
			workingDirectory != null,
			"Working directory variable (%s) not set",
			ApplicationConstants.Environment.PWD.key());

		try {
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}

		Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env, LOG);

		YarnSessionClusterEntrypoint yarnSessionClusterEntrypoint = new YarnSessionClusterEntrypoint(
			configuration,
			workingDirectory);

		yarnSessionClusterEntrypoint.startCluster();
	}
}
