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

package org.apache.flink.mesos.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManager;
import org.apache.flink.mesos.runtime.clusterframework.MesosTaskManagerParameters;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServicesUtils;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import javax.annotation.Nullable;

/**
 * Entry point for Mesos session clusters.
 */
public class MesosSessionClusterEntrypoint extends SessionClusterEntrypoint {

	// ------------------------------------------------------------------------
	//  Command-line options
	// ------------------------------------------------------------------------

	private static final Options ALL_OPTIONS;

	static {
		ALL_OPTIONS =
			new Options()
				.addOption(BootstrapTools.newDynamicPropertiesOption());
	}

	private final Configuration dynamicProperties;

	private MesosConfiguration mesosConfig;

	private MesosServices mesosServices;

	private MesosTaskManagerParameters taskManagerParameters;

	private ContainerSpecification taskManagerContainerSpec;

	public MesosSessionClusterEntrypoint(Configuration config, Configuration dynamicProperties) {
		super(config);

		this.dynamicProperties = Preconditions.checkNotNull(dynamicProperties);
	}

	@Override
	protected void initializeServices(Configuration config) throws Exception {
		super.initializeServices(config);

		final String hostname = config.getString(JobManagerOptions.ADDRESS);

		// Mesos configuration
		mesosConfig = MesosEntrypointUtils.createMesosSchedulerConfiguration(config, hostname);

		// services
		mesosServices = MesosServicesUtils.createMesosServices(config, hostname);

		// TM configuration
		taskManagerParameters = MesosEntrypointUtils.createTmParameters(config, LOG);
		taskManagerContainerSpec = MesosEntrypointUtils.createContainerSpec(config, dynamicProperties);
	}

	@Override
	protected void startClusterComponents(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry) throws Exception {
		super.startClusterComponents(configuration, rpcService, highAvailabilityServices, blobServer, heartbeatServices, metricRegistry);
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
			@Nullable String webInterfaceUrl) throws Exception {
		final ResourceManagerConfiguration rmConfiguration = ResourceManagerConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServicesConfiguration rmServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServices rmRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			rmServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		return new MesosResourceManager(
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME,
			resourceId,
			rmConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			rmRuntimeServices.getSlotManager(),
			metricRegistry,
			rmRuntimeServices.getJobLeaderIdService(),
			fatalErrorHandler,
			configuration,
			mesosServices,
			mesosConfig,
			taskManagerParameters,
			taskManagerContainerSpec
			);
	}

	@Override
	protected void stopClusterComponents(boolean cleanupHaData) throws Exception {
		Throwable exception = null;

		try {
			super.stopClusterComponents(cleanupHaData);
		} catch (Throwable t) {
			exception = t;
		}

		if (mesosServices != null) {
			try {
				mesosServices.close(cleanupHaData);
			} catch (Throwable t) {
				exception = t;
			}
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the Mesos session cluster entry point.", exception);
		}
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, MesosSessionClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// load configuration incl. dynamic properties
		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(ALL_OPTIONS, args);
		}
		catch (Exception e){
			LOG.error("Could not parse the command-line options.", e);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
			return;
		}

		Configuration dynamicProperties = BootstrapTools.parseDynamicProperties(cmd);
		Configuration configuration = GlobalConfiguration.loadConfigurationWithDynamicProperties(dynamicProperties);

		MesosSessionClusterEntrypoint clusterEntrypoint = new MesosSessionClusterEntrypoint(configuration, dynamicProperties);

		clusterEntrypoint.startCluster();
	}
}
