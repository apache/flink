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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManager;
import org.apache.flink.mesos.runtime.clusterframework.MesosTaskManagerParameters;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServicesUtils;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.CompletableFuture;

/**
 * Entry point for Mesos per-job clusters.
 */
public class MesosJobClusterEntrypoint extends JobClusterEntrypoint {

	public static final String JOB_GRAPH_FILE_PATH = "flink.jobgraph.path";

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

	private MesosConfiguration schedulerConfiguration;

	private MesosServices mesosServices;

	private MesosTaskManagerParameters taskManagerParameters;

	private ContainerSpecification taskManagerContainerSpec;

	public MesosJobClusterEntrypoint(Configuration config, Configuration dynamicProperties) {
		super(config);

		this.dynamicProperties = Preconditions.checkNotNull(dynamicProperties);
	}

	@Override
	protected void initializeServices(Configuration config) throws Exception {
		super.initializeServices(config);

		final String hostname = config.getString(JobManagerOptions.ADDRESS);

		// Mesos configuration
		schedulerConfiguration = MesosEntrypointUtils.createMesosSchedulerConfiguration(config, hostname);

		// services
		mesosServices = MesosServicesUtils.createMesosServices(config, hostname);

		// TM configuration
		taskManagerParameters = MesosEntrypointUtils.createTmParameters(config, LOG);
		taskManagerContainerSpec = MesosEntrypointUtils.createContainerSpec(config, dynamicProperties);
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
			taskManagerContainerSpec
			);
	}

	@Override
	protected JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
		String jobGraphFile = configuration.getString(JOB_GRAPH_FILE_PATH, "job.graph");
		File fp = new File(jobGraphFile);

		try (FileInputStream input = new FileInputStream(fp);
			ObjectInputStream obInput = new ObjectInputStream(input)) {

			return (JobGraph) obInput.readObject();
		} catch (FileNotFoundException e) {
			throw new FlinkException("Could not find the JobGraph file.", e);
		} catch (ClassNotFoundException | IOException e) {
			throw new FlinkException("Could not load the JobGraph from file.", e);
		}
	}

	@Override
	protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
		final CompletableFuture<Void> serviceShutDownFuture = super.stopClusterServices(cleanupHaData);

		return FutureUtils.runAfterwards(
			serviceShutDownFuture,
			() -> {
				if (mesosServices != null) {
					mesosServices.close(cleanupHaData);
				}
			});
	}

	@Override
	protected void registerShutdownActions(CompletableFuture<ApplicationStatus> terminationFuture) {}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, MesosJobClusterEntrypoint.class.getSimpleName(), args);
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
		Configuration configuration = MesosEntrypointUtils.loadConfiguration(dynamicProperties, LOG);

		MesosJobClusterEntrypoint clusterEntrypoint = new MesosJobClusterEntrypoint(configuration, dynamicProperties);

		clusterEntrypoint.startCluster();
	}
}
