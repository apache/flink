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
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManagerFactory;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServicesUtils;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.mesos.util.MesosUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Entry point for Mesos per-job clusters.
 */
public class MesosJobClusterEntrypoint extends JobClusterEntrypoint {

	private MesosConfiguration schedulerConfiguration;

	private MesosServices mesosServices;

	public MesosJobClusterEntrypoint(Configuration config) {
		super(config);
	}

	@Override
	protected void initializeServices(Configuration config, PluginManager pluginManager) throws Exception {
		super.initializeServices(config, pluginManager);

		final String hostname = config.getString(JobManagerOptions.ADDRESS);

		// Mesos configuration
		schedulerConfiguration = MesosUtils.createMesosSchedulerConfiguration(config, hostname);

		// services
		mesosServices = MesosServicesUtils.createMesosServices(config, hostname);
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
	protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException {
		return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
			new MesosResourceManagerFactory(
				mesosServices,
				schedulerConfiguration),
			FileJobGraphRetriever.createFrom(configuration, ClusterEntrypointUtils.tryFindUserLibDirectory().orElse(null)));
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, MesosJobClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// load configuration incl. dynamic properties
		Configuration dynamicProperties = ClusterEntrypointUtils.parseParametersOrExit(
			args,
			new DynamicParametersConfigurationParserFactory(),
			MesosJobClusterEntrypoint.class);
		Configuration configuration = MesosUtils.loadConfiguration(dynamicProperties, LOG);

		MesosJobClusterEntrypoint clusterEntrypoint = new MesosJobClusterEntrypoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(clusterEntrypoint);
	}
}
