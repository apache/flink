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

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.mesos.entrypoint.MesosEntrypointUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * The entry point for running a TaskManager in a Mesos container.
 */
public class MesosTaskManagerRunner {

	private static final Logger LOG = LoggerFactory.getLogger(MesosTaskManagerRunner.class);

	private static final Options ALL_OPTIONS;

	static {
		ALL_OPTIONS =
			new Options()
				.addOption(BootstrapTools.newDynamicPropertiesOption());
	}

	/** The process environment variables. */
	private static final Map<String, String> ENV = System.getenv();

	public static void runTaskManager(String[] args, final Class<? extends TaskManager> taskManager) throws Exception {
		EnvironmentInformation.logEnvironmentInfo(LOG, taskManager.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// try to parse the command line arguments
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(ALL_OPTIONS, args);

		final Configuration configuration;
		try {
			final Configuration dynamicProperties = BootstrapTools.parseDynamicProperties(cmd);
			LOG.debug("Mesos dynamic properties: {}", dynamicProperties);

			configuration = MesosEntrypointUtils.loadConfiguration(dynamicProperties, LOG);
		}
		catch (Throwable t) {
			LOG.error("Failed to load the TaskManager configuration and dynamic properties.", t);
			System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
			return;
		}

		final Map<String, String> envs = System.getenv();

		// configure the filesystems
		try {
			FileSystem.initialize(configuration);
		} catch (IOException e) {
			throw new IOException("Error while confoguring the filesystems.", e);
		}

		// tell akka to die in case of an error
		configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, true);

		// Infer the resource identifier from the environment variable
		String containerID = Preconditions.checkNotNull(envs.get(MesosConfigKeys.ENV_FLINK_CONTAINER_ID));
		final ResourceID resourceId = new ResourceID(containerID);
		LOG.info("ResourceID assigned for this container: {}", resourceId);

		// Run the TM in the security context
		SecurityConfiguration sc = new SecurityConfiguration(configuration);
		SecurityUtils.install(sc);

		try {
			SecurityUtils.getInstalledContext().runSecured(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					TaskManager.selectNetworkInterfaceAndRunTaskManager(configuration, resourceId, taskManager);
					return 0;
				}
			});
		}
		catch (Throwable t) {
			LOG.error("Error while starting the TaskManager", t);
			System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
		}
	}
}
