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

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.mesos.util.MesosUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.ExceptionUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;

/**
 * The entry point for running a TaskManager in a Mesos container.
 */
public class MesosTaskExecutorRunner {

	private static final Logger LOG = LoggerFactory.getLogger(MesosTaskExecutorRunner.class);

	private static final int INIT_ERROR_EXIT_CODE = 31;

	private static final Options ALL_OPTIONS;

	static {
		ALL_OPTIONS =
			new Options()
				.addOption(BootstrapTools.newDynamicPropertiesOption());
	}

	public static void main(String[] args) throws Exception {
		EnvironmentInformation.logEnvironmentInfo(LOG, MesosTaskExecutorRunner.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// try to parse the command line arguments
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(ALL_OPTIONS, args);

		final Configuration configuration;
		try {
			Configuration dynamicProperties = BootstrapTools.parseDynamicProperties(cmd);
			LOG.debug("Mesos dynamic properties: {}", dynamicProperties);

			configuration = MesosUtils.loadConfiguration(dynamicProperties, LOG);
		}
		catch (Throwable t) {
			LOG.error("Failed to load the TaskManager configuration and dynamic properties.", t);
			System.exit(INIT_ERROR_EXIT_CODE);
			return;
		}

		final Map<String, String> envs = System.getenv();

		final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);

		// configure the filesystems
		FileSystem.initialize(configuration, pluginManager);

		// tell akka to die in case of an error
		configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, true);

		// Run the TM in the security context
		SecurityConfiguration sc = new SecurityConfiguration(configuration);
		SecurityUtils.install(sc);

		try {
			SecurityUtils.getInstalledContext().runSecured(() -> {
				TaskManagerRunner.runTaskManager(configuration, pluginManager);

				return 0;
			});
		}
		catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("Error while starting the TaskManager", strippedThrowable);
			System.exit(INIT_ERROR_EXIT_CODE);
		}
	}
}
