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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is the executable entry point for running a TaskExecutor in a YARN container.
 */
public class YarnTaskExecutorRunner {

	protected static final Logger LOG = LoggerFactory.getLogger(YarnTaskExecutorRunner.class);

	/** The process environment variables. */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the yarn task executor runner failed. */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN task executor runner.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskExecutor runner", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		run(args);
	}

	/**
	 * The instance entry point for the YARN task executor. Obtains user group information and calls
	 * the main work method {@link TaskManagerRunner#runTaskManager(Configuration, ResourceID)}  as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 */
	private static void run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			final String currDir = ENV.get(Environment.PWD.key());
			LOG.info("Current working Directory: {}", currDir);

      // Some dynamic properties for task manager are added to args, such as managed memory size.
			final CommandLineParser<ClusterConfiguration> commandLineParser =
				new CommandLineParser<>(new ClusterConfigurationParserFactory());

			ClusterConfiguration clusterConfiguration = null;
			try {
				clusterConfiguration = commandLineParser.parse(args);
			} catch (FlinkParseException e) {
				LOG.error("Could not parse command line arguments {}.", args, e);
				commandLineParser.printHelp(YarnTaskExecutorRunner.class.getSimpleName());
				System.exit(1);
			}

			final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(
				clusterConfiguration.getDynamicProperties());
			final Configuration configuration = GlobalConfiguration.loadConfiguration(currDir, dynamicProperties);

			FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

			setupConfigurationAndInstallSecurityContext(configuration, currDir, ENV);

			final String containerId = ENV.get(YarnResourceManager.ENV_FLINK_CONTAINER_ID);
			Preconditions.checkArgument(containerId != null,
				"ContainerId variable %s not set", YarnResourceManager.ENV_FLINK_CONTAINER_ID);

			SecurityUtils.getInstalledContext().runSecured((Callable<Void>) () -> {
				TaskManagerRunner.runTaskManager(configuration, new ResourceID(containerId));
				return null;
			});
		}
		catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			// make sure that everything whatever ends up in the log
			LOG.error("YARN TaskManager initialization failed.", strippedThrowable);
			System.exit(INIT_ERROR_EXIT_CODE);
		}
	}

	@VisibleForTesting
	static void setupConfigurationAndInstallSecurityContext(Configuration configuration, String currDir, Map<String, String> variables) throws Exception {
		final String localDirs = variables.get(Environment.LOCAL_DIRS.key());
		LOG.info("Current working/local Directory: {}", localDirs);

		BootstrapTools.updateTmpDirectoriesInConfiguration(configuration, localDirs);

		setupConfigurationFromVariables(configuration, currDir, variables);

		installSecurityContext(configuration);
	}

	private static void setupConfigurationFromVariables(Configuration configuration, String currDir, Map<String, String> variables) throws IOException {
		final String yarnClientUsername = variables.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);

		final String remoteKeytabPath = variables.get(YarnConfigKeys.KEYTAB_PATH);
		LOG.info("TM: remote keytab path obtained {}", remoteKeytabPath);

		final String remoteKeytabPrincipal = variables.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		LOG.info("TM: remote keytab principal obtained {}", remoteKeytabPrincipal);

		// tell akka to die in case of an error
		configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, true);

		String keytabPath = null;
		if (remoteKeytabPath != null) {
			File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
			keytabPath = f.getAbsolutePath();
			LOG.info("keytab path: {}", keytabPath);
		}

		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

		LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
				currentUser.getShortUserName(), yarnClientUsername);

		if (keytabPath != null && remoteKeytabPrincipal != null) {
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
		}

		// use the hostname passed by job manager
		final String taskExecutorHostname = variables.get(YarnResourceManager.ENV_FLINK_NODE_ID);
		if (taskExecutorHostname != null) {
			configuration.setString(TaskManagerOptions.HOST, taskExecutorHostname);
		}
	}

	private static void installSecurityContext(Configuration configuration) throws Exception {
		SecurityConfiguration sc = new SecurityConfiguration(configuration);
		SecurityUtils.install(sc);
	}
}
