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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is the executable entry point for running a TaskExecutor in a YARN container.
 */
public class YarnTaskExecutorRunnerFactory {

	protected static final Logger LOG = LoggerFactory.getLogger(YarnTaskExecutorRunnerFactory.class);

	/**
	 * Runner for the {@link TaskManagerRunner}.
	 */
	public static class Runner {
		private final Configuration configuration;
		private final ResourceID resourceId;

		Runner(Configuration configuration, ResourceID resourceId) {
			this.configuration = Preconditions.checkNotNull(configuration);
			this.resourceId = Preconditions.checkNotNull(resourceId);
		}

		public void run() throws Exception {
			SecurityUtils.getInstalledContext().runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					TaskManagerRunner.runTaskManager(configuration, resourceId);
					return null;
				}
			});
		}

		@VisibleForTesting
		Configuration getConfiguration() {
			return configuration;
		}

		@VisibleForTesting
		ResourceID getResourceId() {
			return resourceId;
		}
	}

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

		try {
				YarnTaskExecutorRunnerFactory.create(System.getenv()).run();
		} catch (Exception e) {
			LOG.error("Exception occurred while launching Task Executor runner", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Creates a {@link YarnTaskExecutorRunnerFactory.Runner}.
	 *
	 * @param envs environment variables.
	 */
	@VisibleForTesting
	protected static Runner create(Map<String, String> envs) throws IOException {
		LOG.debug("All environment variables: {}", envs);

		final String yarnClientUsername = envs.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());
		LOG.info("Current working/local Directory: {}", localDirs);

		final String currDir = envs.get(Environment.PWD.key());
		LOG.info("Current working Directory: {}", currDir);

		final String remoteKeytabPrincipal = envs.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		LOG.info("TM: remote keytab principal obtained {}", remoteKeytabPrincipal);

		final Configuration configuration = GlobalConfiguration.loadConfiguration(currDir);
		FileSystem.initialize(configuration);

		// configure local directory
		if (configuration.contains(CoreOptions.TMP_DIRS)) {
			LOG.info("Overriding YARN's temporary file directories with those " +
				"specified in the Flink config: " + configuration.getValue(CoreOptions.TMP_DIRS));
		}
		else {
			LOG.info("Setting directories for temporary files to: {}", localDirs);
			configuration.setString(CoreOptions.TMP_DIRS, localDirs);
		}

		// tell akka to die in case of an error
		configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, true);

		try {
			UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

			LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
					currentUser.getShortUserName(), yarnClientUsername);

			File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
			if (f.exists() && remoteKeytabPrincipal != null) {
				configuration.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, f.getAbsolutePath());
				configuration.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
			}

			SecurityConfiguration sc = new SecurityConfiguration(configuration);

			final String containerId = envs.get(YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID);
			Preconditions.checkArgument(containerId != null,
				"ContainerId variable %s not set", YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID);

			// use the hostname passed by job manager
			final String taskExecutorHostname = envs.get(YarnResourceManager.ENV_FLINK_NODE_ID);
			if (taskExecutorHostname != null) {
				configuration.setString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, taskExecutorHostname);
			}

			SecurityUtils.install(sc);

			return new Runner(configuration, new ResourceID(containerId));
		} catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN TaskManager initialization failed.", t);
			throw new RuntimeException(t);
		}
	}
}
