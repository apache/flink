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
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.TaskManager;
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
 * The entry point for running a TaskManager in a YARN container.
 */
public class YarnTaskManagerRunnerFactory {

	private static final Logger LOG = LoggerFactory.getLogger(YarnTaskManagerRunnerFactory.class);

	/**
	 * Runner for the {@link YarnTaskManager}.
	 */
	public static class Runner implements Callable<Object> {

		private final Configuration configuration;
		private final ResourceID resourceId;
		private final Class<? extends YarnTaskManager> taskManager;

		Runner(Configuration configuration, ResourceID resourceId, Class<? extends YarnTaskManager> taskManager) {
			this.configuration = Preconditions.checkNotNull(configuration);
			this.resourceId = Preconditions.checkNotNull(resourceId);
			this.taskManager = Preconditions.checkNotNull(taskManager);
		}

		@Override
		public Object call() throws Exception {
			try {
				TaskManager.selectNetworkInterfaceAndRunTaskManager(
					configuration, resourceId, taskManager);
			} catch (Throwable t) {
				LOG.error("Error while starting the TaskManager", t);
				System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
			}
			return null;
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

	/**
	 * Creates a {@link YarnTaskManagerRunnerFactory.Runner}.
	 */
	public static Runner create(
			String[] args,
			final Class<? extends YarnTaskManager> taskManager,
			Map<String, String> envs) throws IOException {

		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// try to parse the command line arguments
		final Configuration configuration;
		try {
			configuration = TaskManager.parseArgsAndLoadConfig(args);
		}
		catch (Throwable t) {
			LOG.error(t.getMessage(), t);
			System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
			return null;
		}

		// read the environment variables for YARN
		final String yarnClientUsername = envs.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());
		LOG.info("Current working/local Directory: {}", localDirs);

		final String currDir = envs.get(Environment.PWD.key());
		LOG.info("Current working Directory: {}", currDir);

		final String remoteKeytabPrincipal = envs.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		LOG.info("TM: remoteKeytabPrincipal obtained {}", remoteKeytabPrincipal);

		BootstrapTools.updateTmpDirectoriesInConfiguration(configuration, localDirs);

		// tell akka to die in case of an error
		configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, true);

		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

		LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
				currentUser.getShortUserName(), yarnClientUsername);

		// Infer the resource identifier from the environment variable
		String containerID = Preconditions.checkNotNull(envs.get(YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID));
		final ResourceID resourceId = new ResourceID(containerID);
		LOG.info("ResourceID assigned for this container: {}", resourceId);

		File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
		if (remoteKeytabPrincipal != null && f.exists()) {
			// set keytab principal and replace path with the local path of the shipped keytab file in NodeManager
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, f.getAbsolutePath());
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
		}

		try {
			SecurityConfiguration sc = new SecurityConfiguration(configuration);
			SecurityUtils.install(sc);

			return new Runner(configuration, resourceId, taskManager);
		} catch (Exception e) {
			LOG.error("Exception occurred while building Task Manager runner", e);
			throw new RuntimeException(e);
		}

	}
}
