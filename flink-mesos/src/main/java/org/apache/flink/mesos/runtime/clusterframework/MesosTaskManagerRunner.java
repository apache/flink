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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.cli.FlinkMesosSessionCli;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.apache.flink.util.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for running a TaskManager in a Mesos container.
 */
public class MesosTaskManagerRunner {

	private static final Logger LOG = LoggerFactory.getLogger(MesosTaskManagerRunner.class);

	/** The process environment variables */
	private static final Map<String, String> ENV = System.getenv();

	public static void runTaskManager(String[] args, final Class<? extends TaskManager> taskManager) throws IOException {
		EnvironmentInformation.logEnvironmentInfo(LOG, taskManager.getSimpleName(), args);
		org.apache.flink.runtime.util.SignalHandler.register(LOG);

		// try to parse the command line arguments
		final Configuration configuration;
		try {
			configuration = TaskManager.parseArgsAndLoadConfig(args);

			// add dynamic properties to TaskManager configuration.
			final Configuration dynamicProperties =
				FlinkMesosSessionCli.decodeDynamicProperties(ENV.get(MesosConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("Mesos dynamic properties: {}", dynamicProperties);
			configuration.addAll(dynamicProperties);
		}
		catch (Throwable t) {
			LOG.error("Failed to load the TaskManager configuration and dynamic properties.", t);
			System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
			return;
		}

		// read the environment variables
		final Map<String, String> envs = System.getenv();
		final String effectiveUsername = envs.get(MesosConfigKeys.ENV_CLIENT_USERNAME);
		final String tmpDirs = envs.get(MesosConfigKeys.ENV_FLINK_TMP_DIR);

		// configure local directory
		String flinkTempDirs = configuration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, null);
		if (flinkTempDirs != null) {
			LOG.info("Overriding Mesos temporary file directories with those " +
				"specified in the Flink config: {}", flinkTempDirs);
		}
		else if (tmpDirs != null) {
			LOG.info("Setting directories for temporary files to: {}", tmpDirs);
			configuration.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, tmpDirs);
		}

		LOG.info("Mesos task runs as '{}', setting user to execute Flink TaskManager to '{}'",
			UserGroupInformation.getCurrentUser().getShortUserName(), effectiveUsername);

		// tell akka to die in case of an error
		configuration.setBoolean(ConfigConstants.AKKA_JVM_EXIT_ON_FATAL_ERROR, true);

		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(effectiveUsername);
		for (Token<? extends TokenIdentifier> toks : UserGroupInformation.getCurrentUser().getTokens()) {
			ugi.addToken(toks);
		}

		// Infer the resource identifier from the environment variable
		String containerID = Preconditions.checkNotNull(envs.get(MesosConfigKeys.ENV_FLINK_CONTAINER_ID));
		final ResourceID resourceId = new ResourceID(containerID);
		LOG.info("ResourceID assigned for this container: {}", resourceId);

		ugi.doAs(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					TaskManager.selectNetworkInterfaceAndRunTaskManager(configuration, resourceId, taskManager);
				}
				catch (Throwable t) {
					LOG.error("Error while starting the TaskManager", t);
					System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
				}
				return null;
			}
		});
	}
}
