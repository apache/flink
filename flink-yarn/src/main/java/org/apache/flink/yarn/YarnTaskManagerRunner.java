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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for running a TaskManager in a YARN container. The YARN container will invoke
 * this class' main method.
 */
public class YarnTaskManagerRunner {

	private static final Logger LOG = LoggerFactory.getLogger(YarnTaskManagerRunner.class);

	public static <T extends YarnTaskManager> void runYarnTaskManager(String[] args, final Class<T> taskManager) throws IOException {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskManager", args);
		EnvironmentInformation.checkJavaVersion();
		org.apache.flink.runtime.util.SignalHandler.register(LOG);

		// try to parse the command line arguments
		final Configuration configuration;
		try {
			configuration = TaskManager.parseArgsAndLoadConfig(args);
		}
		catch (Throwable t) {
			LOG.error(t.getMessage(), t);
			System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
			return;
		}

		// read the environment variables for YARN
		final Map<String, String> envs = System.getenv();
		final String yarnClientUsername = envs.get(FlinkYarnClient.ENV_CLIENT_USERNAME);
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());

		// configure local directory
		String flinkTempDirs = configuration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, null);
		if (flinkTempDirs == null) {
			LOG.info("Setting directories for temporary file " + localDirs);
			configuration.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, localDirs);
		}
		else {
			LOG.info("Overriding YARN's temporary file directories with those " +
				"specified in the Flink config: " + flinkTempDirs);
		}

		LOG.info("YARN daemon runs as '" + UserGroupInformation.getCurrentUser().getShortUserName() +
			"' setting user to execute Flink TaskManager to '" + yarnClientUsername + "'");

		// tell akka to die in case of an error
		configuration.setBoolean(ConfigConstants.AKKA_JVM_EXIT_ON_FATAL_ERROR, true);

		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(yarnClientUsername);
		for (Token<? extends TokenIdentifier> toks : UserGroupInformation.getCurrentUser().getTokens()) {
			ugi.addToken(toks);
		}
		ugi.doAs(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					TaskManager.selectNetworkInterfaceAndRunTaskManager(configuration, taskManager);
				}
				catch (Throwable t) {
					LOG.error("Error while starting the TaskManager", t);
					System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
				}
				return null;
			}
		});
	}


	public static void main(final String[] args) throws IOException {
		runYarnTaskManager(args, YarnTaskManager.class);
	}
}
