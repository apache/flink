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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for running a TaskManager in a YARN container.
 */
public class YarnTaskManagerRunner {

	private static final Logger LOG = LoggerFactory.getLogger(YarnTaskManagerRunner.class);

	public static void runYarnTaskManager(String[] args, final Class<? extends YarnTaskManager> taskManager) throws IOException {
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
			return;
		}

		// read the environment variables for YARN
		final Map<String, String> envs = System.getenv();
		final String yarnClientUsername = envs.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());
		LOG.info("Current working/local Directory: {}", localDirs);

		final String currDir = envs.get(Environment.PWD.key());
		LOG.info("Current working Directory: {}", currDir);

		final String remoteKeytabPath = envs.get(YarnConfigKeys.KEYTAB_PATH);
		LOG.info("TM: remoteKeytabPath obtained {}", remoteKeytabPath);

		final String remoteKeytabPrincipal = envs.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		LOG.info("TM: remoteKeytabPrincipal obtained {}", remoteKeytabPrincipal);

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

		// tell akka to die in case of an error
		configuration.setBoolean(ConfigConstants.AKKA_JVM_EXIT_ON_FATAL_ERROR, true);

		String keytabPath = null;
		if(remoteKeytabPath != null) {
			File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
			keytabPath = f.getAbsolutePath();
			LOG.info("keytabPath: {}", keytabPath);
		}

		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

		LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
				currentUser.getShortUserName(), yarnClientUsername );

		// Infer the resource identifier from the environment variable
		String containerID = Preconditions.checkNotNull(envs.get(YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID));
		final ResourceID resourceId = new ResourceID(containerID);
		LOG.info("ResourceID assigned for this container: {}", resourceId);

		try {

			SecurityUtils.SecurityConfiguration sc = new SecurityUtils.SecurityConfiguration(configuration);

			//To support Yarn Secure Integration Test Scenario
			File krb5Conf = new File(currDir, Utils.KRB5_FILE_NAME);
			if(krb5Conf.exists() && krb5Conf.canRead()) {
				String krb5Path = krb5Conf.getAbsolutePath();
				LOG.info("KRB5 Conf: {}", krb5Path);
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
				conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
				conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
				sc.setHadoopConfiguration(conf);
			}

			if(keytabPath != null && remoteKeytabPrincipal != null) {
				configuration.setString(ConfigConstants.SECURITY_KEYTAB_KEY, keytabPath);
				configuration.setString(ConfigConstants.SECURITY_PRINCIPAL_KEY, remoteKeytabPrincipal);
			}

			SecurityUtils.install(sc);

			SecurityUtils.getInstalledContext().runSecured(new Callable<Object>() {
				@Override
				public Integer call() {
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
		} catch(Exception e) {
			LOG.error("Exception occurred while launching Task Manager", e);
			throw new RuntimeException(e);
		}

	}
}
