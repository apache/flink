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

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceConfiguration;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
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

			final String yarnClientUsername = ENV.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
			final String localDirs = ENV.get(Environment.LOCAL_DIRS.key());
			LOG.info("Current working/local Directory: {}", localDirs);

			final String currDir = ENV.get(Environment.PWD.key());
			LOG.info("Current working Directory: {}", currDir);

			final String remoteKeytabPath = ENV.get(YarnConfigKeys.KEYTAB_PATH);
			LOG.info("TM: remote keytab path obtained {}", remoteKeytabPath);

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
			LOG.info("TM: remote keytab principal obtained {}", remoteKeytabPrincipal);

			final Configuration configuration = GlobalConfiguration.loadConfiguration(currDir);

			// overwrite taskmanager specific configuration

			if (ENV.containsKey(YarnConfigKeys.ENV_JM_ADDRESS)) {
				configuration.setString(JobManagerOptions.ADDRESS, ENV.get(YarnConfigKeys.ENV_JM_ADDRESS));
			}
			if (ENV.containsKey(YarnConfigKeys.ENV_JM_PORT)) {
				configuration.setInteger(JobManagerOptions.PORT, Integer.valueOf(ENV.get(YarnConfigKeys.ENV_JM_PORT)));
			}
			configuration.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, ENV.get(YarnConfigKeys.ENV_TM_REGISTRATION_TIMEOUT));
			if (ENV.containsKey(YarnConfigKeys.ENV_TM_NUM_TASK_SLOT)) {
				configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, Integer.valueOf(ENV.get(YarnConfigKeys.ENV_TM_NUM_TASK_SLOT)));
			}
			configuration.setString(TaskManagerOptions.TASK_MANAGER_RESOURCE_PROFILE_KEY, ENV.get(YarnConfigKeys.ENV_TM_RESOURCE_PROFILE_KEY));
			configuration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, Long.valueOf(ENV.get(YarnConfigKeys.ENV_TM_MANAGED_MEMORY_SIZE)));
			configuration.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, Float.valueOf(ENV.get(YarnConfigKeys.ENV_TM_NETWORK_BUFFERS_MEMORY_FRACTION)));
			configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, Long.valueOf(ENV.get(YarnConfigKeys.ENV_TM_NETWORK_BUFFERS_MEMORY_MIN)));
			configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, Long.valueOf(ENV.get(YarnConfigKeys.ENV_TM_NETWORK_BUFFERS_MEMORY_MAX)));
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, Integer.valueOf(ENV.get(YarnConfigKeys.ENV_TM_PROCESS_NETTY_MEMORY)));
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_CAPACITY_MEMORY_MB, Integer.valueOf(ENV.get(YarnConfigKeys.ENV_TM_CAPACITY_MEMORY_MB)));
			configuration.setDouble(TaskManagerOptions.TASK_MANAGER_CAPACITY_CPU_CORE, Double.valueOf(ENV.get(YarnConfigKeys.ENV_TM_CAPACITY_CPU_CORE)));

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

			if (configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY)) {
				if (configuration.contains(CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS)) {
					LOG.info("Using specified directories {} as root directories for local recovery.",
						configuration.getValue(CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS));
				} else {
					LOG.info("Setting root directories for local recovery as current working dir {}.", currDir);
					configuration.setString(CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS, currDir);
				}
			}

			if (configuration.contains(CheckpointingOptions.WORKING_DIRS)) {
				LOG.info("Using specified directories {} as root directories for all file-based state backend " +
						"in the Flink config: {}.",
					configuration.getValue(CheckpointingOptions.WORKING_DIRS), CheckpointingOptions.WORKING_DIRS.key());
			} else {
				LOG.info("Setting working directories {} for all file-based state backend " +
					"in the Flink config: {}.", currDir, CheckpointingOptions.WORKING_DIRS.key());
				configuration.setString(CheckpointingOptions.WORKING_DIRS, currDir);
			}

			// configure local directory for shuffle service
			configureLocalOutputDirs(configuration, localDirs);

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

			SecurityConfiguration sc = new SecurityConfiguration(configuration);

			if (keytabPath != null && remoteKeytabPrincipal != null) {
				configuration.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
				configuration.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
			}

			final String containerId = ENV.get(YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID);
			Preconditions.checkArgument(containerId != null,
				"ContainerId variable %s not set", YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID);

			// use the hostname passed by job manager
			final String taskExecutorHostname = ENV.get(YarnResourceManager.ENV_FLINK_NODE_ID);
			if (taskExecutorHostname != null) {
				configuration.setString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, taskExecutorHostname);
			}

			SecurityUtils.install(sc);

			SecurityUtils.getInstalledContext().runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					TaskManagerRunner.runTaskManager(configuration, new ResourceID(containerId));
					return null;
				}
			});
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN TaskManager initialization failed.", t);
			System.exit(INIT_ERROR_EXIT_CODE);
		}
	}

	/**
	 * Choose the shuffle service output directory according to the users' preferred disk types from appLocalDirs.
	 * If appLocalDirs do not specify the disk types, it will try to parse the disk type from the local dirs configuration
	 * of the hadoop configuration.
	 *
	 * @param configuration the hadoop configuration.
	 * @param appLocalDirs the local directories parsed from environment variables. These directories contains application id.
	 */
	private static void configureLocalOutputDirs(Configuration configuration, String appLocalDirs) {
		List<String> appDirs = ExternalBlockShuffleServiceConfiguration.splitDiskConfigList(appLocalDirs);
		String expectedDiskType = configuration.getString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_DISK_TYPE).trim();

		if (expectedDiskType.isEmpty()) {
			configuration.setString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS, StringUtils.join(appDirs, ","));
			return;
		}

		org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		String flinkLocalDirs = hadoopConf.get(ExternalBlockShuffleServiceOptions.LOCAL_DIRS.key(), "");
		String nmLocalDirs = hadoopConf.get(YarnConfiguration.NM_LOCAL_DIRS, "");
		String effectiveLocalDirs = flinkLocalDirs.isEmpty() ? nmLocalDirs : flinkLocalDirs;
		Map<String, String> localDirToDirTypes = ExternalBlockShuffleServiceConfiguration.parseDirToDiskType(effectiveLocalDirs);

		List<String> chosenDirs = new ArrayList<>();

		for (String appDir : appDirs) {
			String appDirType = null;

			// See if the prefix of the appDir has been configured in the local dir configuration.
			for (Map.Entry<String, String> entry : localDirToDirTypes.entrySet()) {
				if (appDir.startsWith(entry.getKey())) {
					appDirType = entry.getValue();
					break;
				}
			}

			if (appDirType != null) {
				if (appDirType.equalsIgnoreCase(expectedDiskType)) {
					chosenDirs.add(appDir);
				}
			} else {
				LOG.warn("Encounters an app dir {} whose parents are not configured in the local dir list in yarn", appDir);
			}
		}

		LOG.info("configured flink local dir is {}, config yarn local dir is {}, appLocalDirs is {}, chosenDirs is {}",
			flinkLocalDirs, nmLocalDirs, appLocalDirs, chosenDirs);

		configuration.setString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS, StringUtils.join(chosenDirs, ","));
	}
}
