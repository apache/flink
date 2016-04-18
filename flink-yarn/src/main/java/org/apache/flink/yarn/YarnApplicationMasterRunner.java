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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import org.apache.flink.client.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.process.ProcessReaper;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This class is the executable entry point for the YARN application master.
 * It starts actor system and the actors for {@link org.apache.flink.runtime.jobmanager.JobManager}
 * and {@link YarnFlinkResourceManager}.
 * 
 * The JobManager handles Flink job execution, while the YarnFlinkResourceManager handles container
 * allocation and failure detection.
 */
public class YarnApplicationMasterRunner {

	/** Logger */
	protected static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMasterRunner.class);

	/** The maximum time that TaskManagers may be waiting to register at the JobManager,
	 * before they quit */
	private static final FiniteDuration TASKMANAGER_REGISTRATION_TIMEOUT = new FiniteDuration(5, TimeUnit.MINUTES);

	/** The process environment variables */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the application master failed */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	/** The exit code returned if the process exits because a critical actor died */
	private static final int ACTOR_DIED_EXIT_CODE = 32;


	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN application master. 
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN ApplicationMaster / JobManager", args);
		SignalHandler.register(LOG);

		// run and exit with the proper return code
		int returnCode = new YarnApplicationMasterRunner().run(args);
		System.exit(returnCode);
	}

	/**
	 * The instance entry point for the YARN application master. Obtains user group
	 * information and calls the main work method {@link #runApplicationMaster()} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			final String yarnClientUsername = ENV.get(YarnConfigKeys.ENV_CLIENT_USERNAME);
			require(yarnClientUsername != null, "YARN client user name environment variable {} not set",
				YarnConfigKeys.ENV_CLIENT_USERNAME);

			final UserGroupInformation currentUser;
			try {
				currentUser = UserGroupInformation.getCurrentUser();
			} catch (Throwable t) {
				throw new Exception("Cannot access UserGroupInformation information for current user", t);
			}

			LOG.info("YARN daemon runs as user {}. Running Flink Application Master/JobManager as user {}",
				currentUser.getShortUserName(), yarnClientUsername);

			UserGroupInformation ugi = UserGroupInformation.createRemoteUser(yarnClientUsername);

			// transfer all security tokens, for example for authenticated HDFS and HBase access
			for (Token<?> token : currentUser.getTokens()) {
				ugi.addToken(token);
			}

			// run the actual work in a secured privileged action
			return ugi.doAs(new PrivilegedAction<Integer>() {
				@Override
				public Integer run() {
					return runApplicationMaster();
				}
			});
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);
			return INIT_ERROR_EXIT_CODE;
		}
	}

	// ------------------------------------------------------------------------
	//  Core work method
	// ------------------------------------------------------------------------

	/**
	 * The main work method, must run as a privileged action.
	 * 
	 * @return The return code for the Java process. 
	 */
	protected int runApplicationMaster() {
		ActorSystem actorSystem = null;
		WebMonitor webMonitor = null;

		try {
			// ------- (1) load and parse / validate all configurations -------

			// loading all config values here has the advantage that the program fails fast, if any
			// configuration problem occurs

			final String currDir = ENV.get(Environment.PWD.key());
			require(currDir != null, "Current working directory variable (%s) not set", Environment.PWD.key());

			// Note that we use the "appMasterHostname" given by YARN here, to make sure
			// we use the hostnames given by YARN consistently throughout akka.
			// for akka "localhost" and "localhost.localdomain" are different actors.
			final String appMasterHostname = ENV.get(Environment.NM_HOST.key());
			require(appMasterHostname != null,
				"ApplicationMaster hostname variable %s not set", Environment.NM_HOST.key());

			LOG.info("YARN assigned hostname for application master: {}", appMasterHostname);

			// Flink configuration
			final Map<String, String> dynamicProperties =
				CliFrontend.getDynamicProperties(ENV.get(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("YARN dynamic properties: {}", dynamicProperties);

			final Configuration config = createConfiguration(currDir, dynamicProperties);

			// Hadoop/Yarn configuration (loads config data automatically from classpath files)
			final YarnConfiguration yarnConfig = new YarnConfiguration();

			final int taskManagerContainerMemory;
			final int numInitialTaskManagers;
			final int slotsPerTaskManager;

			try {
				taskManagerContainerMemory = Integer.parseInt(ENV.get(YarnConfigKeys.ENV_TM_MEMORY));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + YarnConfigKeys.ENV_TM_MEMORY + " : "
					+ e.getMessage());
			}
			try {
				numInitialTaskManagers = Integer.parseInt(ENV.get(YarnConfigKeys.ENV_TM_COUNT));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + YarnConfigKeys.ENV_TM_COUNT + " : "
					+ e.getMessage());
			}
			try {
				slotsPerTaskManager = Integer.parseInt(ENV.get(YarnConfigKeys.ENV_SLOTS));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + YarnConfigKeys.ENV_SLOTS + " : "
					+ e.getMessage());
			}

			final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(config, taskManagerContainerMemory, slotsPerTaskManager);

			LOG.info("TaskManagers will be created with {} task slots", taskManagerParameters.numSlots());
			LOG.info("TaskManagers will be started with container size {} MB, JVM heap size {} MB, " +
				"JVM direct memory limit {} MB",
				taskManagerParameters.taskManagerTotalMemoryMB(),
				taskManagerParameters.taskManagerHeapSizeMB(),
				taskManagerParameters.taskManagerDirectMemoryLimitMB());


			// ----------------- (2) start the actor system -------------------

			// try to start the actor system, JobManager and JobManager actor system
			// using the port range definition from the config.
			final String amPortRange = config.getString(
					ConfigConstants.YARN_APPLICATION_MASTER_PORT,
					ConfigConstants.DEFAULT_YARN_JOB_MANAGER_PORT);

			actorSystem = BootstrapTools.startActorSystem(config, appMasterHostname, amPortRange, LOG);

			final String akkaHostname = AkkaUtils.getAddress(actorSystem).host().get();
			final int akkaPort = (Integer) AkkaUtils.getAddress(actorSystem).port().get();

			LOG.info("Actor system bound to hostname {}.", akkaHostname);


			// ---- (3) Generate the configuration for the TaskManagers

			final Configuration taskManagerConfig = BootstrapTools.generateTaskManagerConfiguration(
					config, akkaHostname, akkaPort, slotsPerTaskManager, TASKMANAGER_REGISTRATION_TIMEOUT);
			LOG.debug("TaskManager configuration: {}", taskManagerConfig);

			final ContainerLaunchContext taskManagerContext = createTaskManagerContext(
				config, yarnConfig, ENV,
				taskManagerParameters, taskManagerConfig,
				currDir, getTaskManagerClass(), LOG);


			// ---- (4) start the actors and components in this order:

			// 1) JobManager & Archive (in non-HA case, the leader service takes this)
			// 2) Web Monitor (we need its port to register)
			// 3) Resource Master for YARN
			// 4) Process reapers for the JobManager and Resource Master


			// 1: the JobManager
			LOG.debug("Starting JobManager actor");

			// we start the JobManager with its standard name
			ActorRef jobManager = JobManager.startJobManagerActors(
				config, actorSystem,
				new scala.Some<>(JobManager.JOB_MANAGER_NAME()),
				scala.Option.<String>empty(),
				getJobManagerClass(),
				getArchivistClass())._1();


			// 2: the web monitor
			LOG.debug("Starting Web Frontend");

			webMonitor = BootstrapTools.startWebMonitorIfConfigured(config, actorSystem, jobManager, LOG);
			final String webMonitorURL = webMonitor == null ? null :
				"http://" + appMasterHostname + ":" + webMonitor.getServerPort();

			// 3: Flink's Yarn ResourceManager
			LOG.debug("Starting YARN Flink Resource Manager");

			// we need the leader retrieval service here to be informed of new
			// leader session IDs, even though there can be only one leader ever
			LeaderRetrievalService leaderRetriever = 
				LeaderRetrievalUtils.createLeaderRetrievalService(config, jobManager);

			Props resourceMasterProps = YarnFlinkResourceManager.createActorProps(
				getResourceManagerClass(),
				config,
				yarnConfig,
				leaderRetriever,
				appMasterHostname,
				webMonitorURL,
				taskManagerParameters,
				taskManagerContext,
				numInitialTaskManagers, 
				LOG);

			ActorRef resourceMaster = actorSystem.actorOf(resourceMasterProps);


			// 4: Process reapers
			// The process reapers ensure that upon unexpected actor death, the process exits
			// and does not stay lingering around unresponsive

			LOG.debug("Starting process reapers for JobManager and YARN Application Master");

			actorSystem.actorOf(
				Props.create(ProcessReaper.class, resourceMaster, LOG, ACTOR_DIED_EXIT_CODE),
				"YARN_Resource_Master_Process_Reaper");

			actorSystem.actorOf(
				Props.create(ProcessReaper.class, jobManager, LOG, ACTOR_DIED_EXIT_CODE),
				"JobManager_Process_Reaper");
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);

			if (actorSystem != null) {
				try {
					actorSystem.shutdown();
				} catch (Throwable tt) {
					LOG.error("Error shutting down actor system", tt);
				}
			}

			if (webMonitor != null) {
				try {
					webMonitor.stop();
				} catch (Throwable ignored) {
					LOG.warn("Failed to stop the web frontend", t);
				}
			}

			return INIT_ERROR_EXIT_CODE;
		}

		// everything started, we can wait until all is done or the process is killed
		LOG.info("YARN Application Master started");

		// wait until everything is done
		actorSystem.awaitTermination();

		// if we get here, everything work out jolly all right, and we even exited smoothly
		if (webMonitor != null) {
			try {
				webMonitor.stop();
			} catch (Throwable t) {
				LOG.error("Failed to stop the web frontend", t);
			}
		}
		return 0;
	}


	// ------------------------------------------------------------------------
	//  For testing, this allows to override the actor classes used for
	//  JobManager and the archive of completed jobs
	// ------------------------------------------------------------------------

	protected Class<? extends YarnFlinkResourceManager> getResourceManagerClass() {
		return YarnFlinkResourceManager.class;
	}

	protected Class<? extends JobManager> getJobManagerClass() {
		return YarnJobManager.class;
	}

	protected Class<? extends MemoryArchivist> getArchivistClass() {
		return MemoryArchivist.class;
	}

	protected Class<? extends TaskManager> getTaskManagerClass() {
		return YarnTaskManager.class;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Validates a condition, throwing a RuntimeException if the condition is violated.
	 * 
	 * @param condition The condition.
	 * @param message The message for the runtime exception, with format variables as defined by
	 *                {@link String#format(String, Object...)}.
	 * @param values The format arguments.
	 */
	private static void require(boolean condition, String message, Object... values) {
		if (!condition) {
			throw new RuntimeException(String.format(message, values));
		}
	}

	/**
	 * 
	 * @param baseDirectory
	 * @param additional
	 * 
	 * @return The configuration to be used by the TaskManagers.
	 */
	@SuppressWarnings("deprecation")
	private static Configuration createConfiguration(String baseDirectory, Map<String, String> additional) {
		LOG.info("Loading config from directory " + baseDirectory);

		GlobalConfiguration.loadConfiguration(baseDirectory);
		Configuration configuration = GlobalConfiguration.getConfiguration();

		configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, baseDirectory);

		// add dynamic properties to JobManager configuration.
		for (Map.Entry<String, String> property : additional.entrySet()) {
			configuration.setString(property.getKey(), property.getValue());
		}

		// if a web monitor shall be started, set the port to random binding
		if (configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) >= 0) {
			configuration.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0);
		}

		// if the user has set the deprecated YARN-specific config keys, we add the 
		// corresponding generic config keys instead. that way, later code needs not
		// deal with deprecated config keys

		BootstrapTools.substituteDeprecatedConfigKey(configuration,
			ConfigConstants.YARN_HEAP_CUTOFF_RATIO,
			ConfigConstants.CONTAINERED_HEAP_CUTOFF_RATIO);

		BootstrapTools.substituteDeprecatedConfigKey(configuration,
			ConfigConstants.YARN_HEAP_CUTOFF_MIN,
			ConfigConstants.CONTAINERED_HEAP_CUTOFF_MIN);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_APPLICATION_MASTER_ENV_PREFIX,
			ConfigConstants.CONTAINERED_MASTER_ENV_PREFIX);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_TASK_MANAGER_ENV_PREFIX,
			ConfigConstants.CONTAINERED_TASK_MANAGER_ENV_PREFIX);

		return configuration;
	}

	/**
	 * Creates the launch context, which describes how to bring up a TaskManager process in
	 * an allocated YARN container.
	 * 
	 * <p>This code is extremely YARN specific and registers all the resources that the TaskManager
	 * needs (such as JAR file, config file, ...) and all environment variables in a YARN
	 * container launch context. The launch context then ensures that those resources will be
	 * copied into the containers transient working directory. 
	 * 
	 * <p>We do this work before we start the ResourceManager actor in order to fail early if
	 * any of the operations here fail.
	 * 
	 * @param flinkConfig
	 *         The Flink configuration object.
	 * @param yarnConfig
	 *         The YARN configuration object.
	 * @param env
	 *         The environment variables.
	 * @param tmParams
	 *         The TaskManager container memory parameters. 
	 * @param taskManagerConfig
	 *         The configuration for the TaskManagers.
	 * @param workingDirectory
	 *         The current application master container's working directory. 
	 * @param taskManagerMainClass
	 *         The class with the main method.
	 * @param log
	 *         The logger.
	 * 
	 * @return The launch context for the TaskManager processes.
	 * 
	 * @throws Exception Thrown if teh launch context could not be created, for example if
	 *                   the resources could not be copied.
	 */
	public static ContainerLaunchContext createTaskManagerContext(
			Configuration flinkConfig,
			YarnConfiguration yarnConfig,
			Map<String, String> env,
			ContaineredTaskManagerParameters tmParams,
			Configuration taskManagerConfig,
			String workingDirectory,
			Class<?> taskManagerMainClass,
			Logger log) throws Exception {

		log.info("Setting up resources for TaskManagers");

		// get and validate all relevant variables

		String remoteFlinkJarPath = env.get(YarnConfigKeys.FLINK_JAR_PATH);
		require(remoteFlinkJarPath != null, "Environment variable %s not set", YarnConfigKeys.FLINK_JAR_PATH);

		String appId = env.get(YarnConfigKeys.ENV_APP_ID);
		require(appId != null, "Environment variable %s not set", YarnConfigKeys.ENV_APP_ID);

		String clientHomeDir = env.get(YarnConfigKeys.ENV_CLIENT_HOME_DIR);
		require(clientHomeDir != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_HOME_DIR);

		String shipListString = env.get(YarnConfigKeys.ENV_CLIENT_SHIP_FILES);
		require(shipListString != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_SHIP_FILES);

		String yarnClientUsername = env.get(YarnConfigKeys.ENV_CLIENT_USERNAME);
		require(yarnClientUsername != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_USERNAME);

		// obtain a handle to the file system used by YARN
		final org.apache.hadoop.fs.FileSystem yarnFileSystem;
		try {
			yarnFileSystem = org.apache.hadoop.fs.FileSystem.get(yarnConfig);
		} catch (IOException e) {
			throw new Exception("Could not access YARN's default file system", e);
		}

		// register Flink Jar with remote HDFS
		LocalResource flinkJar = Records.newRecord(LocalResource.class);
		{
			Path remoteJarPath = new Path(remoteFlinkJarPath);
			Utils.registerLocalResource(yarnFileSystem, remoteJarPath, flinkJar);
		}

		// register conf with local fs
		LocalResource flinkConf = Records.newRecord(LocalResource.class);
		{
			// write the TaskManager configuration to a local file
			final File taskManagerConfigFile = 
				new File(workingDirectory, UUID.randomUUID() + "-taskmanager-conf.yaml");
			LOG.debug("Writing TaskManager configuration to {}", taskManagerConfigFile.getAbsolutePath());
			BootstrapTools.writeConfiguration(taskManagerConfig, taskManagerConfigFile);

			Utils.setupLocalResource(yarnFileSystem, appId, 
				new Path(taskManagerConfigFile.toURI()), flinkConf, new Path(clientHomeDir));

			log.info("Prepared local resource for modified yaml: {}", flinkConf);
		}

		Map<String, LocalResource> taskManagerLocalResources = new HashMap<>();
		taskManagerLocalResources.put("flink.jar", flinkJar);
		taskManagerLocalResources.put("flink-conf.yaml", flinkConf);

		// prepare additional files to be shipped
		for (String pathStr : shipListString.split(",")) {
			if (!pathStr.isEmpty()) {
				LocalResource resource = Records.newRecord(LocalResource.class);
				Path path = new Path(pathStr);
				Utils.registerLocalResource(yarnFileSystem, path, resource);
				taskManagerLocalResources.put(path.getName(), resource);
			}
		}

		// now that all resources are prepared, we can create the launch context

		log.info("Creating container launch context for TaskManagers");

		boolean hasLogback = new File(workingDirectory, "logback.xml").exists();
		boolean hasLog4j = new File(workingDirectory, "log4j.properties").exists();

		String launchCommand = BootstrapTools.getTaskManagerShellCommand(
			flinkConfig, tmParams, ".", ApplicationConstants.LOG_DIR_EXPANSION_VAR,
			hasLogback, hasLog4j, taskManagerMainClass);

		log.info("Starting TaskManagers with command: " + launchCommand);

		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setCommands(Collections.singletonList(launchCommand));
		ctx.setLocalResources(taskManagerLocalResources);

		Map<String, String> containerEnv = new HashMap<>();
		containerEnv.putAll(tmParams.taskManagerEnv());

		// add YARN classpath, etc to the container environment
		Utils.setupEnv(yarnConfig, containerEnv);
		containerEnv.put(YarnConfigKeys.ENV_CLIENT_USERNAME, yarnClientUsername);

		ctx.setEnvironment(containerEnv);

		try {
			UserGroupInformation user = UserGroupInformation.getCurrentUser();
			Credentials credentials = user.getCredentials();
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.writeTokenStorageToStream(dob);
			ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			ctx.setTokens(securityTokens);
		}
		catch (Throwable t) {
			log.error("Getting current user info failed when trying to launch the container", t);
		}

		return ctx;
	}
}
