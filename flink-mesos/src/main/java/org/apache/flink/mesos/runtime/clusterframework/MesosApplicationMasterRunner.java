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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Props;

import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.mesos.cli.FlinkMesosSessionCli;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.runtime.clusterframework.store.StandaloneMesosWorkerStore;
import org.apache.flink.mesos.runtime.clusterframework.store.ZooKeeperMesosWorkerStore;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.mesos.util.ZooKeeperUtils;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.process.ProcessReaper;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;

import org.apache.hadoop.security.UserGroupInformation;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.net.InetAddress;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.mesos.Utils.uri;
import static org.apache.flink.mesos.Utils.variable;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is the executable entry point for the Mesos Application Master.
 * It starts actor system and the actors for {@link org.apache.flink.runtime.jobmanager.JobManager}
 * and {@link MesosFlinkResourceManager}.
 *
 * The JobManager handles Flink job execution, while the MesosFlinkResourceManager handles container
 * allocation and failure detection.
 */
public class MesosApplicationMasterRunner {
	/** Logger */
	protected static final Logger LOG = LoggerFactory.getLogger(MesosApplicationMasterRunner.class);

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
	 * The entry point for the Mesos AppMaster.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "Mesos AppMaster", args);
		SignalHandler.register(LOG);

		// run and exit with the proper return code
		int returnCode = new MesosApplicationMasterRunner().run(args);
		System.exit(returnCode);
	}

	/**
	 * The instance entry point for the Mesos AppMaster. Obtains user group
	 * information and calls the main work method {@link #runPrivileged()} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			final UserGroupInformation currentUser;
			try {
				currentUser = UserGroupInformation.getCurrentUser();
			} catch (Throwable t) {
				throw new Exception("Cannot access UserGroupInformation information for current user", t);
			}

			LOG.info("Running Flink as user {}", currentUser.getShortUserName());

			// run the actual work in a secured privileged action
			return currentUser.doAs(new PrivilegedAction<Integer>() {
				@Override
				public Integer run() {
					return runPrivileged();
				}
			});
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("Mesos AppMaster initialization failed", t);
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
	protected int runPrivileged() {

		ActorSystem actorSystem = null;
		WebMonitor webMonitor = null;
		MesosArtifactServer artifactServer = null;

		try {
			// ------- (1) load and parse / validate all configurations -------

			// loading all config values here has the advantage that the program fails fast, if any
			// configuration problem occurs

			final String workingDir = ENV.get(MesosConfigKeys.ENV_MESOS_SANDBOX);
			checkState(workingDir != null, "Sandbox directory variable (%s) not set", MesosConfigKeys.ENV_MESOS_SANDBOX);

			final String sessionID = ENV.get(MesosConfigKeys.ENV_SESSION_ID);
			checkState(sessionID != null, "Session ID (%s) not set", MesosConfigKeys.ENV_SESSION_ID);

			// Note that we use the "appMasterHostname" given by the system, to make sure
			// we use the hostnames consistently throughout akka.
			// for akka "localhost" and "localhost.localdomain" are different actors.
			final String appMasterHostname = InetAddress.getLocalHost().getHostName();

			// Flink configuration
			final Configuration dynamicProperties =
				FlinkMesosSessionCli.decodeDynamicProperties(ENV.get(MesosConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("Mesos dynamic properties: {}", dynamicProperties);

			final Configuration config = createConfiguration(workingDir, dynamicProperties);

			// Mesos configuration
			final MesosConfiguration mesosConfig = createMesosConfig(config, appMasterHostname);

			// environment values related to TM
			final int taskManagerContainerMemory;
			final int numInitialTaskManagers;
			final int slotsPerTaskManager;

			try {
				taskManagerContainerMemory = Integer.parseInt(ENV.get(MesosConfigKeys.ENV_TM_MEMORY));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + MesosConfigKeys.ENV_TM_MEMORY + " : "
					+ e.getMessage());
			}
			try {
				numInitialTaskManagers = Integer.parseInt(ENV.get(MesosConfigKeys.ENV_TM_COUNT));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + MesosConfigKeys.ENV_TM_COUNT + " : "
					+ e.getMessage());
			}
			try {
				slotsPerTaskManager = Integer.parseInt(ENV.get(MesosConfigKeys.ENV_SLOTS));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + MesosConfigKeys.ENV_SLOTS + " : "
					+ e.getMessage());
			}

			final ContaineredTaskManagerParameters containeredParameters =
				ContaineredTaskManagerParameters.create(config, taskManagerContainerMemory, slotsPerTaskManager);

			final MesosTaskManagerParameters taskManagerParameters =
				MesosTaskManagerParameters.create(config, containeredParameters);

			LOG.info("TaskManagers will be created with {} task slots",
				taskManagerParameters.containeredParameters().numSlots());
			LOG.info("TaskManagers will be started with container size {} MB, JVM heap size {} MB, " +
					"JVM direct memory limit {} MB, {} cpus",
				taskManagerParameters.containeredParameters().taskManagerTotalMemoryMB(),
				taskManagerParameters.containeredParameters().taskManagerHeapSizeMB(),
				taskManagerParameters.containeredParameters().taskManagerDirectMemoryLimitMB(),
				taskManagerParameters.cpus());

			// JM endpoint, which should be explicitly configured by the dispatcher (based on acquired net resources)
			final int listeningPort = config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
			checkState(listeningPort >= 0 && listeningPort <= 65536, "Config parameter \"" +
				ConfigConstants.JOB_MANAGER_IPC_PORT_KEY + "\" is invalid, it must be between 0 and 65536");

			// ----------------- (2) start the actor system -------------------

			// try to start the actor system, JobManager and JobManager actor system
			// using the configured address and ports
			actorSystem = BootstrapTools.startActorSystem(config, appMasterHostname, listeningPort, LOG);

			Address address = AkkaUtils.getAddress(actorSystem);
			final String akkaHostname = address.host().get();
			final int akkaPort = (Integer) address.port().get();

			LOG.info("Actor system bound to hostname {}.", akkaHostname);

			// try to start the artifact server
			LOG.debug("Starting Artifact Server");
			final int artifactServerPort = config.getInteger(ConfigConstants.MESOS_ARTIFACT_SERVER_PORT_KEY,
				ConfigConstants.DEFAULT_MESOS_ARTIFACT_SERVER_PORT);
			artifactServer = new MesosArtifactServer(sessionID, akkaHostname, artifactServerPort);

			// ----------------- (3) Generate the configuration for the TaskManagers -------------------

			final Configuration taskManagerConfig = BootstrapTools.generateTaskManagerConfiguration(
				config, akkaHostname, akkaPort, slotsPerTaskManager, TASKMANAGER_REGISTRATION_TIMEOUT);
			LOG.debug("TaskManager configuration: {}", taskManagerConfig);

			final Protos.TaskInfo.Builder taskManagerContext = createTaskManagerContext(
				config, ENV,
				taskManagerParameters, taskManagerConfig,
				workingDir, getTaskManagerClass(), artifactServer, LOG);

			// ----------------- (4) start the actors -------------------

			// 1) JobManager & Archive (in non-HA case, the leader service takes this)
			// 2) Web Monitor (we need its port to register)
			// 3) Resource Master for Mesos
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
			if(webMonitor != null) {
				final URL webMonitorURL = new URL("http", appMasterHostname, webMonitor.getServerPort(), "/");
				mesosConfig.frameworkInfo().setWebuiUrl(webMonitorURL.toExternalForm());
			}

			// 3: Flink's Mesos ResourceManager
			LOG.debug("Starting Mesos Flink Resource Manager");

			// create the worker store to persist task information across restarts
			MesosWorkerStore workerStore = createWorkerStore(config);

			// we need the leader retrieval service here to be informed of new
			// leader session IDs, even though there can be only one leader ever
			LeaderRetrievalService leaderRetriever =
				LeaderRetrievalUtils.createLeaderRetrievalService(config, jobManager);

			Props resourceMasterProps = MesosFlinkResourceManager.createActorProps(
				getResourceManagerClass(),
				config,
				mesosConfig,
				workerStore,
				leaderRetriever,
				taskManagerParameters,
				taskManagerContext,
				numInitialTaskManagers,
				LOG);

			ActorRef resourceMaster = actorSystem.actorOf(resourceMasterProps, "Mesos_Resource_Master");

			// 4: Process reapers
			// The process reapers ensure that upon unexpected actor death, the process exits
			// and does not stay lingering around unresponsive

			LOG.debug("Starting process reapers for JobManager");

			actorSystem.actorOf(
				Props.create(ProcessReaper.class, resourceMaster, LOG, ACTOR_DIED_EXIT_CODE),
				"Mesos_Resource_Master_Process_Reaper");

			actorSystem.actorOf(
				Props.create(ProcessReaper.class, jobManager, LOG, ACTOR_DIED_EXIT_CODE),
				"JobManager_Process_Reaper");
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("Mesos JobManager initialization failed", t);

			if (webMonitor != null) {
				try {
					webMonitor.stop();
				} catch (Throwable ignored) {
					LOG.warn("Failed to stop the web frontend", ignored);
				}
			}

			if(artifactServer != null) {
				try {
					artifactServer.stop();
				} catch (Throwable ignored) {
					LOG.error("Failed to stop the artifact server", ignored);
				}
			}

			if (actorSystem != null) {
				try {
					actorSystem.shutdown();
				} catch (Throwable tt) {
					LOG.error("Error shutting down actor system", tt);
				}
			}

			return INIT_ERROR_EXIT_CODE;
		}

		// everything started, we can wait until all is done or the process is killed
		LOG.info("Mesos JobManager started");

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

		try {
			artifactServer.stop();
		} catch (Throwable t) {
			LOG.error("Failed to stop the artifact server", t);
		}

		return 0;
	}

	// ------------------------------------------------------------------------
	//  For testing, this allows to override the actor classes used for
	//  JobManager and the archive of completed jobs
	// ------------------------------------------------------------------------

	protected Class<? extends MesosFlinkResourceManager> getResourceManagerClass() {
		return MesosFlinkResourceManager.class;
	}

	protected Class<? extends JobManager> getJobManagerClass() {
		return MesosJobManager.class;
	}

	protected Class<? extends MemoryArchivist> getArchivistClass() {
		return MemoryArchivist.class;
	}

	protected Class<? extends TaskManager> getTaskManagerClass() {
		return MesosTaskManager.class;
	}

	/**
	 *
	 * @param baseDirectory
	 * @param additional
	 *
	 * @return The configuration to be used by the TaskManagers.
	 */
	private static Configuration createConfiguration(String baseDirectory, Configuration additional) {
		LOG.info("Loading config from directory {}", baseDirectory);

		Configuration configuration = GlobalConfiguration.loadConfiguration(baseDirectory);

		configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, baseDirectory);

		// add dynamic properties to JobManager configuration.
		configuration.addAll(additional);

		return configuration;
	}

	/**
	 * Loads and validates the ResourceManager Mesos configuration from the given Flink configuration.
	 */
	public static MesosConfiguration createMesosConfig(Configuration flinkConfig, String hostname) {

		Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
			.setUser("")
			.setHostname(hostname);
		Protos.Credential.Builder credential = null;

		if(!flinkConfig.containsKey(ConfigConstants.MESOS_MASTER_URL)) {
			throw new IllegalConfigurationException(ConfigConstants.MESOS_MASTER_URL + " must be configured.");
		}
		String masterUrl = flinkConfig.getString(ConfigConstants.MESOS_MASTER_URL, null);

		Duration failoverTimeout = FiniteDuration.apply(
			flinkConfig.getInteger(
				ConfigConstants.MESOS_FAILOVER_TIMEOUT_SECONDS,
				ConfigConstants.DEFAULT_MESOS_FAILOVER_TIMEOUT_SECS),
			TimeUnit.SECONDS);
		frameworkInfo.setFailoverTimeout(failoverTimeout.toSeconds());

		frameworkInfo.setName(flinkConfig.getString(
			ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_NAME,
			ConfigConstants.DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_NAME));

		frameworkInfo.setRole(flinkConfig.getString(
			ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE,
			ConfigConstants.DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE));

		if(flinkConfig.containsKey(ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_PRINCIPAL)) {
			frameworkInfo.setPrincipal(flinkConfig.getString(
				ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_PRINCIPAL, null));

			credential = Protos.Credential.newBuilder();
			credential.setPrincipal(frameworkInfo.getPrincipal());

			if(!flinkConfig.containsKey(ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET)) {
				throw new IllegalConfigurationException(ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET + " must be configured.");
			}
			credential.setSecret(flinkConfig.getString(
				ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET, null));
		}

		MesosConfiguration mesos =
			new MesosConfiguration(masterUrl, frameworkInfo, Option.apply(credential));

		return mesos;
	}

	private static MesosWorkerStore createWorkerStore(Configuration flinkConfig) throws Exception {
		MesosWorkerStore workerStore;
		HighAvailabilityMode recoveryMode = HighAvailabilityMode.fromConfig(flinkConfig);
		if (recoveryMode == HighAvailabilityMode.NONE) {
			workerStore = new StandaloneMesosWorkerStore();
		}
		else if (recoveryMode == HighAvailabilityMode.ZOOKEEPER) {
			// note: the store is responsible for closing the client.
			CuratorFramework client = ZooKeeperUtils.startCuratorFramework(flinkConfig);
			workerStore = ZooKeeperMesosWorkerStore.createMesosWorkerStore(client, flinkConfig);
		}
		else {
			throw new IllegalConfigurationException("Unexpected recovery mode '" + recoveryMode + ".");
		}

		return workerStore;
	}

	/**
	 * Creates a Mesos task info template, which describes how to bring up a TaskManager process as
	 * a Mesos task.
	 *
	 * <p>This code is extremely Mesos-specific and registers all the artifacts that the TaskManager
	 * needs (such as JAR file, config file, ...) and all environment variables in a task info record.
	 * The Mesos fetcher then ensures that those artifacts will be copied into the task's sandbox directory.
	 * A lightweight HTTP server serves the artifacts to the fetcher.
	 *
	 * <p>We do this work before we start the ResourceManager actor in order to fail early if
	 * any of the operations here fail.
	 *
	 * @param flinkConfig
	 *         The Flink configuration object.
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
	 * @param artifactServer
	 *         The artifact server.
	 * @param log
	 *         The logger.
	 *
	 * @return The task info template for the TaskManager processes.
	 *
	 * @throws Exception Thrown if the task info could not be created, for example if
	 *                   the resources could not be copied.
	 */
	public static Protos.TaskInfo.Builder createTaskManagerContext(
		Configuration flinkConfig,
		Map<String, String> env,
		MesosTaskManagerParameters tmParams,
		Configuration taskManagerConfig,
		String workingDirectory,
		Class<?> taskManagerMainClass,
		MesosArtifactServer artifactServer,
		Logger log) throws Exception {


		Protos.TaskInfo.Builder info = Protos.TaskInfo.newBuilder();
		Protos.CommandInfo.Builder cmd = Protos.CommandInfo.newBuilder();

		log.info("Setting up artifacts for TaskManagers");

		String shipListString = env.get(MesosConfigKeys.ENV_CLIENT_SHIP_FILES);
		checkState(shipListString != null, "Environment variable %s not set", MesosConfigKeys.ENV_CLIENT_SHIP_FILES);

		String clientUsername = env.get(MesosConfigKeys.ENV_CLIENT_USERNAME);
		checkState(clientUsername != null, "Environment variable %s not set", MesosConfigKeys.ENV_CLIENT_USERNAME);

		String classPathString = env.get(MesosConfigKeys.ENV_FLINK_CLASSPATH);
		checkState(classPathString != null, "Environment variable %s not set", MesosConfigKeys.ENV_FLINK_CLASSPATH);

		// register the Flink jar
		final File flinkJarFile = new File(workingDirectory, "flink.jar");
		cmd.addUris(uri(artifactServer.addFile(flinkJarFile, "flink.jar"), true));

		// register the TaskManager configuration
		final File taskManagerConfigFile =
			new File(workingDirectory, UUID.randomUUID() + "-taskmanager-conf.yaml");
		LOG.debug("Writing TaskManager configuration to {}", taskManagerConfigFile.getAbsolutePath());
		BootstrapTools.writeConfiguration(taskManagerConfig, taskManagerConfigFile);
		cmd.addUris(uri(artifactServer.addFile(taskManagerConfigFile, GlobalConfiguration.FLINK_CONF_FILENAME), true));

		// prepare additional files to be shipped
		for (String pathStr : shipListString.split(",")) {
			if (!pathStr.isEmpty()) {
				File shipFile = new File(workingDirectory, pathStr);
				cmd.addUris(uri(artifactServer.addFile(shipFile, shipFile.getName()), true));
			}
		}

		log.info("Creating task info for TaskManagers");

		// build the launch command
		boolean hasLogback = new File(workingDirectory, "logback.xml").exists();
		boolean hasLog4j = new File(workingDirectory, "log4j.properties").exists();

		String launchCommand = BootstrapTools.getTaskManagerShellCommand(
			flinkConfig, tmParams.containeredParameters(), ".", ".",
			hasLogback, hasLog4j, taskManagerMainClass);
		cmd.setValue(launchCommand);

		// build the environment variables
		Protos.Environment.Builder envBuilder = Protos.Environment.newBuilder();
		for (Map.Entry<String, String> entry : tmParams.containeredParameters().taskManagerEnv().entrySet()) {
			envBuilder.addVariables(variable(entry.getKey(), entry.getValue()));
		}
		envBuilder.addVariables(variable(MesosConfigKeys.ENV_CLASSPATH, classPathString));
		envBuilder.addVariables(variable(MesosConfigKeys.ENV_CLIENT_USERNAME, clientUsername));

		cmd.setEnvironment(envBuilder);

		info.setCommand(cmd);

		return info;
	}
}
