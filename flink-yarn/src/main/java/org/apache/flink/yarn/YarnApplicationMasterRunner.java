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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.process.ProcessReaper;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Some;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.yarn.Utils.require;

/**
 * This class is the executable entry point for the YARN application master.
 * It starts actor system and the actors for {@link JobManager}
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
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN ApplicationMaster / ResourceManager / JobManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// run and exit with the proper return code
		int returnCode = new YarnApplicationMasterRunner().run(args);
		System.exit(returnCode);
	}

	/**
	 * The instance entry point for the YARN application master. Obtains user group
	 * information and calls the main work method {@link #runApplicationMaster(Configuration)} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			final String yarnClientUsername = ENV.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
			require(yarnClientUsername != null, "YARN client user name environment variable {} not set",
				YarnConfigKeys.ENV_HADOOP_USER_NAME);

			final String currDir = ENV.get(Environment.PWD.key());
			require(currDir != null, "Current working directory variable (%s) not set", Environment.PWD.key());
			LOG.debug("Current working Directory: {}", currDir);

			final String remoteKeytabPath = ENV.get(YarnConfigKeys.KEYTAB_PATH);
			LOG.debug("remoteKeytabPath obtained {}", remoteKeytabPath);

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
			LOG.info("remoteKeytabPrincipal obtained {}", remoteKeytabPrincipal);

			String keytabPath = null;
			if(remoteKeytabPath != null) {
				File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
				keytabPath = f.getAbsolutePath();
				LOG.debug("keytabPath: {}", keytabPath);
			}

			UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

			LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
					currentUser.getShortUserName(), yarnClientUsername );

			// Flink configuration
			final Map<String, String> dynamicProperties =
				FlinkYarnSessionCli.getDynamicProperties(ENV.get(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("YARN dynamic properties: {}", dynamicProperties);

			final Configuration flinkConfig = createConfiguration(currDir, dynamicProperties);

			// set keytab principal and replace path with the local path of the shipped keytab file in NodeManager
			if (keytabPath != null && remoteKeytabPrincipal != null) {
				flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
				flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
			}

			org.apache.hadoop.conf.Configuration hadoopConfiguration = null;

			//To support Yarn Secure Integration Test Scenario
			File krb5Conf = new File(currDir, Utils.KRB5_FILE_NAME);
			if(krb5Conf.exists() && krb5Conf.canRead()) {
				String krb5Path = krb5Conf.getAbsolutePath();
				LOG.info("KRB5 Conf: {}", krb5Path);
				hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
				hadoopConfiguration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
				hadoopConfiguration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
			}

			SecurityUtils.SecurityConfiguration sc;
			if(hadoopConfiguration != null) {
				sc = new SecurityUtils.SecurityConfiguration(flinkConfig, hadoopConfiguration);
			} else {
				sc = new SecurityUtils.SecurityConfiguration(flinkConfig);
			}

			SecurityUtils.install(sc);

			return SecurityUtils.getInstalledContext().runSecured(new Callable<Integer>() {
				@Override
				public Integer call() {
					return runApplicationMaster(flinkConfig);
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
	protected int runApplicationMaster(Configuration config) {
		ActorSystem actorSystem = null;
		WebMonitor webMonitor = null;

		int numberProcessors = Hardware.getNumberCPUCores();

		final ScheduledExecutorService futureExecutor = Executors.newScheduledThreadPool(
			numberProcessors,
			new ExecutorThreadFactory("yarn-jobmanager-future"));

		final ExecutorService ioExecutor = Executors.newFixedThreadPool(
			numberProcessors,
			new ExecutorThreadFactory("yarn-jobmanager-io"));

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

			//Update keytab and principal path to reflect YARN container path location
			final String remoteKeytabPath = ENV.get(YarnConfigKeys.KEYTAB_PATH);

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);

			String keytabPath = null;
			if (remoteKeytabPath != null) {
				File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
				keytabPath = f.getAbsolutePath();
				LOG.info("keytabPath: {}", keytabPath);
			}
			if (keytabPath != null && remoteKeytabPrincipal != null) {
				config.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
				config.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
			}

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

			final ContainerLaunchContext taskManagerContext = Utils.createTaskExecutorContext(
				config, yarnConfig, ENV,
				taskManagerParameters, taskManagerConfig,
				currDir, getTaskManagerClass(), LOG);


			// ---- (4) start the actors and components in this order:

			// 1) Web Monitor (we need its port to register)
			// 2) JobManager & Archive (in non-HA case, the leader service takes this)
			// 3) Resource Master for YARN
			// 4) Process reapers for the JobManager and Resource Master

			// 1: the web monitor
			LOG.debug("Starting Web Frontend");

			webMonitor = BootstrapTools.startWebMonitorIfConfigured(config, actorSystem, LOG);

			String protocol = "http://";
			if (config.getBoolean(ConfigConstants.JOB_MANAGER_WEB_SSL_ENABLED,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_SSL_ENABLED) && SSLUtils.getSSLEnabled(config)) {
				protocol = "https://";
			}
			final String webMonitorURL = webMonitor == null ? null :
				protocol + appMasterHostname + ":" + webMonitor.getServerPort();

			if (webMonitor != null) {
				config.setString(
					ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, String.valueOf(webMonitor.getServerPort()));
			}

			// 2: the JobManager
			LOG.debug("Starting JobManager actor");

			// we start the JobManager with its standard name
			ActorRef jobManager = JobManager.startJobManagerActors(
				config,
				actorSystem,
				futureExecutor,
				ioExecutor,
				new Some<>(JobManager.JOB_MANAGER_NAME()),
				Option.<String>empty(),
				getJobManagerClass(),
				getArchivistClass())._1();

			// 3: Flink's Yarn ResourceManager
			LOG.debug("Starting YARN Flink Resource Manager");

			// we need the leader retrieval service here to be informed of new leaders and session IDs
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

			if (webMonitor != null) {
				try {
					webMonitor.stop();
				} catch (Throwable ignored) {
					LOG.warn("Failed to stop the web frontend", t);
				}
			}

			if (actorSystem != null) {
				try {
					actorSystem.shutdown();
				} catch (Throwable tt) {
					LOG.error("Error shutting down actor system", tt);
				}
			}

			futureExecutor.shutdownNow();
			ioExecutor.shutdownNow();

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

		org.apache.flink.runtime.concurrent.Executors.gracefulShutdown(
			AkkaUtils.getTimeout(config).toMillis(),
			TimeUnit.MILLISECONDS,
			futureExecutor,
			ioExecutor);

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
	 * 
	 * @param baseDirectory
	 * @param additional
	 * 
	 * @return The configuration to be used by the TaskManagers.
	 */
	@SuppressWarnings("deprecation")
	private static Configuration createConfiguration(String baseDirectory, Map<String, String> additional) {
		LOG.info("Loading config from directory " + baseDirectory);

		Configuration configuration = GlobalConfiguration.loadConfiguration(baseDirectory);

		// add dynamic properties to JobManager configuration.
		for (Map.Entry<String, String> property : additional.entrySet()) {
			configuration.setString(property.getKey(), property.getValue());
		}

		// override zookeeper namespace with user cli argument (if provided)
		String cliZKNamespace = ENV.get(YarnConfigKeys.ENV_ZOOKEEPER_NAMESPACE);
		if (cliZKNamespace != null && !cliZKNamespace.isEmpty()) {
			configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, cliZKNamespace);
		}

		// if the user doesn't specify port, set the port to random binding
		if (!configuration.containsKey(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY)) {
			configuration.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, "0");
		}

		// if the user has set the deprecated YARN-specific config keys, we add the 
		// corresponding generic config keys instead. that way, later code needs not
		// deal with deprecated config keys

		BootstrapTools.substituteDeprecatedConfigKey(configuration,
			ConfigConstants.YARN_HEAP_CUTOFF_RATIO,
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_RATIO);

		BootstrapTools.substituteDeprecatedConfigKey(configuration,
			ConfigConstants.YARN_HEAP_CUTOFF_MIN,
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_MIN);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_APPLICATION_MASTER_ENV_PREFIX,
			ConfigConstants.CONTAINERIZED_MASTER_ENV_PREFIX);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_TASK_MANAGER_ENV_PREFIX,
			ConfigConstants.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX);

		return configuration;
	}
}
