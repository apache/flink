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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.process.ProcessReaper;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaJobManagerRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.dispatch.OnComplete;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Option;
import scala.Some;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.concurrent.Executors.directExecutionContext;
import static org.apache.flink.yarn.Utils.require;

/**
 * This class is the executable entry point for the YARN application master.
 * It starts actor system and the actors for {@link JobManager}
 * and {@link YarnFlinkResourceManager}.
 *
 * <p>The JobManager handles Flink job execution, while the YarnFlinkResourceManager handles container
 * allocation and failure detection.
 */
public class YarnApplicationMasterRunner {

	protected static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMasterRunner.class);

	/** The maximum time that TaskManagers may be waiting to register at the JobManager,
	 * before they quit. */
	private static final FiniteDuration TASKMANAGER_REGISTRATION_TIMEOUT = new FiniteDuration(5, TimeUnit.MINUTES);

	/** The process environment variables. */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the application master failed. */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	/** The exit code returned if the process exits because a critical actor died. */
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
			require(yarnClientUsername != null, "YARN client user name environment variable (%s) not set",
				YarnConfigKeys.ENV_HADOOP_USER_NAME);

			final String currDir = ENV.get(Environment.PWD.key());
			require(currDir != null, "Current working directory variable (%s) not set", Environment.PWD.key());
			LOG.debug("Current working Directory: {}", currDir);

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
			LOG.info("remoteKeytabPrincipal obtained {}", remoteKeytabPrincipal);

			UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

			LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
					currentUser.getShortUserName(), yarnClientUsername);

			// Flink configuration
			final Map<String, String> dynamicProperties =
				FlinkYarnSessionCli.getDynamicProperties(ENV.get(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("YARN dynamic properties: {}", dynamicProperties);

			final Configuration flinkConfig = createConfiguration(currDir, dynamicProperties, LOG);

			// configure the filesystems
			try {
				FileSystem.initialize(flinkConfig);
			} catch (IOException e) {
				throw new IOException("Error while configuring the filesystems.", e);
			}

			File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
			if (remoteKeytabPrincipal != null && f.exists()) {
				String keytabPath = f.getAbsolutePath();
				LOG.debug("keytabPath: {}", keytabPath);

				// set keytab principal and replace path with the local path of the shipped keytab file in NodeManager
				flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, f.getAbsolutePath());
				flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
			}

			SecurityConfiguration sc = new SecurityConfiguration(flinkConfig);

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
		HighAvailabilityServices highAvailabilityServices = null;
		MetricRegistryImpl metricRegistry = null;

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

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);

			File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
			if (remoteKeytabPrincipal != null && f.exists()) {
				String keytabPath = f.getAbsolutePath();
				LOG.debug("keytabPath: {}", keytabPath);

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
					YarnConfigOptions.APPLICATION_MASTER_PORT);

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

			// 1) JobManager & Archive (in non-HA case, the leader service takes this)
			// 2) Web Monitor (we need its port to register)
			// 3) Resource Master for YARN
			// 4) Process reapers for the JobManager and Resource Master

			// 0: Start the JobManager services

			// update the configuration used to create the high availability services
			config.setString(JobManagerOptions.ADDRESS, akkaHostname);
			config.setInteger(JobManagerOptions.PORT, akkaPort);

			highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
				config,
				ioExecutor,
				HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

			// 1: the web monitor
			LOG.debug("Starting Web Frontend");

			Time webMonitorTimeout = Time.milliseconds(config.getLong(WebOptions.TIMEOUT));

			webMonitor = BootstrapTools.startWebMonitorIfConfigured(
				config,
				highAvailabilityServices,
				new AkkaJobManagerRetriever(actorSystem, webMonitorTimeout, 10, Time.milliseconds(50L)),
				new AkkaQueryServiceRetriever(actorSystem, webMonitorTimeout),
				webMonitorTimeout,
				new ScheduledExecutorServiceAdapter(futureExecutor),
				LOG);

			metricRegistry = new MetricRegistryImpl(
				MetricRegistryConfiguration.fromConfiguration(config));

			metricRegistry.startQueryService(actorSystem, null);

			// 2: the JobManager
			LOG.debug("Starting JobManager actor");

			// we start the JobManager with its standard name
			ActorRef jobManager = JobManager.startJobManagerActors(
				config,
				actorSystem,
				futureExecutor,
				ioExecutor,
				highAvailabilityServices,
				metricRegistry,
				webMonitor == null ? Option.empty() : Option.apply(webMonitor.getRestAddress()),
				new Some<>(JobMaster.JOB_MANAGER_NAME),
				Option.<String>empty(),
				getJobManagerClass(),
				getArchivistClass())._1();

			final String webMonitorURL = webMonitor == null ? null : webMonitor.getRestAddress();

			// 3: Flink's Yarn ResourceManager
			LOG.debug("Starting YARN Flink Resource Manager");

			Props resourceMasterProps = YarnFlinkResourceManager.createActorProps(
				getResourceManagerClass(),
				config,
				yarnConfig,
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
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
				actorSystem.terminate().onComplete(
					new OnComplete<Terminated>() {
						public void onComplete(Throwable failure, Terminated result) {
							if (failure != null) {
								LOG.error("Error shutting down actor system", failure);
							}
						}
					}, directExecutionContext());
			}

			futureExecutor.shutdownNow();
			ioExecutor.shutdownNow();

			return INIT_ERROR_EXIT_CODE;
		}

		// everything started, we can wait until all is done or the process is killed
		LOG.info("YARN Application Master started");

		// wait until everything is done
		try {
			Await.ready(actorSystem.whenTerminated(), Duration.Inf());
		} catch (InterruptedException | TimeoutException e) {
			LOG.error("Error shutting down actor system", e);
		}

		// if we get here, everything work out jolly all right, and we even exited smoothly
		if (webMonitor != null) {
			try {
				webMonitor.stop();
			} catch (Throwable t) {
				LOG.error("Failed to stop the web frontend", t);
			}
		}

		if (highAvailabilityServices != null) {
			try {
				highAvailabilityServices.close();
			} catch (Throwable t) {
				LOG.error("Failed to stop the high availability services.", t);
			}
		}

		if (metricRegistry != null) {
			try {
				metricRegistry.shutdown().get();
			} catch (Throwable t) {
				LOG.error("Could not properly shut down the metric registry.", t);
			}
		}

		ExecutorUtils.gracefulShutdown(
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
	 * Reads the global configuration from the given directory and adds the given parameters to it.
	 *
	 * @param baseDirectory directory to load the configuration from
	 * @param additional additional parameters to be included in the configuration
	 * @param log logger instance
	 *
	 * @return The configuration to be used by the TaskManagers.
	 */
	@SuppressWarnings("deprecation")
	private static Configuration createConfiguration(String baseDirectory, Map<String, String> additional, Logger log) {
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

		// if a web monitor shall be started, set the port to random binding
		if (configuration.getInteger(WebOptions.PORT, 0) >= 0) {
			configuration.setInteger(WebOptions.PORT, 0);
		}

		// if the user has set the deprecated YARN-specific config keys, we add the
		// corresponding generic config keys instead. that way, later code needs not
		// deal with deprecated config keys

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_APPLICATION_MASTER_ENV_PREFIX,
			ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_TASK_MANAGER_ENV_PREFIX,
			ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX);

		final String localDirs = ENV.get(Environment.LOCAL_DIRS.key());
		BootstrapTools.updateTmpDirectoriesInConfiguration(configuration, localDirs);

		return configuration;
	}
}
