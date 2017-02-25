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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServicesUtils;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.overlays.CompositeContainerOverlay;
import org.apache.flink.runtime.clusterframework.overlays.FlinkDistributionOverlay;
import org.apache.flink.runtime.clusterframework.overlays.HadoopConfOverlay;
import org.apache.flink.runtime.clusterframework.overlays.HadoopUserOverlay;
import org.apache.flink.runtime.clusterframework.overlays.KeytabOverlay;
import org.apache.flink.runtime.clusterframework.overlays.Krb5ConfOverlay;
import org.apache.flink.runtime.clusterframework.overlays.SSLStoreOverlay;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.process.ProcessReaper;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.mesos.Protos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is the executable entry point for the Mesos Application Master.
 * It starts actor system and the actors for {@link JobManager}
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
	//  Command-line options
	// ------------------------------------------------------------------------

	private static final Options ALL_OPTIONS;

	static {
		ALL_OPTIONS =
			new Options()
			.addOption(BootstrapTools.newDynamicPropertiesOption());
	}

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
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// run and exit with the proper return code
		int returnCode = new MesosApplicationMasterRunner().run(args);
		System.exit(returnCode);
	}

	/**
	 * The instance entry point for the Mesos AppMaster. Obtains user group
	 * information and calls the main work method {@link #runPrivileged(Configuration,Configuration)} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(final String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			// loading all config values here has the advantage that the program fails fast, if any
			// configuration problem occurs

			CommandLineParser parser = new PosixParser();
			CommandLine cmd = parser.parse(ALL_OPTIONS, args);

			final Configuration dynamicProperties = BootstrapTools.parseDynamicProperties(cmd);
			GlobalConfiguration.setDynamicProperties(dynamicProperties);
			final Configuration config = GlobalConfiguration.loadConfiguration();

			// configure the default filesystem
			try {
				FileSystem.setDefaultScheme(config);
			} catch (IOException e) {
				throw new IOException("Error while setting the default " +
					"filesystem scheme from configuration.", e);
			}

			// configure security
			SecurityUtils.SecurityConfiguration sc = new SecurityUtils.SecurityConfiguration(config);
			SecurityUtils.install(sc);

			// run the actual work in the installed security context
			return SecurityUtils.getInstalledContext().runSecured(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					return runPrivileged(config, dynamicProperties);
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
	protected int runPrivileged(Configuration config, Configuration dynamicProperties) {

		ActorSystem actorSystem = null;
		WebMonitor webMonitor = null;
		MesosArtifactServer artifactServer = null;
		ScheduledExecutorService futureExecutor = null;
		ExecutorService ioExecutor = null;
		MesosServices mesosServices = null;

		try {
			// ------- (1) load and parse / validate all configurations -------

			// Note that we use the "appMasterHostname" given by the system, to make sure
			// we use the hostnames consistently throughout akka.
			// for akka "localhost" and "localhost.localdomain" are different actors.
			final String appMasterHostname = InetAddress.getLocalHost().getHostName();

			// Mesos configuration
			final MesosConfiguration mesosConfig = createMesosConfig(config, appMasterHostname);

			// JM configuration
			int numberProcessors = Hardware.getNumberCPUCores();

			futureExecutor = Executors.newScheduledThreadPool(
				numberProcessors,
				new ExecutorThreadFactory("mesos-jobmanager-future"));

			ioExecutor = Executors.newFixedThreadPool(
				numberProcessors,
				new ExecutorThreadFactory("mesos-jobmanager-io"));

			mesosServices = MesosServicesUtils.createMesosServices(config);

			// TM configuration
			final MesosTaskManagerParameters taskManagerParameters = MesosTaskManagerParameters.create(config);

			LOG.info("TaskManagers will be created with {} task slots",
				taskManagerParameters.containeredParameters().numSlots());
			LOG.info("TaskManagers will be started with container size {} MB, JVM heap size {} MB, " +
					"JVM direct memory limit {} MB, {} cpus",
				taskManagerParameters.containeredParameters().taskManagerTotalMemoryMB(),
				taskManagerParameters.containeredParameters().taskManagerHeapSizeMB(),
				taskManagerParameters.containeredParameters().taskManagerDirectMemoryLimitMB(),
				taskManagerParameters.cpus());

			// JM endpoint, which should be explicitly configured based on acquired net resources
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
			final String artifactServerPrefix = UUID.randomUUID().toString();
			artifactServer = new MesosArtifactServer(artifactServerPrefix, akkaHostname, artifactServerPort, config);

			// ----------------- (3) Generate the configuration for the TaskManagers -------------------

			// generate a container spec which conveys the artifacts/vars needed to launch a TM
			ContainerSpecification taskManagerContainerSpec = new ContainerSpecification();

			// propagate the AM dynamic configuration to the TM
			taskManagerContainerSpec.getDynamicConfiguration().addAll(dynamicProperties);

			// propagate newly-generated configuration elements
			final Configuration taskManagerConfig = BootstrapTools.generateTaskManagerConfiguration(
				new Configuration(), akkaHostname, akkaPort, taskManagerParameters.containeredParameters().numSlots(),
				TASKMANAGER_REGISTRATION_TIMEOUT);
			taskManagerContainerSpec.getDynamicConfiguration().addAll(taskManagerConfig);

			// apply the overlays
			applyOverlays(config, taskManagerContainerSpec);

			// configure the artifact server to serve the specified artifacts
			configureArtifactServer(artifactServer, taskManagerContainerSpec);

			// ----------------- (4) start the actors -------------------

			// 1) JobManager & Archive (in non-HA case, the leader service takes this)
			// 2) Web Monitor (we need its port to register)
			// 3) Resource Master for Mesos
			// 4) Process reapers for the JobManager and Resource Master

			// 1: the JobManager
			LOG.debug("Starting JobManager actor");

			// we start the JobManager with its standard name
			ActorRef jobManager = JobManager.startJobManagerActors(
				config,
				actorSystem,
				futureExecutor,
				ioExecutor,
				new scala.Some<>(JobManager.JOB_MANAGER_NAME()),
				scala.Option.<String>empty(),
				getJobManagerClass(),
				getArchivistClass())._1();


			// 2: the web monitor
			LOG.debug("Starting Web Frontend");

			webMonitor = BootstrapTools.startWebMonitorIfConfigured(config, actorSystem, LOG);
			if(webMonitor != null) {
				final URL webMonitorURL = new URL("http", appMasterHostname, webMonitor.getServerPort(), "/");
				mesosConfig.frameworkInfo().setWebuiUrl(webMonitorURL.toExternalForm());
			}

			// 3: Flink's Mesos ResourceManager
			LOG.debug("Starting Mesos Flink Resource Manager");

			// create the worker store to persist task information across restarts
			MesosWorkerStore workerStore = mesosServices.createMesosWorkerStore(
				config,
				ioExecutor);

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
				taskManagerContainerSpec,
				artifactServer,
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

			if(futureExecutor != null) {
				try {
					futureExecutor.shutdownNow();
				} catch (Throwable tt) {
					LOG.error("Error shutting down future executor", tt);
				}
			}

			if(ioExecutor != null) {
				try {
					ioExecutor.shutdownNow();
				} catch (Throwable tt) {
					LOG.error("Error shutting down io executor", tt);
				}
			}

			if (mesosServices != null) {
				try {
					mesosServices.close(false);
				} catch (Throwable tt) {
					LOG.error("Error closing the mesos services.", tt);
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

		org.apache.flink.runtime.concurrent.Executors.gracefulShutdown(
			AkkaUtils.getTimeout(config).toMillis(),
			TimeUnit.MILLISECONDS,
			futureExecutor,
			ioExecutor);

		try {
			mesosServices.close(true);
		} catch (Throwable t) {
			LOG.error("Failed to clean up and close MesosServices.", t);
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

	/**
	 * Loads and validates the ResourceManager Mesos configuration from the given Flink configuration.
	 */
	public static MesosConfiguration createMesosConfig(Configuration flinkConfig, String hostname) {

		Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
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

		frameworkInfo.setUser(flinkConfig.getString(
			ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_USER,
			ConfigConstants.DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_USER));

		if(flinkConfig.containsKey(ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_PRINCIPAL)) {
			frameworkInfo.setPrincipal(flinkConfig.getString(
				ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_PRINCIPAL, null));

			credential = Protos.Credential.newBuilder();
			credential.setPrincipal(frameworkInfo.getPrincipal());

			// some environments use a side-channel to communicate the secret to Mesos,
			// and thus don't set the 'secret' configuration setting
			if(flinkConfig.containsKey(ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET)) {
				credential.setSecret(flinkConfig.getString(
					ConfigConstants.MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET, null));
			}
		}

		MesosConfiguration mesos =
			new MesosConfiguration(masterUrl, frameworkInfo, scala.Option.apply(credential));

		return mesos;
	}

	/**
	 * Generate a container specification as a TaskManager template.
	 *
	 * <p>This code is extremely Mesos-specific and registers all the artifacts that the TaskManager
	 * needs (such as JAR file, config file, ...) and all environment variables into a container specification.
	 * The Mesos fetcher then ensures that those artifacts will be copied into the task's sandbox directory.
	 * A lightweight HTTP server serves the artifacts to the fetcher.
     */
	private static void applyOverlays(
		Configuration globalConfiguration, ContainerSpecification containerSpec) throws IOException {

		// create the overlays that will produce the specification
		CompositeContainerOverlay overlay = new CompositeContainerOverlay(
			FlinkDistributionOverlay.newBuilder().fromEnvironment(globalConfiguration).build(),
			HadoopConfOverlay.newBuilder().fromEnvironment(globalConfiguration).build(),
			HadoopUserOverlay.newBuilder().fromEnvironment(globalConfiguration).build(),
			KeytabOverlay.newBuilder().fromEnvironment(globalConfiguration).build(),
			Krb5ConfOverlay.newBuilder().fromEnvironment(globalConfiguration).build(),
			SSLStoreOverlay.newBuilder().fromEnvironment(globalConfiguration).build()
		);

		// apply the overlays
		overlay.configure(containerSpec);
	}

	private static void configureArtifactServer(MesosArtifactServer server, ContainerSpecification container) throws IOException {
		// serve the artifacts associated with the container environment
		for(ContainerSpecification.Artifact artifact : container.getArtifacts()) {
			server.addPath(artifact.source, artifact.dest);
		}
	}
}
