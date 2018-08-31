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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.mesos.entrypoint.MesosEntrypointUtils;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServicesUtils;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
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
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaJobManagerRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.util.ExecutorUtils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.dispatch.OnComplete;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.concurrent.Executors.directExecutionContext;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is the executable entry point for the Mesos Application Master.
 * It starts actor system and the actors for {@link JobManager}
 * and {@link MesosFlinkResourceManager}.
 *
 * <p>The JobManager handles Flink job execution, while the MesosFlinkResourceManager handles container
 * allocation and failure detection.
 */
public class MesosApplicationMasterRunner {

	protected static final Logger LOG = LoggerFactory.getLogger(MesosApplicationMasterRunner.class);

	/**
	 * The maximum time that TaskManagers may be waiting to register at the JobManager,
	 * before they quit.
	 */
	private static final FiniteDuration TASKMANAGER_REGISTRATION_TIMEOUT = new FiniteDuration(5, TimeUnit.MINUTES);

	/** The process environment variables. */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the application master failed. */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	/** The exit code returned if the process exits because a critical actor died. */
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
	 * information and calls the main work method {@link #runPrivileged(Configuration, Configuration)} as a
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
			final Configuration config = MesosEntrypointUtils.loadConfiguration(dynamicProperties, LOG);

			// configure the filesystems
			try {
				FileSystem.initialize(config);
			} catch (IOException e) {
				throw new IOException("Error while configuring the filesystems.", e);
			}

			// configure security
			SecurityConfiguration sc = new SecurityConfiguration(config);
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
		HighAvailabilityServices highAvailabilityServices = null;
		MetricRegistryImpl metricRegistry = null;

		try {
			// ------- (1) load and parse / validate all configurations -------

			final String appMasterHostname = config.getString(
				JobManagerOptions.ADDRESS,
				InetAddress.getLocalHost().getHostName());

			LOG.info("App Master Hostname to use: {}", appMasterHostname);

			// Mesos configuration
			final MesosConfiguration mesosConfig = MesosEntrypointUtils.createMesosSchedulerConfiguration(config, appMasterHostname);

			// JM configuration
			int numberProcessors = Hardware.getNumberCPUCores();

			futureExecutor = Executors.newScheduledThreadPool(
				numberProcessors,
				new ExecutorThreadFactory("mesos-jobmanager-future"));

			ioExecutor = Executors.newFixedThreadPool(
				numberProcessors,
				new ExecutorThreadFactory("mesos-jobmanager-io"));

			mesosServices = MesosServicesUtils.createMesosServices(config, appMasterHostname);

			// TM configuration
			final MesosTaskManagerParameters taskManagerParameters = MesosEntrypointUtils.createTmParameters(config, LOG);

			// JM endpoint, which should be explicitly configured based on acquired net resources
			final int listeningPort = config.getInteger(JobManagerOptions.PORT);
			checkState(listeningPort >= 0 && listeningPort <= 65536, "Config parameter \"" +
				JobManagerOptions.PORT.key() + "\" is invalid, it must be between 0 and 65536");

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
			artifactServer = mesosServices.getArtifactServer();

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
			MesosEntrypointUtils.applyOverlays(config, taskManagerContainerSpec);

			// configure the artifact server to serve the specified artifacts
			LaunchableMesosWorker.configureArtifactServer(artifactServer, taskManagerContainerSpec);

			// ----------------- (4) start the actors -------------------

			// 1) JobManager & Archive (in non-HA case, the leader service takes this)
			// 2) Web Monitor (we need its port to register)
			// 3) Resource Master for Mesos
			// 4) Process reapers for the JobManager and Resource Master

			// 0: Start the JobManager services
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
			if (webMonitor != null) {
				final URL webMonitorURL = new URL(webMonitor.getRestAddress());
				mesosConfig.frameworkInfo().setWebuiUrl(webMonitorURL.toExternalForm());
			}

			// 2: the JobManager
			LOG.debug("Starting JobManager actor");

			metricRegistry = new MetricRegistryImpl(
				MetricRegistryConfiguration.fromConfiguration(config));

			metricRegistry.startQueryService(actorSystem, null);

			// we start the JobManager with its standard name
			ActorRef jobManager = JobManager.startJobManagerActors(
				config,
				actorSystem,
				futureExecutor,
				ioExecutor,
				highAvailabilityServices,
				metricRegistry,
				webMonitor != null ? Option.apply(webMonitor.getRestAddress()) : Option.empty(),
				Option.apply(JobMaster.JOB_MANAGER_NAME),
				Option.apply(JobMaster.ARCHIVE_NAME),
				getJobManagerClass(),
				getArchivistClass())._1();

			// 3: Flink's Mesos ResourceManager
			LOG.debug("Starting Mesos Flink Resource Manager");

			// create the worker store to persist task information across restarts
			MesosWorkerStore workerStore = mesosServices.createMesosWorkerStore(
				config,
				ioExecutor);

			Props resourceMasterProps = MesosFlinkResourceManager.createActorProps(
				getResourceManagerClass(),
				config,
				mesosConfig,
				workerStore,
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
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

			if (actorSystem != null) {
				actorSystem.terminate().onComplete(
					new OnComplete<Terminated>() {
						public void onComplete(Throwable failure, Terminated success) {
							if (failure != null) {
								LOG.error("Error shutting down actor system", failure);
							}
						}
					}, directExecutionContext());
			}

			if (futureExecutor != null) {
				try {
					futureExecutor.shutdownNow();
				} catch (Throwable tt) {
					LOG.error("Error shutting down future executor", tt);
				}
			}

			if (ioExecutor != null) {
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
				LOG.error("Could not properly stop the high availability services.");
			}
		}

		if (metricRegistry != null) {
			try {
				metricRegistry.shutdown().get();
			} catch (Throwable t) {
				LOG.error("Could not shut down metric registry.", t);
			}
		}

		ExecutorUtils.gracefulShutdown(
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

}
