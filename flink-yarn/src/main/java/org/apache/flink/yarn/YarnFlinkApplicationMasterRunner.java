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

import akka.actor.ActorSystem;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
 * This class is the executable entry point for the YARN Application Master that
 * executes a single Flink job and then shuts the YARN application down.
 * 
 * <p>The lifetime of the YARN application bound to that of the Flink job. Other
 * YARN Application Master implementations are for example the YARN session.
 * 
 * It starts actor system and the actors for {@link org.apache.flink.runtime.jobmaster.JobManagerRunner}
 * and {@link org.apache.flink.yarn.YarnResourceManager}.
 *
 * The JobMasnagerRunner start a {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * JobMaster handles Flink job execution, while the YarnResourceManager handles container
 * allocation and failure detection.
 */
public class YarnFlinkApplicationMasterRunner extends AbstractYarnFlinkApplicationMasterRunner
		implements OnCompletionActions, FatalErrorHandler {

	/** Logger */
	protected static final Logger LOG = LoggerFactory.getLogger(YarnFlinkApplicationMasterRunner.class);

	/** The job graph file path */
	private static final String JOB_GRAPH_FILE_PATH = "flink.jobgraph.path";

	// ------------------------------------------------------------------------

	/** The lock to guard startup / shutdown / manipulation methods */
	private final Object lock = new Object();

	@GuardedBy("lock")
	private MetricRegistry metricRegistry;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private ResourceManager resourceManager;

	@GuardedBy("lock")
	private JobManagerRunner jobManagerRunner;

	@GuardedBy("lock")
	private JobGraph jobGraph;

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
		int returnCode = new YarnFlinkApplicationMasterRunner().run(args);
		System.exit(returnCode);
	}

	@Override
	protected int runApplicationMaster(Configuration config) {

		try {
			// ---- (1) create common services

			// try to start the rpc service
			// using the port range definition from the config.
			final String amPortRange = config.getString(
					ConfigConstants.YARN_APPLICATION_MASTER_PORT,
					ConfigConstants.DEFAULT_YARN_JOB_MANAGER_PORT);

			synchronized (lock) {
				LOG.info("Starting High Availability Services");
				haServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(config);
				
				metricRegistry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));
				commonRpcService = createRpcService(config, appMasterHostname, amPortRange);

				// ---- (2) init resource manager -------
				resourceManager = createResourceManager(config);

				// ---- (3) init job master parameters
				jobManagerRunner = createJobManagerRunner(config);

				// ---- (4) start the resource manager  and job manager runner:
				resourceManager.start();
				LOG.debug("YARN Flink Resource Manager started");

				jobManagerRunner.start();
				LOG.debug("Job Manager Runner started");

				// ---- (5) start the web monitor
				// TODO: add web monitor
			}

			// wait for resource manager to finish
			resourceManager.getTerminationFuture().get();
			// everything started, we can wait until all is done or the process is killed
			LOG.info("YARN Application Master finished");
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);
			shutdown(ApplicationStatus.FAILED, t.getMessage());
			return INIT_ERROR_EXIT_CODE;
		}

		return 0;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	protected RpcService createRpcService(
			Configuration configuration,
			String bindAddress,
			String portRange) throws Exception{
		ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, bindAddress, portRange, LOG);
		FiniteDuration duration = AkkaUtils.getTimeout(configuration);
		return new AkkaRpcService(actorSystem, Time.of(duration.length(), duration.unit()));
	}

	private ResourceManager<?> createResourceManager(Configuration config) throws Exception {
		final ResourceManagerConfiguration resourceManagerConfiguration = ResourceManagerConfiguration.fromConfiguration(config);
		final SlotManagerFactory slotManagerFactory = new DefaultSlotManager.Factory();
		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(haServices);

		return new YarnResourceManager(config,
				ENV,
				commonRpcService,
				resourceManagerConfiguration,
				haServices,
				slotManagerFactory,
				metricRegistry,
				jobLeaderIdService,
				this);
	}

	private JobManagerRunner createJobManagerRunner(Configuration config) throws Exception{
		// first get JobGraph from local resources
		//TODO: generate the job graph from user's jar
		jobGraph = loadJobGraph(config);

		// we first need to mark the job as running in the HA services, so that the
		// JobManager leader will recognize that it as work to do
		try {
			haServices.getRunningJobsRegistry().setJobRunning(jobGraph.getJobID());
		}
		catch (Throwable t) {
			throw new JobExecutionException(jobGraph.getJobID(),
					"Could not register the job at the high-availability services", t);
		}

		// now the JobManagerRunner
		return new JobManagerRunner(
				jobGraph, config,
				commonRpcService,
				haServices,
				this,
				this);
	}

	protected void shutdown(ApplicationStatus status, String msg) {
		synchronized (lock) {
			if (jobManagerRunner != null) {
				try {
					jobManagerRunner.shutdown();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the JobManagerRunner", tt);
				}
			}
			if (resourceManager != null) {
				try {
					resourceManager.shutDownCluster(status, msg);
					resourceManager.shutDown();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the ResourceManager", tt);
				}
			}
			if (commonRpcService != null) {
				try {
					commonRpcService.stopService();
				} catch (Throwable tt) {
					LOG.error("Error shutting down resource manager rpc service", tt);
				}
			}
			if (haServices != null) {
				try {
					haServices.close();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the HA service", tt);
				}
			}
			if (metricRegistry != null) {
				try {
					metricRegistry.shutdown();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the metrics registry", tt);
				}
			}
		}
	}

	private static JobGraph loadJobGraph(Configuration config) throws Exception {
		JobGraph jg = null;
		String jobGraphFile = config.getString(JOB_GRAPH_FILE_PATH, "job.graph");
		if (jobGraphFile != null) {
			File fp = new File(jobGraphFile);
			if (fp.isFile()) {
				FileInputStream input = new FileInputStream(fp);
				ObjectInputStream obInput = new ObjectInputStream(input);
				jg = (JobGraph) obInput.readObject();
				input.close();
			}
		}
		if (jg == null) {
			throw new Exception("Fail to load job graph " + jobGraphFile);
		}
		return jg;
	}

	//-------------------------------------------------------------------------------------
	// Fatal error handler
	//-------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Encountered fatal error.", exception);

		shutdown(ApplicationStatus.FAILED, exception.getMessage());
	}

	//----------------------------------------------------------------------------------------------
	// Result and error handling methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFinished(JobExecutionResult result) {
		shutdown(ApplicationStatus.SUCCEEDED, null);
	}

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFailed(Throwable cause) {
		shutdown(ApplicationStatus.FAILED, cause.getMessage());
	}

	/**
	 * Job completion notification triggered by self
	 */
	@Override
	public void jobFinishedByOther() {
		shutdown(ApplicationStatus.UNKNOWN, null);
	}
}
