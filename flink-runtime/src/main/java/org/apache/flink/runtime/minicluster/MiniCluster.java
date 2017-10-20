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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderelection.LeaderAddressAndId;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRunner;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.util.ExceptionUtils;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class MiniCluster {

	private static final Logger LOG = LoggerFactory.getLogger(MiniCluster.class);

	/** The lock to guard startup / shutdown / manipulation methods */
	private final Object lock = new Object();

	/** The configuration for this mini cluster */
	private final MiniClusterConfiguration miniClusterConfiguration;

	@GuardedBy("lock") 
	private MetricRegistryImpl metricRegistry;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private RpcService[] jobManagerRpcServices;

	@GuardedBy("lock")
	private RpcService[] taskManagerRpcServices;

	@GuardedBy("lock")
	private RpcService[] resourceManagerRpcServices;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private BlobServer blobServer;

	@GuardedBy("lock")
	private HeartbeatServices heartbeatServices;

	@GuardedBy("lock")
	private ResourceManagerRunner[] resourceManagerRunners;

	private volatile TaskExecutor[] taskManagers;

	@GuardedBy("lock")
	private MiniClusterJobDispatcher jobDispatcher;

	/** Flag marking the mini cluster as started/running */
	private volatile boolean running;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Flink mini cluster based on the given configuration.
	 * 
	 * @param miniClusterConfiguration The configuration for the mini cluster
	 */
	public MiniCluster(MiniClusterConfiguration miniClusterConfiguration) {
		this.miniClusterConfiguration = checkNotNull(miniClusterConfiguration, "config may not be null");

		running = false;
	}

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	/**
	 * Checks if the mini cluster was started and is running.
	 */
	public boolean isRunning() {
		return running;
	}

	/**
	 * Starts the mini cluster, based on the configured properties.
	 * 
	 * @throws Exception This method passes on any exception that occurs during the startup of
	 *                   the mini cluster.
	 */
	public void start() throws Exception {
		synchronized (lock) {
			checkState(!running, "FlinkMiniCluster is already running");

			LOG.info("Starting Flink Mini Cluster");
			LOG.debug("Using configuration {}", miniClusterConfiguration);

			final Configuration configuration = miniClusterConfiguration.getConfiguration();
			final Time rpcTimeout = miniClusterConfiguration.getRpcTimeout();
			final int numJobManagers = miniClusterConfiguration.getNumJobManagers();
			final int numTaskManagers = miniClusterConfiguration.getNumTaskManagers();
			final int numResourceManagers = miniClusterConfiguration.getNumResourceManagers();
			final boolean useSingleRpcService = miniClusterConfiguration.getRpcServiceSharing() == MiniClusterConfiguration.RpcServiceSharing.SHARED;

			try {
				LOG.info("Starting Metrics Registry");
				metricRegistry = createMetricRegistry(configuration);

				RpcService[] jobManagerRpcServices = new RpcService[numJobManagers];
				RpcService[] taskManagerRpcServices = new RpcService[numTaskManagers];
				RpcService[] resourceManagerRpcServices = new RpcService[numResourceManagers];

				// bring up all the RPC services
				LOG.info("Starting RPC Service(s)");

				// we always need the 'commonRpcService' for auxiliary calls
				commonRpcService = createRpcService(configuration, rpcTimeout, false, null);

				// TODO: Temporary hack until the metric query service is ported to the RpcEndpoint
				final ActorSystem actorSystem = ((AkkaRpcService) commonRpcService).getActorSystem();
				metricRegistry.startQueryService(actorSystem, null);

				if (useSingleRpcService) {
					// set that same RPC service for all JobManagers and TaskManagers
					for (int i = 0; i < numJobManagers; i++) {
						jobManagerRpcServices[i] = commonRpcService;
					}
					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = commonRpcService;
					}
					for (int i = 0; i < numResourceManagers; i++) {
						resourceManagerRpcServices[i] = commonRpcService;
					}

					this.resourceManagerRpcServices = null;
					this.jobManagerRpcServices = null;
					this.taskManagerRpcServices = null;
				}
				else {
					// start a new service per component, possibly with custom bind addresses
					final String jobManagerBindAddress = miniClusterConfiguration.getJobManagerBindAddress();
					final String taskManagerBindAddress = miniClusterConfiguration.getTaskManagerBindAddress();
					final String resourceManagerBindAddress = miniClusterConfiguration.getResourceManagerBindAddress();

					for (int i = 0; i < numJobManagers; i++) {
						jobManagerRpcServices[i] = createRpcService(
								configuration, rpcTimeout, true, jobManagerBindAddress);
					}

					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = createRpcService(
								configuration, rpcTimeout, true, taskManagerBindAddress);
					}

					for (int i = 0; i < numResourceManagers; i++) {
						resourceManagerRpcServices[i] = createRpcService(
								configuration, rpcTimeout, true, resourceManagerBindAddress);
					}

					this.jobManagerRpcServices = jobManagerRpcServices;
					this.taskManagerRpcServices = taskManagerRpcServices;
					this.resourceManagerRpcServices = resourceManagerRpcServices;
				}

				// create the high-availability services
				LOG.info("Starting high-availability services");
				haServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
					configuration,
					commonRpcService.getExecutor());

				blobServer = new BlobServer(configuration, haServices.createBlobStore());
				blobServer.start();

				heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

				// bring up the ResourceManager(s)
				LOG.info("Starting {} ResourceManger(s)", numResourceManagers);
				resourceManagerRunners = startResourceManagers(
					configuration,
					haServices,
					heartbeatServices,
					metricRegistry,
					numResourceManagers,
					resourceManagerRpcServices);

				// bring up the TaskManager(s) for the mini cluster
				LOG.info("Starting {} TaskManger(s)", numTaskManagers);
				taskManagers = startTaskManagers(
						configuration, haServices, metricRegistry, numTaskManagers, taskManagerRpcServices);

				// bring up the dispatcher that launches JobManagers when jobs submitted
				LOG.info("Starting job dispatcher(s) for {} JobManger(s)", numJobManagers);
				jobDispatcher = new MiniClusterJobDispatcher(
					configuration,
					haServices,
					blobServer,
					heartbeatServices,
					metricRegistry,
					numJobManagers,
					jobManagerRpcServices);
			}
			catch (Exception e) {
				// cleanup everything
				try {
					shutdownInternally();
				} catch (Exception ee) {
					e.addSuppressed(ee);
				}
				throw e;
			}

			// now officially mark this as running
			running = true;

			LOG.info("Flink Mini Cluster started successfully");
		}
	}

	/**
	 * Shuts down the mini cluster, failing all currently executing jobs.
	 * The mini cluster can be started again by calling the {@link #start()} method again.
	 * 
	 * <p>This method shuts down all started services and components,
	 * even if an exception occurs in the process of shutting down some component. 
	 * 
	 * @throws Exception Thrown, if the shutdown did not complete cleanly.
	 */
	public void shutdown() throws Exception {
		synchronized (lock) {
			if (running) {
				LOG.info("Shutting down Flink Mini Cluster");
				try {
					shutdownInternally();
				} finally {
					running = false;
				}
				LOG.info("Flink Mini Cluster is shut down");
			}
		}
	}

	@GuardedBy("lock")
	private void shutdownInternally() throws Exception {
		// this should always be called under the lock
		assert Thread.holdsLock(lock);

		// collect the first exception, but continue and add all successive
		// exceptions as suppressed
		Throwable exception = null;

		// cancel all jobs and shut down the job dispatcher
		if (jobDispatcher != null) {
			try {
				jobDispatcher.shutdown();
			} catch (Exception e) {
				exception = e;
			}
			jobDispatcher = null;
		}

		if (resourceManagerRunners != null) {
			for (ResourceManagerRunner rm : resourceManagerRunners) {
				if (rm != null) {
					try {
						rm.shutDown();
					} catch (Throwable t) {
						exception = firstOrSuppressed(t, exception);
					}
				}
			}
			resourceManagerRunners = null;
		}

		if (taskManagers != null) {
			for (TaskExecutor tm : taskManagers) {
				if (tm != null) {
					try {
						tm.shutDown();
						// wait for the TaskManager to properly terminate
						tm.getTerminationFuture().get();
					} catch (Throwable t) {
						exception = firstOrSuppressed(t, exception);
					}
				}
			}
			taskManagers = null;
		}

		// metrics shutdown
		if (metricRegistry != null) {
			metricRegistry.shutdown();
			metricRegistry = null;
		}

		// shut down the RpcServices
		exception = shutDownRpc(commonRpcService, exception);
		exception = shutDownRpcs(jobManagerRpcServices, exception);
		exception = shutDownRpcs(taskManagerRpcServices, exception);
		exception = shutDownRpcs(resourceManagerRpcServices, exception);
		commonRpcService = null;
		jobManagerRpcServices = null;
		taskManagerRpcServices = null;
		resourceManagerRpcServices = null;

		// shut down the blob server
		if (blobServer != null) {
			try {
				blobServer.close();
			} catch (Exception e) {
				exception = firstOrSuppressed(e, exception);
			}
			blobServer = null;
		}

		// shut down high-availability services
		if (haServices != null) {
			try {
				haServices.closeAndCleanupAllData();
			} catch (Exception e) {
				exception = firstOrSuppressed(e, exception);
			}
			haServices = null;
		}

		// if anything went wrong, throw the first error with all the additional suppressed exceptions
		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Error while shutting down mini cluster");
		}
	}

	public void waitUntilTaskManagerRegistrationsComplete() throws Exception {
		LeaderRetrievalService rmMasterListener = null;
		CompletableFuture<LeaderAddressAndId> addressAndIdFuture;

		try {
			synchronized (lock) {
				checkState(running, "FlinkMiniCluster is not running");

				OneTimeLeaderListenerFuture listenerFuture = new OneTimeLeaderListenerFuture();
				rmMasterListener = haServices.getResourceManagerLeaderRetriever();
				rmMasterListener.start(listenerFuture);
				addressAndIdFuture = listenerFuture.future(); 
			}

			final LeaderAddressAndId addressAndId = addressAndIdFuture.get();

			final ResourceManagerGateway resourceManager = 
					commonRpcService.connect(addressAndId.leaderAddress(), new ResourceManagerId(addressAndId.leaderId()), ResourceManagerGateway.class).get();

			final int numTaskManagersToWaitFor = taskManagers.length;

			// poll and wait until enough TaskManagers are available
			while (true) {
				int numTaskManagersAvailable = 
						resourceManager.getNumberOfRegisteredTaskManagers().get();

				if (numTaskManagersAvailable >= numTaskManagersToWaitFor) {
					break;
				}
				Thread.sleep(2);
			}
		}
		finally {
			try {
				if (rmMasterListener != null) {
					rmMasterListener.stop();
				}
			} catch (Exception e) {
				LOG.warn("Error shutting down leader listener for ResourceManager");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  running jobs
	// ------------------------------------------------------------------------

	/**
	 * This method executes a job in detached mode. The method returns immediately after the job
	 * has been added to the
	 *
	 * @param job  The Flink job to execute
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	public void runDetached(JobGraph job) throws JobExecutionException {
		checkNotNull(job, "job is null");

		synchronized (lock) {
			checkState(running, "mini cluster is not running");
			jobDispatcher.runDetached(job);
		}
	}

	/**
	 * This method runs a job in blocking mode. The method returns only after the job
	 * completed successfully, or after it failed terminally.
	 *
	 * @param job  The Flink job to execute 
	 * @return The result of the job execution
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	public JobExecutionResult runJobBlocking(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job, "job is null");

		MiniClusterJobDispatcher dispatcher;
		synchronized (lock) {
			checkState(running, "mini cluster is not running");
			dispatcher = this.jobDispatcher;
		}

		return dispatcher.runJobBlocking(job);
	}

	// ------------------------------------------------------------------------
	//  factories - can be overridden by subclasses to alter behavior
	// ------------------------------------------------------------------------

	/**
	 * Factory method to create the metric registry for the mini cluster
	 * 
	 * @param config The configuration of the mini cluster
	 */
	protected MetricRegistryImpl createMetricRegistry(Configuration config) {
		return new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));
	}

	/**
	 * Factory method to instantiate the RPC service.
	 * 
	 * @param configuration
	 *            The configuration of the mini cluster
	 * @param askTimeout
	 *            The default RPC timeout for asynchronous "ask" requests.
	 * @param remoteEnabled
	 *            True, if the RPC service should be reachable from other (remote) RPC services.
	 * @param bindAddress
	 *            The address to bind the RPC service to. Only relevant when "remoteEnabled" is true.
	 * 
	 * @return The instantiated RPC service
	 */
	protected RpcService createRpcService(
			Configuration configuration,
			Time askTimeout,
			boolean remoteEnabled,
			String bindAddress) {

		ActorSystem actorSystem;
		if (remoteEnabled) {
			actorSystem = AkkaUtils.createActorSystem(configuration, bindAddress, 0);
		} else {
			actorSystem = AkkaUtils.createLocalActorSystem(configuration);
		}

		return new AkkaRpcService(actorSystem, askTimeout);
	}

	protected ResourceManagerRunner[] startResourceManagers(
			Configuration configuration,
			HighAvailabilityServices haServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			int numResourceManagers,
			RpcService[] resourceManagerRpcServices) throws Exception {

		final ResourceManagerRunner[] resourceManagerRunners = new ResourceManagerRunner[numResourceManagers];

		for (int i = 0; i < numResourceManagers; i++) {

			resourceManagerRunners[i] = new ResourceManagerRunner(
				ResourceID.generate(),
				FlinkResourceManager.RESOURCE_MANAGER_NAME + '_' + i,
				configuration,
				resourceManagerRpcServices[i],
				haServices,
				heartbeatServices,
				metricRegistry);

			resourceManagerRunners[i].start();
		}

		return resourceManagerRunners;
	}

	protected TaskExecutor[] startTaskManagers(
			Configuration configuration,
			HighAvailabilityServices haServices,
			MetricRegistry metricRegistry,
			int numTaskManagers,
			RpcService[] taskManagerRpcServices) throws Exception {

		final TaskExecutor[] taskExecutors = new TaskExecutor[numTaskManagers];
		final boolean localCommunication = numTaskManagers == 1;

		for (int i = 0; i < numTaskManagers; i++) {
			taskExecutors[i] = TaskManagerRunner.startTaskManager(
				configuration,
				new ResourceID(UUID.randomUUID().toString()),
				taskManagerRpcServices[i],
				haServices,
				heartbeatServices,
				metricRegistry,
				localCommunication,
				new TerminatingFatalErrorHandler(i));

			taskExecutors[i].start();
		}

		return taskExecutors;
	}

	// ------------------------------------------------------------------------
	//  miscellaneous utilities
	// ------------------------------------------------------------------------

	private static Throwable shutDownRpc(RpcService rpcService, Throwable priorException) {
		if (rpcService != null) {
			try {
				rpcService.stopService();
			}
			catch (Throwable t) {
				return firstOrSuppressed(t, priorException);
			}
		}

		return priorException;
	}

	private static Throwable shutDownRpcs(RpcService[] rpcServices, Throwable priorException) {
		if (rpcServices != null) {
			Throwable exception = priorException;

			for (RpcService service : rpcServices) {
				try {
					if (service != null) {
						service.stopService();
					}
				}
				catch (Throwable t) {
					exception = firstOrSuppressed(t, exception);
				}
			}
		}
		return priorException;
	}

	private class TerminatingFatalErrorHandler implements FatalErrorHandler {

		private final int index;

		private TerminatingFatalErrorHandler(int index) {
			this.index = index;
		}

		@Override
		public void onFatalError(Throwable exception) {
			// first check if we are still running
			if (running) {
				LOG.error("TaskManager #{} failed.", index, exception);

				// let's check if there are still TaskManagers because there could be a concurrent
				// shut down operation taking place
				TaskExecutor[] currentTaskManagers = taskManagers;

				if (currentTaskManagers != null) {
					// the shutDown is asynchronous
					currentTaskManagers[index].shutDown();
				}
			}
		}
	}
}
