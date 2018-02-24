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
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRunner;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Flip-6 based MiniCluster.
 */
public class MiniCluster implements JobExecutorService, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(MiniCluster.class);

	/** The lock to guard startup / shutdown / manipulation methods. */
	private final Object lock = new Object();

	/** The configuration for this mini cluster. */
	private final MiniClusterConfiguration miniClusterConfiguration;

	private final Time rpcTimeout;

	@GuardedBy("lock")
	private MetricRegistryImpl metricRegistry;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private RpcService jobManagerRpcService;

	@GuardedBy("lock")
	private RpcService[] taskManagerRpcServices;

	@GuardedBy("lock")
	private RpcService resourceManagerRpcService;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private BlobServer blobServer;

	@GuardedBy("lock")
	private HeartbeatServices heartbeatServices;

	@GuardedBy("lock")
	private BlobCacheService blobCacheService;

	@GuardedBy("lock")
	private ResourceManagerRunner resourceManagerRunner;

	private volatile TaskExecutor[] taskManagers;

	@GuardedBy("lock")
	private DispatcherRestEndpoint dispatcherRestEndpoint;

	@GuardedBy("lock")
	private URI restAddressURI;

	@GuardedBy("lock")
	private LeaderRetrievalService resourceManagerLeaderRetriever;

	@GuardedBy("lock")
	private LeaderRetrievalService dispatcherLeaderRetriever;

	@GuardedBy("lock")
	private StandaloneDispatcher dispatcher;

	@GuardedBy("lock")
	private RpcGatewayRetriever<DispatcherId, DispatcherGateway> dispatcherGatewayRetriever;

	/** Flag marking the mini cluster as started/running. */
	private volatile boolean running;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Flink mini cluster based on the given configuration.
	 *
	 * @param miniClusterConfiguration The configuration for the mini cluster
	 */
	public MiniCluster(MiniClusterConfiguration miniClusterConfiguration) {
		this.miniClusterConfiguration = checkNotNull(miniClusterConfiguration, "config may not be null");

		this.rpcTimeout = Time.seconds(10L);
		running = false;
	}

	public URI getRestAddress() {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running.");
			return restAddressURI;
		}
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
			final int numTaskManagers = miniClusterConfiguration.getNumTaskManagers();
			final boolean useSingleRpcService = miniClusterConfiguration.getRpcServiceSharing() == MiniClusterConfiguration.RpcServiceSharing.SHARED;

			try {
				initializeIOFormatClasses(configuration);

				LOG.info("Starting Metrics Registry");
				metricRegistry = createMetricRegistry(configuration);

				final RpcService jobManagerRpcService;
				final RpcService resourceManagerRpcService;
				final RpcService[] taskManagerRpcServices = new RpcService[numTaskManagers];

				// bring up all the RPC services
				LOG.info("Starting RPC Service(s)");

				// we always need the 'commonRpcService' for auxiliary calls
				commonRpcService = createRpcService(configuration, rpcTimeout, false, null);

				// TODO: Temporary hack until the metric query service is ported to the RpcEndpoint
				final ActorSystem actorSystem = ((AkkaRpcService) commonRpcService).getActorSystem();
				metricRegistry.startQueryService(actorSystem, null);

				if (useSingleRpcService) {
					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = commonRpcService;
					}

					jobManagerRpcService = commonRpcService;
					resourceManagerRpcService = commonRpcService;

					this.resourceManagerRpcService = null;
					this.jobManagerRpcService = null;
					this.taskManagerRpcServices = null;
				}
				else {
					// start a new service per component, possibly with custom bind addresses
					final String jobManagerBindAddress = miniClusterConfiguration.getJobManagerBindAddress();
					final String taskManagerBindAddress = miniClusterConfiguration.getTaskManagerBindAddress();
					final String resourceManagerBindAddress = miniClusterConfiguration.getResourceManagerBindAddress();

					jobManagerRpcService = createRpcService(configuration, rpcTimeout, true, jobManagerBindAddress);
					resourceManagerRpcService = createRpcService(configuration, rpcTimeout, true, resourceManagerBindAddress);

					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = createRpcService(
								configuration, rpcTimeout, true, taskManagerBindAddress);
					}

					this.jobManagerRpcService = jobManagerRpcService;
					this.taskManagerRpcServices = taskManagerRpcServices;
					this.resourceManagerRpcService = resourceManagerRpcService;
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
				LOG.info("Starting ResourceManger");
				resourceManagerRunner = startResourceManager(
					configuration,
					haServices,
					heartbeatServices,
					metricRegistry,
					resourceManagerRpcService,
					new ClusterInformation("localhost", blobServer.getPort()));

				blobCacheService = new BlobCacheService(
					configuration, haServices.createBlobStore(), new InetSocketAddress(InetAddress.getLocalHost(), blobServer.getPort())
				);

				// bring up the TaskManager(s) for the mini cluster
				LOG.info("Starting {} TaskManger(s)", numTaskManagers);
				taskManagers = startTaskManagers(
					configuration,
					haServices,
					heartbeatServices,
					metricRegistry,
					blobCacheService,
					numTaskManagers,
					taskManagerRpcServices);

				// starting the dispatcher rest endpoint
				LOG.info("Starting dispatcher rest endpoint.");

				dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
					jobManagerRpcService,
					DispatcherGateway.class,
					DispatcherId::new,
					20,
					Time.milliseconds(20L));
				final RpcGatewayRetriever<ResourceManagerId, ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
					jobManagerRpcService,
					ResourceManagerGateway.class,
					ResourceManagerId::new,
					20,
					Time.milliseconds(20L));

				this.dispatcherRestEndpoint = new DispatcherRestEndpoint(
					RestServerEndpointConfiguration.fromConfiguration(configuration),
					dispatcherGatewayRetriever,
					configuration,
					RestHandlerConfiguration.fromConfiguration(configuration),
					resourceManagerGatewayRetriever,
					blobServer.getTransientBlobService(),
					commonRpcService.getExecutor(),
					new AkkaQueryServiceRetriever(
						actorSystem,
						Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT))),
					haServices.getWebMonitorLeaderElectionService(),
					new ShutDownFatalErrorHandler());

				dispatcherRestEndpoint.start();

				restAddressURI = new URI(dispatcherRestEndpoint.getRestAddress());

				// bring up the dispatcher that launches JobManagers when jobs submitted
				LOG.info("Starting job dispatcher(s) for JobManger");

				dispatcher = new StandaloneDispatcher(
					jobManagerRpcService,
					Dispatcher.DISPATCHER_NAME + UUID.randomUUID(),
					configuration,
					haServices,
					resourceManagerRunner.getResourceManageGateway(),
					blobServer,
					heartbeatServices,
					metricRegistry,
					new MemoryArchivedExecutionGraphStore(),
					Dispatcher.DefaultJobManagerRunnerFactory.INSTANCE,
					new ShutDownFatalErrorHandler(),
					dispatcherRestEndpoint.getRestAddress());

				dispatcher.start();

				resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();
				dispatcherLeaderRetriever = haServices.getDispatcherLeaderRetriever();

				resourceManagerLeaderRetriever.start(resourceManagerGatewayRetriever);
				dispatcherLeaderRetriever.start(dispatcherGatewayRetriever);
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
		if (dispatcher != null) {
			try {
				RpcUtils.terminateRpcEndpoint(dispatcher, rpcTimeout);
			} catch (Exception e) {
				exception = e;
			}
			dispatcher = null;
		}

		if (dispatcherRestEndpoint != null) {
			try {
				dispatcherRestEndpoint.shutDownAsync().get();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			dispatcherRestEndpoint = null;
		}

		if (resourceManagerLeaderRetriever != null) {
			try {
				resourceManagerLeaderRetriever.stop();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			resourceManagerLeaderRetriever = null;
		}

		if (dispatcherLeaderRetriever != null) {
			try {
				dispatcherLeaderRetriever.stop();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			dispatcherLeaderRetriever = null;
		}

		if (resourceManagerRunner != null) {
			try {
				resourceManagerRunner.shutDown();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (taskManagers != null) {
			for (TaskExecutor tm : taskManagers) {
				if (tm != null) {
					try {
						tm.shutDown();
						// wait for the TaskManager to properly terminate
						tm.getTerminationFuture().get();
					} catch (Throwable t) {
						exception = ExceptionUtils.firstOrSuppressed(t, exception);
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
		exception = shutDownRpc(jobManagerRpcService, exception);
		exception = shutDownRpcs(taskManagerRpcServices, exception);
		exception = shutDownRpc(resourceManagerRpcService, exception);
		commonRpcService = null;
		jobManagerRpcService = null;
		taskManagerRpcServices = null;
		resourceManagerRpcService = null;

		if (blobCacheService != null) {
			try {
				blobCacheService.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}
			blobCacheService = null;
		}

		// shut down the blob server
		if (blobServer != null) {
			try {
				blobServer.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}
			blobServer = null;
		}

		// shut down high-availability services
		if (haServices != null) {
			try {
				haServices.closeAndCleanupAllData();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}
			haServices = null;
		}

		// if anything went wrong, throw the first error with all the additional suppressed exceptions
		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Error while shutting down mini cluster");
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
	public void runDetached(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job, "job is null");

		final DispatcherGateway currentDispatcherGateway;
		try {
			currentDispatcherGateway = getDispatcherGateway();
		} catch (LeaderRetrievalException e) {
			throw new JobExecutionException(job.getJobID(), e);
		}

		// we have to allow queued scheduling in Flip-6 mode because we need to request slots
		// from the ResourceManager
		job.setAllowQueuedScheduling(true);

		final CompletableFuture<Acknowledge> submissionFuture = currentDispatcherGateway.submitJob(job, rpcTimeout);

		try {
			submissionFuture.get();
		} catch (ExecutionException e) {
			throw new JobExecutionException(job.getJobID(), ExceptionUtils.stripExecutionException(e));
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
	@Override
	public JobExecutionResult executeJobBlocking(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job, "job is null");

		final DispatcherGateway currentDispatcherGateway;
		try {
			currentDispatcherGateway = getDispatcherGateway();
		} catch (LeaderRetrievalException e) {
			throw new JobExecutionException(job.getJobID(), e);
		}

		// we have to allow queued scheduling in Flip-6 mode because we need to request slots
		// from the ResourceManager
		job.setAllowQueuedScheduling(true);

		final CompletableFuture<Acknowledge> submissionFuture = currentDispatcherGateway.submitJob(job, rpcTimeout);

		final CompletableFuture<JobResult> jobResultFuture = submissionFuture.thenCompose(
			(Acknowledge ack) -> currentDispatcherGateway.requestJobResult(job.getJobID(), RpcUtils.INF_TIMEOUT));

		final JobResult jobResult;

		try {
			jobResult = jobResultFuture.get();
		} catch (ExecutionException e) {
			throw new JobExecutionException(job.getJobID(), "Could not retrieve JobResult.", ExceptionUtils.stripExecutionException(e));
		}

		try {
			return jobResult.toJobExecutionResult(Thread.currentThread().getContextClassLoader());
		} catch (JobResult.WrappedJobException e) {
			throw new JobExecutionException(job.getJobID(), e.getCause());
		} catch (IOException | ClassNotFoundException e) {
			throw new JobExecutionException(job.getJobID(), e);
		}
	}

	private DispatcherGateway getDispatcherGateway() throws LeaderRetrievalException, InterruptedException {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running.");
			try {
				return dispatcherGatewayRetriever.getFuture().get();
			} catch (ExecutionException e) {
				throw new LeaderRetrievalException("Could not retrieve the leading dispatcher.", ExceptionUtils.stripExecutionException(e));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  factories - can be overridden by subclasses to alter behavior
	// ------------------------------------------------------------------------

	/**
	 * Factory method to create the metric registry for the mini cluster.
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

		final Config akkaConfig;

		if (remoteEnabled) {
			akkaConfig = AkkaUtils.getAkkaConfig(configuration, bindAddress, 0);
		} else {
			akkaConfig = AkkaUtils.getAkkaConfig(configuration);
		}

		final Config effectiveAkkaConfig = AkkaUtils.testDispatcherConfig().withFallback(akkaConfig);

		final ActorSystem actorSystem = AkkaUtils.createActorSystem(effectiveAkkaConfig);

		return new AkkaRpcService(actorSystem, askTimeout);
	}

	protected ResourceManagerRunner startResourceManager(
			Configuration configuration,
			HighAvailabilityServices haServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			RpcService resourceManagerRpcService,
			ClusterInformation clusterInformation) throws Exception {

		final ResourceManagerRunner resourceManagerRunner = new ResourceManagerRunner(
			ResourceID.generate(),
			FlinkResourceManager.RESOURCE_MANAGER_NAME + '_' + UUID.randomUUID(),
			configuration,
			resourceManagerRpcService,
			haServices,
			heartbeatServices,
			metricRegistry,
			clusterInformation);

			resourceManagerRunner.start();

		return resourceManagerRunner;
	}

	protected TaskExecutor[] startTaskManagers(
			Configuration configuration,
			HighAvailabilityServices haServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
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
				blobCacheService,
				localCommunication,
				new TerminatingFatalErrorHandler(i));

			taskExecutors[i].start();
		}

		return taskExecutors;
	}

	// ------------------------------------------------------------------------
	//  miscellaneous utilities
	// ------------------------------------------------------------------------

	private void initializeIOFormatClasses(Configuration configuration) {
		// TODO: That we still have to call something like this is a crime against humanity
		FileOutputFormat.initDefaultsFromConfiguration(configuration);
	}

	private static Throwable shutDownRpc(RpcService rpcService, Throwable priorException) {
		if (rpcService != null) {
			try {
				rpcService.stopService().get();
			}
			catch (Throwable t) {
				return ExceptionUtils.firstOrSuppressed(t, priorException);
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
						service.stopService().get();
					}
				}
				catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}
		}
		return priorException;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		try {
			shutdown();
			return CompletableFuture.completedFuture(null);
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
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

	private class ShutDownFatalErrorHandler implements FatalErrorHandler {

		@Override
		public void onFatalError(Throwable exception) {
			LOG.warn("Error in MiniCluster. Shutting the MiniCluster down.", exception);
			try {
				shutdown();
			} catch (Exception e) {
				LOG.warn("Could not shut down the MiniCluster.", e);
			}
		}
	}
}
