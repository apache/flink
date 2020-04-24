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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.ClusterEntrypointUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcMetricQueryServiceRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * MiniCluster to execute Flink jobs locally.
 */
public class MiniCluster implements JobExecutorService, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(MiniCluster.class);

	/** The lock to guard startup / shutdown / manipulation methods. */
	private final Object lock = new Object();

	/** The configuration for this mini cluster. */
	private final MiniClusterConfiguration miniClusterConfiguration;

	private final Time rpcTimeout;

	@GuardedBy("lock")
	private final List<TaskExecutor> taskManagers;

	private final TerminatingFatalErrorHandlerFactory taskManagerTerminatingFatalErrorHandlerFactory = new TerminatingFatalErrorHandlerFactory();

	private CompletableFuture<Void> terminationFuture;

	@GuardedBy("lock")
	private MetricRegistryImpl metricRegistry;

	@GuardedBy("lock")
	private ProcessMetricGroup processMetricGroup;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private ExecutorService ioExecutor;

	@GuardedBy("lock")
	private final Collection<RpcService> rpcServices;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private BlobServer blobServer;

	@GuardedBy("lock")
	private HeartbeatServices heartbeatServices;

	@GuardedBy("lock")
	private BlobCacheService blobCacheService;

	@GuardedBy("lock")
	private LeaderRetrievalService resourceManagerLeaderRetriever;

	@GuardedBy("lock")
	private LeaderRetrievalService dispatcherLeaderRetriever;

	@GuardedBy("lock")
	private LeaderRetrievalService clusterRestEndpointLeaderRetrievalService;

	@GuardedBy("lock")
	private Collection<DispatcherResourceManagerComponent> dispatcherResourceManagerComponents;

	@GuardedBy("lock")
	private RpcGatewayRetriever<DispatcherId, DispatcherGateway> dispatcherGatewayRetriever;

	@GuardedBy("lock")
	private RpcGatewayRetriever<ResourceManagerId, ResourceManagerGateway> resourceManagerGatewayRetriever;

	@GuardedBy("lock")
	private LeaderRetriever webMonitorLeaderRetriever;

	@GuardedBy("lock")
	private RpcServiceFactory taskManagerRpcServiceFactory;

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
		this.rpcServices = new ArrayList<>(1 + 2 + miniClusterConfiguration.getNumTaskManagers()); // common + JM + RM + TMs
		this.dispatcherResourceManagerComponents = new ArrayList<>(1);

		this.rpcTimeout = miniClusterConfiguration.getRpcTimeout();
		this.terminationFuture = CompletableFuture.completedFuture(null);
		running = false;

		this.taskManagers = new ArrayList<>(miniClusterConfiguration.getNumTaskManagers());
	}

	public CompletableFuture<URI> getRestAddress() {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running or has already been shut down.");
			return webMonitorLeaderRetriever.getLeaderFuture().thenApply(FunctionUtils.uncheckedFunction(addressLeaderIdTuple -> new URI(addressLeaderIdTuple.f0)));
		}
	}

	public ClusterInformation getClusterInformation() {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running or has already been shut down.");
			return new ClusterInformation("localhost", blobServer.getPort());
		}
	}

	protected Executor getIOExecutor() {
		return ioExecutor;
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
			checkState(!running, "MiniCluster is already running");

			LOG.info("Starting Flink Mini Cluster");
			LOG.debug("Using configuration {}", miniClusterConfiguration);

			final Configuration configuration = miniClusterConfiguration.getConfiguration();
			final boolean useSingleRpcService = miniClusterConfiguration.getRpcServiceSharing() == RpcServiceSharing.SHARED;

			try {
				initializeIOFormatClasses(configuration);

				LOG.info("Starting Metrics Registry");
				metricRegistry = createMetricRegistry(configuration);

				// bring up all the RPC services
				LOG.info("Starting RPC Service(s)");

				final RpcServiceFactory dispatcherResourceManagreComponentRpcServiceFactory;
				final RpcService metricQueryServiceRpcService;

				if (useSingleRpcService) {
					// we always need the 'commonRpcService' for auxiliary calls
					commonRpcService = createLocalRpcService(configuration);
					final CommonRpcServiceFactory commonRpcServiceFactory = new CommonRpcServiceFactory(commonRpcService);
					taskManagerRpcServiceFactory = commonRpcServiceFactory;
					dispatcherResourceManagreComponentRpcServiceFactory = commonRpcServiceFactory;
					metricQueryServiceRpcService = MetricUtils.startLocalMetricsRpcService(configuration);
				} else {

					// start a new service per component, possibly with custom bind addresses
					final String jobManagerExternalAddress = miniClusterConfiguration.getJobManagerExternalAddress();
					final String taskManagerExternalAddress = miniClusterConfiguration.getTaskManagerExternalAddress();
					final String jobManagerExternalPortRange = miniClusterConfiguration.getJobManagerExternalPortRange();
					final String taskManagerExternalPortRange = miniClusterConfiguration.getTaskManagerExternalPortRange();
					final String jobManagerBindAddress = miniClusterConfiguration.getJobManagerBindAddress();
					final String taskManagerBindAddress = miniClusterConfiguration.getTaskManagerBindAddress();

					dispatcherResourceManagreComponentRpcServiceFactory =
						new DedicatedRpcServiceFactory(
							configuration,
							jobManagerExternalAddress,
							jobManagerExternalPortRange,
							jobManagerBindAddress);
					taskManagerRpcServiceFactory =
						new DedicatedRpcServiceFactory(
							configuration,
							taskManagerExternalAddress,
							taskManagerExternalPortRange,
							taskManagerBindAddress);

					// we always need the 'commonRpcService' for auxiliary calls
					// bind to the JobManager address with port 0
					commonRpcService = createRemoteRpcService(configuration, jobManagerBindAddress, 0);
					metricQueryServiceRpcService = MetricUtils.startRemoteMetricsRpcService(
						configuration,
						commonRpcService.getAddress());
				}

				metricRegistry.startQueryService(metricQueryServiceRpcService, null);

				processMetricGroup = MetricUtils.instantiateProcessMetricGroup(
					metricRegistry,
					RpcUtils.getHostname(commonRpcService),
					ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));

				ioExecutor = Executors.newFixedThreadPool(
					ClusterEntrypointUtils.getPoolSize(configuration),
					new ExecutorThreadFactory("mini-cluster-io"));
				haServices = createHighAvailabilityServices(configuration, ioExecutor);

				blobServer = new BlobServer(configuration, haServices.createBlobStore());
				blobServer.start();

				heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

				blobCacheService = new BlobCacheService(
					configuration, haServices.createBlobStore(), new InetSocketAddress(InetAddress.getLocalHost(), blobServer.getPort())
				);

				startTaskManagers();

				MetricQueryServiceRetriever metricQueryServiceRetriever = new RpcMetricQueryServiceRetriever(metricRegistry.getMetricQueryServiceRpcService());

				setupDispatcherResourceManagerComponents(configuration, dispatcherResourceManagreComponentRpcServiceFactory, metricQueryServiceRetriever);

				resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();
				dispatcherLeaderRetriever = haServices.getDispatcherLeaderRetriever();
				clusterRestEndpointLeaderRetrievalService = haServices.getClusterRestEndpointLeaderRetriever();

				dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
					commonRpcService,
					DispatcherGateway.class,
					DispatcherId::fromUuid,
					20,
					Time.milliseconds(20L));
				resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
					commonRpcService,
					ResourceManagerGateway.class,
					ResourceManagerId::fromUuid,
					20,
					Time.milliseconds(20L));
				webMonitorLeaderRetriever = new LeaderRetriever();

				resourceManagerLeaderRetriever.start(resourceManagerGatewayRetriever);
				dispatcherLeaderRetriever.start(dispatcherGatewayRetriever);
				clusterRestEndpointLeaderRetrievalService.start(webMonitorLeaderRetriever);
			}
			catch (Exception e) {
				// cleanup everything
				try {
					close();
				} catch (Exception ee) {
					e.addSuppressed(ee);
				}
				throw e;
			}

			// create a new termination future
			terminationFuture = new CompletableFuture<>();

			// now officially mark this as running
			running = true;

			LOG.info("Flink Mini Cluster started successfully");
		}
	}

	@GuardedBy("lock")
	private void setupDispatcherResourceManagerComponents(Configuration configuration, RpcServiceFactory dispatcherResourceManagreComponentRpcServiceFactory, MetricQueryServiceRetriever metricQueryServiceRetriever) throws Exception {
		dispatcherResourceManagerComponents.addAll(createDispatcherResourceManagerComponents(
			configuration,
			dispatcherResourceManagreComponentRpcServiceFactory,
			haServices,
			blobServer,
			heartbeatServices,
			metricRegistry,
			metricQueryServiceRetriever,
			new ShutDownFatalErrorHandler()
		));

		final Collection<CompletableFuture<ApplicationStatus>> shutDownFutures = new ArrayList<>(dispatcherResourceManagerComponents.size());

		for (DispatcherResourceManagerComponent dispatcherResourceManagerComponent : dispatcherResourceManagerComponents) {
			final CompletableFuture<ApplicationStatus> shutDownFuture = dispatcherResourceManagerComponent.getShutDownFuture();
			FutureUtils.assertNoException(shutDownFuture.thenRun(dispatcherResourceManagerComponent::closeAsync));
			shutDownFutures.add(shutDownFuture);
		}

		FutureUtils.assertNoException(FutureUtils.completeAll(shutDownFutures).thenRun(this::closeAsync));
	}

	@VisibleForTesting
	protected Collection<? extends DispatcherResourceManagerComponent> createDispatcherResourceManagerComponents(
			Configuration configuration,
			RpcServiceFactory rpcServiceFactory,
			HighAvailabilityServices haServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		DispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory = createDispatcherResourceManagerComponentFactory();
		return Collections.singleton(
			dispatcherResourceManagerComponentFactory.create(
				configuration,
				ioExecutor,
				rpcServiceFactory.createRpcService(),
				haServices,
				blobServer,
				heartbeatServices,
				metricRegistry,
				new MemoryArchivedExecutionGraphStore(),
				metricQueryServiceRetriever,
				fatalErrorHandler));
	}

	@Nonnull
	DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory() {
		return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(StandaloneResourceManagerFactory.getInstance());
	}

	@VisibleForTesting
	protected HighAvailabilityServices createHighAvailabilityServices(Configuration configuration, Executor executor) throws Exception {
		LOG.info("Starting high-availability services");
		return HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
			configuration,
			executor);
	}

	/**
	 * Shuts down the mini cluster, failing all currently executing jobs.
	 * The mini cluster can be started again by calling the {@link #start()} method again.
	 *
	 * <p>This method shuts down all started services and components,
	 * even if an exception occurs in the process of shutting down some component.
	 *
	 * @return Future which is completed once the MiniCluster has been completely shut down
	 */
	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (running) {
				LOG.info("Shutting down Flink Mini Cluster");
				try {
					final long shutdownTimeoutMillis = miniClusterConfiguration.getConfiguration().getLong(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT);
					final int numComponents = 2 + miniClusterConfiguration.getNumTaskManagers();
					final Collection<CompletableFuture<Void>> componentTerminationFutures = new ArrayList<>(numComponents);

					componentTerminationFutures.addAll(terminateTaskExecutors());

					componentTerminationFutures.add(shutDownResourceManagerComponents());

					final FutureUtils.ConjunctFuture<Void> componentsTerminationFuture = FutureUtils.completeAll(componentTerminationFutures);

					final CompletableFuture<Void> metricSystemTerminationFuture = FutureUtils.composeAfterwards(
						componentsTerminationFuture,
						this::closeMetricSystem);

					final CompletableFuture<Void> rpcServicesTerminationFuture = FutureUtils.composeAfterwards(
						metricSystemTerminationFuture,
						this::terminateRpcServices);

					final CompletableFuture<Void> remainingServicesTerminationFuture = FutureUtils.runAfterwards(
						rpcServicesTerminationFuture,
						this::terminateMiniClusterServices);

					final CompletableFuture<Void> executorsTerminationFuture = FutureUtils.composeAfterwards(
						remainingServicesTerminationFuture,
						() -> terminateExecutors(shutdownTimeoutMillis));

					executorsTerminationFuture.whenComplete(
							(Void ignored, Throwable throwable) -> {
								if (throwable != null) {
									terminationFuture.completeExceptionally(ExceptionUtils.stripCompletionException(throwable));
								} else {
									terminationFuture.complete(null);
								}
							});
				} finally {
					running = false;
				}
			}

			return terminationFuture;
		}
	}

	private CompletableFuture<Void> closeMetricSystem() {
		synchronized (lock) {
			final ArrayList<CompletableFuture<Void>> terminationFutures = new ArrayList<>(2);

			if (processMetricGroup != null) {
				processMetricGroup.close();
				processMetricGroup = null;
			}

			// metrics shutdown
			if (metricRegistry != null) {
				terminationFutures.add(metricRegistry.shutdown());
				metricRegistry = null;
			}

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	@GuardedBy("lock")
	private void startTaskManagers() throws Exception {
		final int numTaskManagers = miniClusterConfiguration.getNumTaskManagers();

		LOG.info("Starting {} TaskManger(s)", numTaskManagers);

		for (int i = 0; i < numTaskManagers; i++) {
			startTaskExecutor();
		}
	}

	@VisibleForTesting
	void startTaskExecutor() throws Exception {
		synchronized (lock) {
			final Configuration configuration = miniClusterConfiguration.getConfiguration();

			final TaskExecutor taskExecutor = TaskManagerRunner.startTaskManager(
				configuration,
				new ResourceID(UUID.randomUUID().toString()),
				taskManagerRpcServiceFactory.createRpcService(),
				haServices,
				heartbeatServices,
				metricRegistry,
				blobCacheService,
				useLocalCommunication(),
				ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
				taskManagerTerminatingFatalErrorHandlerFactory.create(taskManagers.size()));

			taskExecutor.start();
			taskManagers.add(taskExecutor);
		}
	}

	@VisibleForTesting
	protected boolean useLocalCommunication() {
		return miniClusterConfiguration.getNumTaskManagers() == 1;
	}

	@GuardedBy("lock")
	private Collection<? extends CompletableFuture<Void>> terminateTaskExecutors() {
		final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(taskManagers.size());
		for (int i = 0; i < taskManagers.size(); i++) {
			terminationFutures.add(terminateTaskExecutor(i));
		}

		return terminationFutures;
	}

	@VisibleForTesting
	@Nonnull
	protected CompletableFuture<Void> terminateTaskExecutor(int index) {
		synchronized (lock) {
			final TaskExecutor taskExecutor = taskManagers.get(index);
			return taskExecutor.closeAsync();
		}
	}

	// ------------------------------------------------------------------------
	//  Accessing jobs
	// ------------------------------------------------------------------------

	public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
		return runDispatcherCommand(dispatcherGateway ->
			dispatcherGateway
				.requestMultipleJobDetails(rpcTimeout)
				.thenApply(jobs ->
					jobs.getJobs().stream()
						.map(details -> new JobStatusMessage(details.getJobId(), details.getJobName(), details.getStatus(), details.getStartTime()))
						.collect(Collectors.toList())));
	}

	public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.requestJobStatus(jobId, rpcTimeout));
	}

	public CompletableFuture<Acknowledge> cancelJob(JobID jobId) {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.cancelJob(jobId, rpcTimeout));
	}

	public CompletableFuture<String> triggerSavepoint(JobID jobId, String targetDirectory, boolean cancelJob) {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.triggerSavepoint(jobId, targetDirectory, cancelJob, rpcTimeout));
	}

	public CompletableFuture<String> stopWithSavepoint(JobID jobId, String targetDirectory, boolean advanceToEndOfEventTime) {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.stopWithSavepoint(jobId, targetDirectory, advanceToEndOfEventTime, rpcTimeout));
	}

	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.disposeSavepoint(savepointPath, rpcTimeout));
	}

	public CompletableFuture<? extends AccessExecutionGraph> getExecutionGraph(JobID jobId) {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.requestJob(jobId, rpcTimeout));
	}

	public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
			JobID jobId,
			OperatorID operatorId,
			SerializedValue<CoordinationRequest> serializedRequest) {
		return runDispatcherCommand(
			dispatcherGateway ->
				dispatcherGateway.deliverCoordinationRequestToCoordinator(
					jobId, operatorId, serializedRequest, rpcTimeout));
	}

	private <T> CompletableFuture<T> runDispatcherCommand(Function<DispatcherGateway, CompletableFuture<T>> dispatcherCommand) {
		return getDispatcherGatewayFuture().thenApply(dispatcherCommand).thenCompose(Function.identity());
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

		final CompletableFuture<JobSubmissionResult> submissionFuture = submitJob(job);

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

		final CompletableFuture<JobSubmissionResult> submissionFuture = submitJob(job);

		final CompletableFuture<JobResult> jobResultFuture = submissionFuture.thenCompose(
			(JobSubmissionResult ignored) -> requestJobResult(job.getJobID()));

		final JobResult jobResult;

		try {
			jobResult = jobResultFuture.get();
		} catch (ExecutionException e) {
			throw new JobExecutionException(job.getJobID(), "Could not retrieve JobResult.", ExceptionUtils.stripExecutionException(e));
		}

		try {
			return jobResult.toJobExecutionResult(Thread.currentThread().getContextClassLoader());
		} catch (IOException | ClassNotFoundException e) {
			throw new JobExecutionException(job.getJobID(), e);
		}
	}

	public CompletableFuture<JobSubmissionResult> submitJob(JobGraph jobGraph) {
		final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = getDispatcherGatewayFuture();
		final CompletableFuture<InetSocketAddress> blobServerAddressFuture = createBlobServerAddress(dispatcherGatewayFuture);
		final CompletableFuture<Void> jarUploadFuture = uploadAndSetJobFiles(blobServerAddressFuture, jobGraph);
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture
			.thenCombine(
				dispatcherGatewayFuture,
				(Void ack, DispatcherGateway dispatcherGateway) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout))
			.thenCompose(Function.identity());
		return acknowledgeCompletableFuture.thenApply(
			(Acknowledge ignored) -> new JobSubmissionResult(jobGraph.getJobID()));
	}

	public CompletableFuture<JobResult> requestJobResult(JobID jobId) {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.requestJobResult(jobId, RpcUtils.INF_TIMEOUT));
	}

	public CompletableFuture<ClusterOverview> requestClusterOverview() {
		return runDispatcherCommand(dispatcherGateway -> dispatcherGateway.requestClusterOverview(RpcUtils.INF_TIMEOUT));
	}

	@VisibleForTesting
	protected CompletableFuture<DispatcherGateway> getDispatcherGatewayFuture() {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running or has already been shut down.");
			return dispatcherGatewayRetriever.getFuture();
		}
	}

	private CompletableFuture<Void> uploadAndSetJobFiles(final CompletableFuture<InetSocketAddress> blobServerAddressFuture, final JobGraph job) {
		return blobServerAddressFuture.thenAccept(blobServerAddress -> {
			try {
				ClientUtils.extractAndUploadJobGraphFiles(job, () -> new BlobClient(blobServerAddress, miniClusterConfiguration.getConfiguration()));
			} catch (FlinkException e) {
				throw new CompletionException(e);
			}
		});
	}

	private CompletableFuture<InetSocketAddress> createBlobServerAddress(final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture) {
		return dispatcherGatewayFuture.thenApply(dispatcherGateway ->
				dispatcherGateway
					.getBlobServerPort(rpcTimeout)
					.thenApply(blobServerPort -> new InetSocketAddress(dispatcherGateway.getHostname(), blobServerPort)))
			.thenCompose(Function.identity());
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
		return new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(config),
			ReporterSetup.fromConfiguration(config, null));
	}

	/**
	 * Factory method to instantiate the remote RPC service.
	 *
	 * @param configuration Flink configuration.
	 * @param bindAddress The address to bind the RPC service to.
	 * @param bindPort The port range to bind the RPC service to.
	 * @return The instantiated RPC service
	 */
	protected RpcService createRemoteRpcService(
			Configuration configuration,
			String bindAddress,
			int bindPort) throws Exception {
		return AkkaRpcServiceUtils.remoteServiceBuilder(configuration, bindAddress, String.valueOf(bindPort))
			.withBindAddress(bindAddress)
			.withBindPort(bindPort)
			.withCustomConfig(AkkaUtils.testDispatcherConfig())
			.createAndStart();
	}

	/**
	 * Factory method to instantiate the remote RPC service.
	 *
	 * @param configuration Flink configuration.
	 * @param externalAddress The external address to access the RPC service.
	 * @param externalPortRange The external port range to access the RPC service.
	 * @param bindAddress The address to bind the RPC service to.
	 * @return The instantiated RPC service
	 */
	protected RpcService createRemoteRpcService(
		Configuration configuration,
		String externalAddress,
		String externalPortRange,
		String bindAddress) throws Exception {
		return AkkaRpcServiceUtils.remoteServiceBuilder(configuration, externalAddress, externalPortRange)
			.withBindAddress(bindAddress)
			.withCustomConfig(AkkaUtils.testDispatcherConfig())
			.createAndStart();
	}

	/**
	 * Factory method to instantiate the local RPC service.
	 *
	 * @param configuration Flink configuration.
	 * @return The instantiated RPC service
	 */
	protected RpcService createLocalRpcService(Configuration configuration) throws Exception {
		return AkkaRpcServiceUtils.localServiceBuilder(configuration)
			.withCustomConfig(AkkaUtils.testDispatcherConfig())
			.createAndStart();
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	@GuardedBy("lock")
	private CompletableFuture<Void> shutDownResourceManagerComponents() {

		final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(dispatcherResourceManagerComponents.size());

		for (DispatcherResourceManagerComponent dispatcherResourceManagerComponent : dispatcherResourceManagerComponents) {
			terminationFutures.add(dispatcherResourceManagerComponent.closeAsync());
		}

		final FutureUtils.ConjunctFuture<Void> dispatcherTerminationFuture = FutureUtils.completeAll(terminationFutures);

		return FutureUtils.runAfterwards(
			dispatcherTerminationFuture,
			() -> {
				Exception exception = null;

				synchronized (lock) {
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

					if (clusterRestEndpointLeaderRetrievalService != null) {
						try {
							clusterRestEndpointLeaderRetrievalService.stop();
						} catch (Exception e) {
							exception = ExceptionUtils.firstOrSuppressed(e, exception);
						}

						clusterRestEndpointLeaderRetrievalService = null;
					}
				}

				if (exception != null) {
					throw exception;
				}
			});
	}

	private void terminateMiniClusterServices() throws Exception {
		// collect the first exception, but continue and add all successive
		// exceptions as suppressed
		Exception exception = null;

		synchronized (lock) {
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

			if (exception != null) {
				throw exception;
			}
		}
	}

	@Nonnull
	private CompletableFuture<Void> terminateRpcServices() {
		synchronized (lock) {
			final int numRpcServices = 1 + rpcServices.size();

			final Collection<CompletableFuture<?>> rpcTerminationFutures = new ArrayList<>(numRpcServices);

			rpcTerminationFutures.add(commonRpcService.stopService());

			for (RpcService rpcService : rpcServices) {
				rpcTerminationFutures.add(rpcService.stopService());
			}

			commonRpcService = null;
			rpcServices.clear();

			return FutureUtils.completeAll(rpcTerminationFutures);
		}
	}

	private CompletableFuture<Void> terminateExecutors(long executorShutdownTimeoutMillis) {
		synchronized (lock) {
			if (ioExecutor != null) {
				return ExecutorUtils.nonBlockingShutdown(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS, ioExecutor);
			} else {
				return CompletableFuture.completedFuture(null);
			}
		}
	}

	/**
	 * Internal factory for {@link RpcService}.
	 */
	protected interface RpcServiceFactory {
		RpcService createRpcService() throws Exception;
	}

	/**
	 * Factory which returns always the common {@link RpcService}.
	 */
	protected static class CommonRpcServiceFactory implements RpcServiceFactory {

		private final RpcService commonRpcService;

		CommonRpcServiceFactory(RpcService commonRpcService) {
			this.commonRpcService = commonRpcService;
		}

		@Override
		public RpcService createRpcService() {
			return commonRpcService;
		}
	}

	/**
	 * Factory which creates and registers new {@link RpcService}.
	 */
	protected class DedicatedRpcServiceFactory implements RpcServiceFactory {

		private final Configuration configuration;
		private final String externalAddress;
		private final String externalPortRange;
		private final String bindAddress;

		DedicatedRpcServiceFactory(
				Configuration configuration,
				String externalAddress,
				String externalPortRange,
				String bindAddress) {
			this.configuration = configuration;
			this.externalAddress = externalAddress;
			this.externalPortRange = externalPortRange;
			this.bindAddress = bindAddress;
		}

		@Override
		public RpcService createRpcService() throws Exception {
			final RpcService rpcService = MiniCluster.this.createRemoteRpcService(
				configuration, externalAddress, externalPortRange, bindAddress);

			synchronized (lock) {
				rpcServices.add(rpcService);
			}

			return rpcService;
		}
	}

	// ------------------------------------------------------------------------
	//  miscellaneous utilities
	// ------------------------------------------------------------------------

	private void initializeIOFormatClasses(Configuration configuration) {
		// TODO: That we still have to call something like this is a crime against humanity
		FileOutputFormat.initDefaultsFromConfiguration(configuration);
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

				synchronized (lock) {
					taskManagers.get(index).closeAsync();
				}
			}
		}
	}

	private class ShutDownFatalErrorHandler implements FatalErrorHandler {

		@Override
		public void onFatalError(Throwable exception) {
			LOG.warn("Error in MiniCluster. Shutting the MiniCluster down.", exception);
			closeAsync();
		}
	}

	private class TerminatingFatalErrorHandlerFactory {

		/**
		 * Create a new {@link TerminatingFatalErrorHandler} for the {@link TaskExecutor} with
		 * the given index.
		 *
		 * @param index into the {@link #taskManagers} collection to identify the correct {@link TaskExecutor}.
		 * @return {@link TerminatingFatalErrorHandler} for the given index
		 */
		@GuardedBy("lock")
		private TerminatingFatalErrorHandler create(int index) {
			return new TerminatingFatalErrorHandler(index);
		}
	}
}
