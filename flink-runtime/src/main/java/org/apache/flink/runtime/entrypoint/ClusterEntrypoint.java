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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.concurrent.duration.FiniteDuration;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 */
public abstract class ClusterEntrypoint implements FatalErrorHandler {

	public static final ConfigOption<String> EXECUTION_MODE = ConfigOptions
		.key("internal.cluster.execution-mode")
		.defaultValue(ExecutionMode.NORMAL.toString());

	protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

	protected static final int SUCCESS_RETURN_CODE = 0;
	protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
	protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	/** The lock to guard startup / shutdown / manipulation methods. */
	private final Object lock = new Object();

	private final Configuration configuration;

	private final CompletableFuture<Boolean> terminationFuture;

	private final AtomicBoolean isTerminating = new AtomicBoolean(false);

	@GuardedBy("lock")
	private MetricRegistryImpl metricRegistry;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private BlobServer blobServer;

	@GuardedBy("lock")
	private HeartbeatServices heartbeatServices;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private ResourceManager<?> resourceManager;

	@GuardedBy("lock")
	private Dispatcher dispatcher;

	@GuardedBy("lock")
	private LeaderRetrievalService dispatcherLeaderRetrievalService;

	@GuardedBy("lock")
	private LeaderRetrievalService resourceManagerRetrievalService;

	@GuardedBy("lock")
	private WebMonitorEndpoint<?> webMonitorEndpoint;

	@GuardedBy("lock")
	private ArchivedExecutionGraphStore archivedExecutionGraphStore;

	@GuardedBy("lock")
	private TransientBlobCache transientBlobCache;

	protected ClusterEntrypoint(Configuration configuration) {
		this.configuration = Preconditions.checkNotNull(configuration);
		this.terminationFuture = new CompletableFuture<>();
	}

	public CompletableFuture<Boolean> getTerminationFuture() {
		return terminationFuture;
	}

	protected void startCluster() {
		LOG.info("Starting {}.", getClass().getSimpleName());

		try {
			configureFileSystems(configuration);

			SecurityContext securityContext = installSecurityContext(configuration);

			securityContext.runSecured((Callable<Void>) () -> {
				runCluster(configuration);

				return null;
			});
		} catch (Throwable t) {
			LOG.error("Cluster initialization failed.", t);

			shutDownAndTerminate(
				STARTUP_FAILURE_RETURN_CODE,
				ApplicationStatus.FAILED,
				false);
		}
	}

	protected void configureFileSystems(Configuration configuration) throws Exception {
		LOG.info("Install default filesystem.");

		try {
			FileSystem.initialize(configuration);
		} catch (IOException e) {
			throw new IOException("Error while setting the default " +
				"filesystem scheme from configuration.", e);
		}
	}

	protected SecurityContext installSecurityContext(Configuration configuration) throws Exception {
		LOG.info("Install security context.");

		SecurityUtils.install(new SecurityConfiguration(configuration));

		return SecurityUtils.getInstalledContext();
	}

	protected void runCluster(Configuration configuration) throws Exception {
		synchronized (lock) {
			initializeServices(configuration);

			// write host information into configuration
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

			startClusterComponents(
				configuration,
				commonRpcService,
				haServices,
				blobServer,
				heartbeatServices,
				metricRegistry);

			// TODO: Make shutDownAndTerminate non blocking to not use the global executor
			dispatcher.getTerminationFuture().whenCompleteAsync(
				(Boolean success, Throwable throwable) -> {
					if (throwable != null) {
						LOG.info("Could not properly terminate the Dispatcher.", throwable);
					}

					shutDownAndTerminate(
						SUCCESS_RETURN_CODE,
						ApplicationStatus.SUCCEEDED,
						true);
				});
		}
	}

	protected void initializeServices(Configuration configuration) throws Exception {

		LOG.info("Initializing cluster services.");

		synchronized (lock) {
			final String bindAddress = configuration.getString(JobManagerOptions.ADDRESS);
			final String portRange = getRPCPortRange(configuration);

			commonRpcService = createRpcService(configuration, bindAddress, portRange);

			// update the configuration used to create the high availability services
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

			haServices = createHaServices(configuration, commonRpcService.getExecutor());
			blobServer = new BlobServer(configuration, haServices.createBlobStore());
			blobServer.start();
			heartbeatServices = createHeartbeatServices(configuration);
			metricRegistry = createMetricRegistry(configuration);

			// TODO: This is a temporary hack until we have ported the MetricQueryService to the new RpcEndpoint
			// start the MetricQueryService
			final ActorSystem actorSystem = ((AkkaRpcService) commonRpcService).getActorSystem();
			metricRegistry.startQueryService(actorSystem, null);
		}
	}

	protected void startClusterComponents(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry) throws Exception {
		synchronized (lock) {
			archivedExecutionGraphStore = createSerializableExecutionGraphStore(configuration, rpcService.getScheduledExecutor());

			dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

			resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

			final ClusterInformation clusterInformation = new ClusterInformation(
				rpcService.getAddress(),
				blobServer.getPort());

			transientBlobCache = new TransientBlobCache(
				configuration,
				new InetSocketAddress(
					clusterInformation.getBlobServerHostname(),
					clusterInformation.getBlobServerPort()));

			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				DispatcherGateway.class,
				DispatcherId::new,
				10,
				Time.milliseconds(50L));

			LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				ResourceManagerGateway.class,
				ResourceManagerId::new,
				10,
				Time.milliseconds(50L));

			// TODO: Remove once we have ported the MetricFetcher to the RpcEndpoint
			final ActorSystem actorSystem = ((AkkaRpcService) rpcService).getActorSystem();
			final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

			webMonitorEndpoint = createRestEndpoint(
				configuration,
				dispatcherGatewayRetriever,
				resourceManagerGatewayRetriever,
				transientBlobCache,
				rpcService.getExecutor(),
				new AkkaQueryServiceRetriever(actorSystem, timeout),
				highAvailabilityServices.getWebMonitorLeaderElectionService());

			LOG.debug("Starting Dispatcher REST endpoint.");
			webMonitorEndpoint.start();

			resourceManager = createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				metricRegistry,
				this,
				clusterInformation,
				webMonitorEndpoint.getRestAddress());

			dispatcher = createDispatcher(
				configuration,
				rpcService,
				highAvailabilityServices,
				resourceManager.getSelfGateway(ResourceManagerGateway.class),
				blobServer,
				heartbeatServices,
				metricRegistry,
				archivedExecutionGraphStore,
				this,
				webMonitorEndpoint.getRestAddress());

			LOG.debug("Starting ResourceManager.");
			resourceManager.start();
			resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);

			LOG.debug("Starting Dispatcher.");
			dispatcher.start();
			dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);
		}
	}

	/**
	 * Returns the port range for the common {@link RpcService}.
	 *
	 * @param configuration to extract the port range from
	 * @return Port range for the common {@link RpcService}
	 */
	protected String getRPCPortRange(Configuration configuration) {
		return String.valueOf(configuration.getInteger(JobManagerOptions.PORT));
	}

	protected RpcService createRpcService(
			Configuration configuration,
			String bindAddress,
			String portRange) throws Exception {
		ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, bindAddress, portRange, LOG);
		FiniteDuration duration = AkkaUtils.getTimeout(configuration);
		return new AkkaRpcService(actorSystem, Time.of(duration.length(), duration.unit()));
	}

	protected HighAvailabilityServices createHaServices(
		Configuration configuration,
		Executor executor) throws Exception {
		return HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);
	}

	protected HeartbeatServices createHeartbeatServices(Configuration configuration) {
		return HeartbeatServices.fromConfiguration(configuration);
	}

	protected MetricRegistryImpl createMetricRegistry(Configuration configuration) {
		return new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration));
	}

	private void shutDown(boolean cleanupHaData) throws FlinkException {
		LOG.info("Stopping {}.", getClass().getSimpleName());

		Throwable exception = null;

		synchronized (lock) {

			try {
				stopClusterComponents();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}

			try {
				stopClusterServices(cleanupHaData);
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}

			terminationFuture.complete(true);
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the cluster entrypoint.", exception);
		}
	}

	protected void stopClusterServices(boolean cleanupHaData) throws FlinkException {
		Throwable exception = null;

		synchronized (lock) {
			if (metricRegistry != null) {
				try {
					metricRegistry.shutdown();
				} catch (Throwable t) {
					exception = t;
				}
			}

			if (blobServer != null) {
				try {
					blobServer.close();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (haServices != null) {
				try {
					if (cleanupHaData) {
						haServices.closeAndCleanupAllData();
					} else {
						haServices.close();
					}
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (commonRpcService != null) {
				try {
					commonRpcService.stopService();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the cluster services.", exception);
		}
	}

	protected void stopClusterComponents() throws Exception {
		synchronized (lock) {
			Throwable exception = null;

			if (webMonitorEndpoint != null) {
				webMonitorEndpoint.shutdown(Time.seconds(10L));
			}

			if (dispatcherLeaderRetrievalService != null) {
				try {
					dispatcherLeaderRetrievalService.stop();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (dispatcher != null) {
				try {
					dispatcher.shutDown();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (resourceManagerRetrievalService != null) {
				try {
					resourceManagerRetrievalService.stop();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (resourceManager != null) {
				try {
					resourceManager.shutDown();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (archivedExecutionGraphStore != null) {
				try {
					archivedExecutionGraphStore.close();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (transientBlobCache != null) {
				try {
					transientBlobCache.close();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (exception != null) {
				throw new FlinkException("Could not properly shut down the session cluster entry point.", exception);
			}
		}
	}

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Fatal error occurred in the cluster entrypoint.", exception);

		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	// --------------------------------------------------
	// Internal methods
	// --------------------------------------------------

	private void shutDownAndTerminate(
		int returnCode,
		ApplicationStatus applicationStatus,
		boolean cleanupHaData) {

		LOG.info("Shut down and terminate {} with return code {} and application status {}.",
			getClass().getSimpleName(),
			returnCode,
			applicationStatus);

		if (isTerminating.compareAndSet(false, true)) {
			try {
				shutDown(cleanupHaData);
			} catch (Throwable t) {
				LOG.info("Could not properly shut down cluster entrypoint.", t);
			}

			System.exit(returnCode);
		} else {
			LOG.debug("Concurrent termination call detected. Ignoring termination call with return code {} and application status {}.",
				returnCode,
				applicationStatus);
		}
	}

	// --------------------------------------------------
	// Abstract methods
	// --------------------------------------------------

	protected abstract Dispatcher createDispatcher(
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		ResourceManagerGateway resourceManagerGateway,
		BlobServer blobServer,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		ArchivedExecutionGraphStore archivedExecutionGraphStore,
		FatalErrorHandler fatalErrorHandler,
		@Nullable String restAddress) throws Exception;

	protected abstract ResourceManager<?> createResourceManager(
		Configuration configuration,
		ResourceID resourceId,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		FatalErrorHandler fatalErrorHandler,
		ClusterInformation clusterInformation,
		@Nullable String webInterfaceUrl) throws Exception;

	protected abstract WebMonitorEndpoint<?> createRestEndpoint(
		Configuration configuration,
		LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
		LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
		TransientBlobService transientBlobService,
		Executor executor,
		MetricQueryServiceRetriever metricQueryServiceRetriever,
		LeaderElectionService leaderElectionService) throws Exception;

	protected abstract ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
		Configuration configuration,
		ScheduledExecutor scheduledExecutor) throws IOException;

	protected static ClusterConfiguration parseArguments(String[] args) {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		final String configDir = parameterTool.get("configDir", "");

		return new ClusterConfiguration(configDir);
	}

	protected static Configuration loadConfiguration(ClusterConfiguration clusterConfiguration) {
		return GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir());
	}

	/**
	 * Execution mode of the {@link MiniDispatcher}.
	 */
	public enum ExecutionMode {
		/**
		 * Waits until the job result has been served.
		 */
		NORMAL,

		/**
		 * Directly stops after the job has finished.
		 */
		DETACHED
	}
}
