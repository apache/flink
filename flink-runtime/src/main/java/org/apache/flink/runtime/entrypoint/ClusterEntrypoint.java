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
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcMetricQueryServiceRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 */
public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

	public static final ConfigOption<String> EXECUTION_MODE = ConfigOptions
		.key("internal.cluster.execution-mode")
		.defaultValue(ExecutionMode.NORMAL.toString());

	protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

	protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
	protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private static final Time INITIALIZATION_SHUTDOWN_TIMEOUT = Time.seconds(30L);

	/** The lock to guard startup / shutdown / manipulation methods. */
	private final Object lock = new Object();

	private final Configuration configuration;

	private final CompletableFuture<ApplicationStatus> terminationFuture;

	private final AtomicBoolean isShutDown = new AtomicBoolean(false);

	@GuardedBy("lock")
	private DispatcherResourceManagerComponent<?> clusterComponent;

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
	private ExecutorService ioExecutor;

	private ArchivedExecutionGraphStore archivedExecutionGraphStore;

	private final Thread shutDownHook;

	protected ClusterEntrypoint(Configuration configuration) {
		this.configuration = generateClusterConfiguration(configuration);
		this.terminationFuture = new CompletableFuture<>();

		shutDownHook = ShutdownHookUtil.addShutdownHook(this::cleanupDirectories, getClass().getSimpleName(), LOG);
	}

	public CompletableFuture<ApplicationStatus> getTerminationFuture() {
		return terminationFuture;
	}

	public void startCluster() throws ClusterEntrypointException {
		LOG.info("Starting {}.", getClass().getSimpleName());

		try {

			configureFileSystems(configuration);

			SecurityContext securityContext = installSecurityContext(configuration);

			securityContext.runSecured((Callable<Void>) () -> {
				runCluster(configuration);

				return null;
			});
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);

			try {
				// clean up any partial state
				shutDownAsync(
					ApplicationStatus.FAILED,
					ExceptionUtils.stringifyException(strippedThrowable),
					false).get(INITIALIZATION_SHUTDOWN_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				strippedThrowable.addSuppressed(e);
			}

			throw new ClusterEntrypointException(
				String.format("Failed to initialize the cluster entrypoint %s.", getClass().getSimpleName()),
				strippedThrowable);
		}
	}

	private void configureFileSystems(Configuration configuration) {
		LOG.info("Install default filesystem.");
		FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));
	}

	protected SecurityContext installSecurityContext(Configuration configuration) throws Exception {
		LOG.info("Install security context.");

		SecurityUtils.install(new SecurityConfiguration(configuration));

		return SecurityUtils.getInstalledContext();
	}

	private void runCluster(Configuration configuration) throws Exception {
		synchronized (lock) {
			initializeServices(configuration);

			// write host information into configuration
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

			final DispatcherResourceManagerComponentFactory<?> dispatcherResourceManagerComponentFactory = createDispatcherResourceManagerComponentFactory(configuration);

			clusterComponent = dispatcherResourceManagerComponentFactory.create(
				configuration,
				commonRpcService,
				haServices,
				blobServer,
				heartbeatServices,
				metricRegistry,
				archivedExecutionGraphStore,
				new RpcMetricQueryServiceRetriever(metricRegistry.getMetricQueryServiceRpcService()),
				this);

			clusterComponent.getShutDownFuture().whenComplete(
				(ApplicationStatus applicationStatus, Throwable throwable) -> {
					if (throwable != null) {
						shutDownAsync(
							ApplicationStatus.UNKNOWN,
							ExceptionUtils.stringifyException(throwable),
							false);
					} else {
						// This is the general shutdown path. If a separate more specific shutdown was
						// already triggered, this will do nothing
						shutDownAsync(
							applicationStatus,
							null,
							true);
					}
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

			ioExecutor = Executors.newFixedThreadPool(
				Hardware.getNumberCPUCores(),
				new ExecutorThreadFactory("cluster-io"));
			haServices = createHaServices(configuration, ioExecutor);
			blobServer = new BlobServer(configuration, haServices.createBlobStore());
			blobServer.start();
			heartbeatServices = createHeartbeatServices(configuration);
			metricRegistry = createMetricRegistry(configuration);

			final RpcService metricQueryServiceRpcService = MetricUtils.startMetricsRpcService(configuration, bindAddress);
			metricRegistry.startQueryService(metricQueryServiceRpcService, null);

			archivedExecutionGraphStore = createSerializableExecutionGraphStore(configuration, commonRpcService.getScheduledExecutor());
		}
	}

	@Nonnull
	private RpcService createRpcService(Configuration configuration, String bindAddress, String portRange) throws Exception {
		return AkkaRpcServiceUtils.createRpcService(bindAddress, portRange, configuration);
	}

	/**
	 * Returns the port range for the common {@link RpcService}.
	 *
	 * @param configuration to extract the port range from
	 * @return Port range for the common {@link RpcService}
	 */
	protected String getRPCPortRange(Configuration configuration) {
		if (ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
			return configuration.getString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE);
		} else {
			return String.valueOf(configuration.getInteger(JobManagerOptions.PORT));
		}
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
		return new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration),
			ReporterSetup.fromConfiguration(configuration));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return shutDownAsync(
			ApplicationStatus.UNKNOWN,
			"Cluster entrypoint has been closed externally.",
			true).thenAccept(ignored -> {});
	}

	protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
		final long shutdownTimeout = configuration.getLong(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT);

		synchronized (lock) {
			Throwable exception = null;

			final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

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

			if (archivedExecutionGraphStore != null) {
				try {
					archivedExecutionGraphStore.close();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (metricRegistry != null) {
				terminationFutures.add(metricRegistry.shutdown());
			}

			if (ioExecutor != null) {
				terminationFutures.add(ExecutorUtils.nonBlockingShutdown(shutdownTimeout, TimeUnit.MILLISECONDS, ioExecutor));
			}

			if (commonRpcService != null) {
				terminationFutures.add(commonRpcService.stopService());
			}

			if (exception != null) {
				terminationFutures.add(FutureUtils.completedExceptionally(exception));
			}

			return FutureUtils.completeAll(terminationFutures);
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

	private Configuration generateClusterConfiguration(Configuration configuration) {
		final Configuration resultConfiguration = new Configuration(Preconditions.checkNotNull(configuration));

		final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);
		final File uniqueWebTmpDir = new File(webTmpDir, "flink-web-" + UUID.randomUUID());

		resultConfiguration.setString(WebOptions.TMP_DIR, uniqueWebTmpDir.getAbsolutePath());

		return resultConfiguration;
	}

	private CompletableFuture<ApplicationStatus> shutDownAsync(
			ApplicationStatus applicationStatus,
			@Nullable String diagnostics,
			boolean cleanupHaData) {
		if (isShutDown.compareAndSet(false, true)) {
			LOG.info("Shutting {} down with application status {}. Diagnostics {}.",
				getClass().getSimpleName(),
				applicationStatus,
				diagnostics);

			final CompletableFuture<Void> shutDownApplicationFuture = closeClusterComponent(applicationStatus, diagnostics);

			final CompletableFuture<Void> serviceShutdownFuture = FutureUtils.composeAfterwards(
				shutDownApplicationFuture,
				() -> stopClusterServices(cleanupHaData));

			final CompletableFuture<Void> cleanupDirectoriesFuture = FutureUtils.runAfterwards(
				serviceShutdownFuture,
				this::cleanupDirectories);

			cleanupDirectoriesFuture.whenComplete(
				(Void ignored2, Throwable serviceThrowable) -> {
					if (serviceThrowable != null) {
						terminationFuture.completeExceptionally(serviceThrowable);
					} else {
						terminationFuture.complete(applicationStatus);
					}
				});
		}

		return terminationFuture;
	}

	/**
	 * Deregister the Flink application from the resource management system by signalling
	 * the {@link ResourceManager}.
	 *
	 * @param applicationStatus to terminate the application with
	 * @param diagnostics additional information about the shut down, can be {@code null}
	 * @return Future which is completed once the shut down
	 */
	private CompletableFuture<Void> closeClusterComponent(ApplicationStatus applicationStatus, @Nullable String diagnostics) {
		synchronized (lock) {
			if (clusterComponent != null) {
				return clusterComponent.deregisterApplicationAndClose(applicationStatus, diagnostics);
			} else {
				return CompletableFuture.completedFuture(null);
			}
		}
	}

	/**
	 * Clean up of temporary directories created by the {@link ClusterEntrypoint}.
	 *
	 * @throws IOException if the temporary directories could not be cleaned up
	 */
	private void cleanupDirectories() throws IOException {
		ShutdownHookUtil.removeShutdownHook(shutDownHook, getClass().getSimpleName(), LOG);

		final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);

		FileUtils.deleteDirectory(new File(webTmpDir));
	}

	// --------------------------------------------------
	// Abstract methods
	// --------------------------------------------------

	protected abstract DispatcherResourceManagerComponentFactory<?> createDispatcherResourceManagerComponentFactory(Configuration configuration);

	protected abstract ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
		Configuration configuration,
		ScheduledExecutor scheduledExecutor) throws IOException;

	protected static EntrypointClusterConfiguration parseArguments(String[] args) throws FlinkParseException {
		final CommandLineParser<EntrypointClusterConfiguration> clusterConfigurationParser = new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

		return clusterConfigurationParser.parse(args);
	}

	protected static Configuration loadConfiguration(EntrypointClusterConfiguration entrypointClusterConfiguration) {
		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(entrypointClusterConfiguration.getDynamicProperties());
		final Configuration configuration = GlobalConfiguration.loadConfiguration(entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

		final int restPort = entrypointClusterConfiguration.getRestPort();

		if (restPort >= 0) {
			configuration.setInteger(RestOptions.PORT, restPort);
		}

		final String hostname = entrypointClusterConfiguration.getHostname();

		if (hostname != null) {
			configuration.setString(JobManagerOptions.ADDRESS, hostname);
		}

		return configuration;
	}

	// --------------------------------------------------
	// Helper methods
	// --------------------------------------------------

	public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {

		final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
		try {
			clusterEntrypoint.startCluster();
		} catch (ClusterEntrypointException e) {
			LOG.error(String.format("Could not start cluster entrypoint %s.", clusterEntrypointName), e);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}

		clusterEntrypoint.getTerminationFuture().whenComplete((applicationStatus, throwable) -> {
			final int returnCode;

			if (throwable != null) {
				returnCode = RUNTIME_FAILURE_RETURN_CODE;
			} else {
				returnCode = applicationStatus.processExitCode();
			}

			LOG.info("Terminating cluster entrypoint process {} with exit code {}.", clusterEntrypointName, returnCode, throwable);
			System.exit(returnCode);
		});
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
