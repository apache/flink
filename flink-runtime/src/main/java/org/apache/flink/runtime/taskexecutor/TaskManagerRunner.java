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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.NetUtils;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode.
 * It constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

	private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;

	private static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private final Object lock = new Object();

	private final Configuration configuration;

	private final ResourceID resourceId;

	private final Time timeout;

	private final RpcService rpcService;

	private final HighAvailabilityServices highAvailabilityServices;

	private final MetricRegistryImpl metricRegistry;

	private final BlobCacheService blobCacheService;

	/** Executor used to run future callbacks. */
	private final ExecutorService executor;

	private final TaskExecutor taskManager;

	private final CompletableFuture<Void> terminationFuture;

	private boolean shutdown;

	public TaskManagerRunner(Configuration configuration, ResourceID resourceId) throws Exception {
		this.configuration = checkNotNull(configuration);
		this.resourceId = checkNotNull(resourceId);

		timeout = AkkaUtils.getTimeoutAsTime(configuration);

		this.executor = java.util.concurrent.Executors.newScheduledThreadPool(
			Hardware.getNumberCPUCores(),
			new ExecutorThreadFactory("taskmanager-future"));

		highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

		rpcService = createRpcService(configuration, highAvailabilityServices);

		HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

		metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration));

		// TODO: Temporary hack until the MetricQueryService has been ported to RpcEndpoint
		final ActorSystem actorSystem = ((AkkaRpcService) rpcService).getActorSystem();
		metricRegistry.startQueryService(actorSystem, resourceId);

		blobCacheService = new BlobCacheService(
			configuration, highAvailabilityServices.createBlobStore(), null
		);

		taskManager = startTaskManager(
			this.configuration,
			this.resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			blobCacheService,
			false,
			this);

		this.terminationFuture = new CompletableFuture<>();
		this.shutdown = false;

		MemoryLogger.startIfConfigured(LOG, configuration, actorSystem);
	}

	// --------------------------------------------------------------------------------------------
	//  Lifecycle management
	// --------------------------------------------------------------------------------------------

	public void start() throws Exception {
		taskManager.start();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				taskManager.shutDown();
				final CompletableFuture<Void> taskManagerTerminationFuture = taskManager.getTerminationFuture();

				final CompletableFuture<Void> serviceTerminationFuture = FutureUtils.composeAfterwards(
					taskManagerTerminationFuture,
					this::shutDownServices);

				serviceTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							terminationFuture.completeExceptionally(throwable);
						} else {
							terminationFuture.complete(null);
						}
					});
			}
		}

		return terminationFuture;
	}

	private CompletableFuture<Void> shutDownServices() {
		synchronized (lock) {
			Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
			Exception exception = null;

			try {
				blobCacheService.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				metricRegistry.shutdown();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				highAvailabilityServices.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			terminationFutures.add(rpcService.stopService());

			terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

			if (exception != null) {
				terminationFutures.add(FutureUtils.completedExceptionally(exception));
			}

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	// export the termination future for caller to know it is terminated
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  FatalErrorHandler methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);

		if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)) {
			terminateJVM();
		} else {
			closeAsync();

			FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

			terminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					terminateJVM();
				});
		}
	}

	protected void terminateJVM() {
		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	// --------------------------------------------------------------------------------------------
	//  Static entry point
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}

		final Configuration configuration = loadConfiguration(args);

		try {
			FileSystem.initialize(configuration);
		} catch (IOException e) {
			throw new IOException("Error while setting the default " +
				"filesystem scheme from configuration.", e);
		}

		SecurityUtils.install(new SecurityConfiguration(configuration));

		try {
			SecurityUtils.getInstalledContext().runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					runTaskManager(configuration, ResourceID.generate());
					return null;
				}
			});
		} catch (Throwable t) {
			LOG.error("TaskManager initialization failed.", t);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	private static Configuration loadConfiguration(String[] args) throws FlinkParseException {
		final CommandLineParser<ClusterConfiguration> commandLineParser = new CommandLineParser<>(new ClusterConfigurationParserFactory());

		final ClusterConfiguration clusterConfiguration;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse the command line options.", e);
			commandLineParser.printHelp();
			throw e;
		}

		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
		return GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir(), dynamicProperties);
	}

	public static void runTaskManager(Configuration configuration, ResourceID resourceId) throws Exception {
		final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, resourceId);

		taskManagerRunner.start();
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	public static TaskExecutor startTaskManager(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		checkNotNull(configuration);
		checkNotNull(resourceID);
		checkNotNull(rpcService);
		checkNotNull(highAvailabilityServices);

		InetAddress remoteAddress = InetAddress.getByName(rpcService.getAddress());

		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(
				configuration,
				remoteAddress,
				localCommunicationOnly);

		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			resourceID,
			rpcService.getExecutor(), // TODO replace this later with some dedicated executor for io.
			EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag(),
			EnvironmentInformation.getMaxJvmHeapMemory());

		TaskManagerMetricGroup taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
			metricRegistry,
			taskManagerServices.getTaskManagerLocation(),
			taskManagerServices.getNetworkEnvironment());

		TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);

		return new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			highAvailabilityServices,
			taskManagerServices,
			heartbeatServices,
			taskManagerMetricGroup,
			blobCacheService,
			fatalErrorHandler);
	}

	/**
	 * Create a RPC service for the task manager.
	 *
	 * @param configuration The configuration for the TaskManager.
	 * @param haServices to use for the task manager hostname retrieval
	 */
	public static RpcService createRpcService(
		final Configuration configuration,
		final HighAvailabilityServices haServices) throws Exception {

		checkNotNull(configuration);
		checkNotNull(haServices);

		String taskManagerHostname = configuration.getString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null);

		if (taskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: {}.", taskManagerHostname);
		} else {
			Time lookupTimeout = Time.milliseconds(AkkaUtils.getLookupTimeout(configuration).toMillis());

			InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
				haServices.getResourceManagerLeaderRetriever(),
				lookupTimeout);

			taskManagerHostname = taskManagerAddress.getHostName();

			LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
				taskManagerHostname, taskManagerAddress.getHostAddress());
		}

		final String portRangeDefinition = configuration.getString(TaskManagerOptions.RPC_PORT);

		// parse port range definition and create port iterator
		Iterator<Integer> portsIterator;
		try {
			portsIterator = NetUtils.getPortRangeFromString(portRangeDefinition);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid port range definition: " + portRangeDefinition);
		}

		while (portsIterator.hasNext()) {
			try {
				return AkkaRpcServiceUtils.createRpcService(taskManagerHostname, portsIterator.next(), configuration);
			}
			catch (Exception e) {
				// we can continue to try if this contains a netty channel exception
				Throwable cause = e.getCause();
				if (!(cause instanceof org.jboss.netty.channel.ChannelException ||
					cause instanceof java.net.BindException)) {
					throw e;
				} // else fall through the loop and try the next port
			}
		}

		// if we come here, we have exhausted the port range
		throw new BindException("Could not start task manager on any port in port range "
			+ portRangeDefinition);
	}
}
