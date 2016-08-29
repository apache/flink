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

import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts$;
import akka.util.Timeout;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.NetUtils;

import scala.Tuple2;
import scala.Option;
import scala.Some;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link org.apache.flink.runtime.taskmanager.Task}.
 */
public class TaskExecutor extends RpcEndpoint<TaskExecutorGateway> {

	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

	/** The unique resource ID of this TaskExecutor */
	private final ResourceID resourceID;

	/** The access to the leader election and metadata storage services */
	private final HighAvailabilityServices haServices;

	/** The task manager configuration */
	private final TaskExecutorConfiguration taskExecutorConfig;

	/** The I/O manager component in the task manager */
	private final IOManager ioManager;

	/** The memory manager component in the task manager */
	private final MemoryManager memoryManager;

	/** The network component in the task manager */
	private final NetworkEnvironment networkEnvironment;

	/** The number of slots in the task manager, should be 1 for YARN */
	private final int numberOfSlots;

	// --------- resource manager --------

	private TaskExecutorToResourceManagerConnection resourceManagerConnection;

	// ------------------------------------------------------------------------

	public TaskExecutor(
			TaskExecutorConfiguration taskExecutorConfig,
			ResourceID resourceID,
			MemoryManager memoryManager,
			IOManager ioManager,
			NetworkEnvironment networkEnvironment,
			int numberOfSlots,
			RpcService rpcService,
			HighAvailabilityServices haServices) {

		super(rpcService);

		this.taskExecutorConfig = checkNotNull(taskExecutorConfig);
		this.resourceID = checkNotNull(resourceID);
		this.memoryManager = checkNotNull(memoryManager);
		this.ioManager = checkNotNull(ioManager);
		this.networkEnvironment = checkNotNull(networkEnvironment);
		this.numberOfSlots = checkNotNull(numberOfSlots);
		this.haServices = checkNotNull(haServices);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@Override
	public void start() {
		super.start();

		// start by connecting to the ResourceManager
		try {
			haServices.getResourceManagerLeaderRetriever().start(new ResourceManagerLeaderListener());
		} catch (Exception e) {
			onFatalErrorAsync(e);
		}
	}

	// ------------------------------------------------------------------------
	//  RPC methods - ResourceManager related
	// ------------------------------------------------------------------------

	@RpcMethod
	public void notifyOfNewResourceManagerLeader(String newLeaderAddress, UUID newLeaderId) {
		if (resourceManagerConnection != null) {
			if (newLeaderAddress != null) {
				// the resource manager switched to a new leader
				log.info("ResourceManager leader changed from {} to {}. Registering at new leader.",
					resourceManagerConnection.getResourceManagerAddress(), newLeaderAddress);
			}
			else {
				// address null means that the current leader is lost without a new leader being there, yet
				log.info("Current ResourceManager {} lost leader status. Waiting for new ResourceManager leader.",
					resourceManagerConnection.getResourceManagerAddress());
			}

			// drop the current connection or connection attempt
			if (resourceManagerConnection != null) {
				resourceManagerConnection.close();
				resourceManagerConnection = null;
			}
		}

		// establish a connection to the new leader
		if (newLeaderAddress != null) {
			log.info("Attempting to register at ResourceManager {}", newLeaderAddress);
			resourceManagerConnection =
				new TaskExecutorToResourceManagerConnection(log, this, newLeaderAddress, newLeaderId);
			resourceManagerConnection.start();
		}
	}

	/**
	 * Starts and runs the TaskManager.
	 * <p/>
	 * This method first tries to select the network interface to use for the TaskManager
	 * communication. The network interface is used both for the actor communication
	 * (coordination) as well as for the data exchange between task managers. Unless
	 * the hostname/interface is explicitly configured in the configuration, this
	 * method will try out various interfaces and methods to connect to the JobManager
	 * and select the one where the connection attempt is successful.
	 * <p/>
	 * After selecting the network interface, this method brings up an actor system
	 * for the TaskManager and its actors, starts the TaskManager's services
	 * (library cache, shuffle network stack, ...), and starts the TaskManager itself.
	 *
	 * @param configuration    The configuration for the TaskManager.
	 * @param resourceID       The id of the resource which the task manager will run on.
	 */
	public static void selectNetworkInterfaceAndRunTaskManager(
		Configuration configuration,
		ResourceID resourceID) throws Exception {

		final InetSocketAddress taskManagerAddress = selectNetworkInterfaceAndPort(configuration);

		runTaskManager(taskManagerAddress.getHostName(), resourceID, taskManagerAddress.getPort(), configuration);
	}

	private static InetSocketAddress selectNetworkInterfaceAndPort(Configuration configuration)
		throws Exception {
		String taskManagerHostname = configuration.getString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null);
		if (taskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: " + taskManagerHostname);
		} else {
			LeaderRetrievalService leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(configuration);
			FiniteDuration lookupTimeout = AkkaUtils.getLookupTimeout(configuration);

			InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(leaderRetrievalService, lookupTimeout);
			taskManagerHostname = taskManagerAddress.getHostName();
			LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
				taskManagerHostname, taskManagerAddress.getHostAddress());
		}

		// if no task manager port has been configured, use 0 (system will pick any free port)
		final int actorSystemPort = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0);
		if (actorSystemPort < 0 || actorSystemPort > 65535) {
			throw new IllegalConfigurationException("Invalid value for '" +
				ConfigConstants.TASK_MANAGER_IPC_PORT_KEY +
				"' (port for the TaskManager actor system) : " + actorSystemPort +
				" - Leave config parameter empty or use 0 to let the system choose a port automatically.");
		}

		return new InetSocketAddress(taskManagerHostname, actorSystemPort);
	}

	/**
	 * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
	 * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
	 * and starts the TaskManager itself.
	 * <p/>
	 * This method will also spawn a process reaper for the TaskManager (kill the process if
	 * the actor fails) and optionally start the JVM memory logging thread.
	 *
	 * @param taskManagerHostname The hostname/address of the interface where the actor system
	 *                            will communicate.
	 * @param resourceID          The id of the resource which the task manager will run on.
	 * @param actorSystemPort   The port at which the actor system will communicate.
	 * @param configuration       The configuration for the TaskManager.
	 */
	private static void runTaskManager(
		String taskManagerHostname,
		ResourceID resourceID,
		int actorSystemPort,
		final Configuration configuration) throws Exception {

		LOG.info("Starting TaskManager");

		// Bring up the TaskManager actor system first, bind it to the given address.

		LOG.info("Starting TaskManager actor system at " +
			NetUtils.hostAndPortToUrlString(taskManagerHostname, actorSystemPort));

		final ActorSystem taskManagerSystem;
		try {
			Tuple2<String, Object> address = new Tuple2<String, Object>(taskManagerHostname, actorSystemPort);
			Config akkaConfig = AkkaUtils.getAkkaConfig(configuration, new Some<>(address));
			LOG.debug("Using akka configuration\n " + akkaConfig);
			taskManagerSystem = AkkaUtils.createActorSystem(akkaConfig);
		} catch (Throwable t) {
			if (t instanceof org.jboss.netty.channel.ChannelException) {
				Throwable cause = t.getCause();
				if (cause != null && t.getCause() instanceof java.net.BindException) {
					String address = NetUtils.hostAndPortToUrlString(taskManagerHostname, actorSystemPort);
					throw new IOException("Unable to bind TaskManager actor system to address " +
						address + " - " + cause.getMessage(), t);
				}
			}
			throw new Exception("Could not create TaskManager actor system", t);
		}

		// start akka rpc service based on actor system
		final Timeout timeout = new Timeout(AkkaUtils.getTimeout(configuration).toMillis(), TimeUnit.MILLISECONDS);
		final AkkaRpcService akkaRpcService = new AkkaRpcService(taskManagerSystem, timeout);

		// start high availability service to implement getResourceManagerLeaderRetriever method only
		final HighAvailabilityServices haServices = new HighAvailabilityServices() {
			@Override
			public LeaderRetrievalService getResourceManagerLeaderRetriever() throws Exception {
				return LeaderRetrievalUtils.createLeaderRetrievalService(configuration);
			}

			@Override
			public LeaderElectionService getResourceManagerLeaderElectionService() throws Exception {
				return null;
			}

			@Override
			public LeaderElectionService getJobMasterLeaderElectionService(JobID jobID) throws Exception {
				return null;
			}
		};

		// start all the TaskManager services (network stack,  library cache, ...)
		// and the TaskManager actor
		try {
			LOG.info("Starting TaskManager actor");
			TaskExecutor taskExecutor = startTaskManagerComponentsAndActor(
				configuration,
				resourceID,
				akkaRpcService,
				taskManagerHostname,
				haServices,
				false);

			taskExecutor.start();

			// if desired, start the logging daemon that periodically logs the memory usage information
			if (LOG.isInfoEnabled() && configuration.getBoolean(
				ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
				ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD)) {
				LOG.info("Starting periodic memory usage logger");

				long interval = configuration.getLong(
					ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS,
					ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS);

				MemoryLogger logger = new MemoryLogger(LOG, interval, taskManagerSystem);
				logger.start();
			}

			// block until everything is done
			taskManagerSystem.awaitTermination();
		} catch (Throwable t) {
			LOG.error("Error while starting up taskManager", t);
			try {
				taskManagerSystem.shutdown();
			} catch (Throwable tt) {
				LOG.warn("Could not cleanly shut down actor system", tt);
			}
			throw t;
		}
	}

	// --------------------------------------------------------------------------
	//  Starting and running the TaskManager
	// --------------------------------------------------------------------------

	/**
	 * @param configuration                 The configuration for the TaskManager.
	 * @param resourceID                    The id of the resource which the task manager will run on.
	 * @param rpcService                  The rpc service which is used to start and connect to the TaskManager RpcEndpoint .
	 * @param taskManagerHostname       The hostname/address that describes the TaskManager's data location.
	 * @param haServices        Optionally, a high availability service can be provided. If none is given,
	 *                                      then a HighAvailabilityServices is constructed from the configuration.
	 * @param localTaskManagerCommunication     If true, the TaskManager will not initiate the TCP network stack.
	 * @return An ActorRef to the TaskManager actor.
	 * @throws org.apache.flink.configuration.IllegalConfigurationException     Thrown, if the given config contains illegal values.
	 * @throws java.io.IOException      Thrown, if any of the I/O components (such as buffer pools,
	 *                                       I/O manager, ...) cannot be properly started.
	 * @throws java.lang.Exception      Thrown is some other error occurs while parsing the configuration
	 *                                      or starting the TaskManager components.
	 */
	public static TaskExecutor startTaskManagerComponentsAndActor(
		Configuration configuration,
		ResourceID resourceID,
		RpcService rpcService,
		String taskManagerHostname,
		HighAvailabilityServices haServices,
		boolean localTaskManagerCommunication) throws Exception {

		final TaskExecutorConfiguration taskExecutorConfig = parseTaskManagerConfiguration(
			configuration, taskManagerHostname, localTaskManagerCommunication);

		MemoryType memType = taskExecutorConfig.getNetworkConfig().memoryType();

		// pre-start checks
		checkTempDirs(taskExecutorConfig.getTmpDirPaths());

		ExecutionContext executionContext = ExecutionContexts$.MODULE$.fromExecutor(new ForkJoinPool());

		// we start the network first, to make sure it can allocate its buffers first
		final NetworkEnvironment network = new NetworkEnvironment(
			executionContext,
			taskExecutorConfig.getTimeout(),
			taskExecutorConfig.getNetworkConfig(),
			taskExecutorConfig.getConnectionInfo());

		// computing the amount of memory to use depends on how much memory is available
		// it strictly needs to happen AFTER the network stack has been initialized

		// check if a value has been configured
		long configuredMemory = configuration.getLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1L);
		checkConfigParameter(configuredMemory == -1 || configuredMemory > 0, configuredMemory,
			ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY,
			"MemoryManager needs at least one MB of memory. " +
				"If you leave this config parameter empty, the system automatically " +
				"pick a fraction of the available memory.");

		final long memorySize;
		boolean preAllocateMemory = configuration.getBoolean(
			ConfigConstants.TASK_MANAGER_MEMORY_PRE_ALLOCATE_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_PRE_ALLOCATE);
		if (configuredMemory > 0) {
			if (preAllocateMemory) {
				LOG.info("Using {} MB for managed memory." , configuredMemory);
			} else {
				LOG.info("Limiting managed memory to {} MB, memory will be allocated lazily." , configuredMemory);
			}
			memorySize = configuredMemory << 20; // megabytes to bytes
		} else {
			float fraction = configuration.getFloat(
				ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
				ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);
			checkConfigParameter(fraction > 0.0f && fraction < 1.0f, fraction,
				ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
				"MemoryManager fraction of the free memory must be between 0.0 and 1.0");

			if (memType == MemoryType.HEAP) {
				long relativeMemSize = (long) (EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag() * fraction);
				if (preAllocateMemory) {
					LOG.info("Using {} of the currently free heap space for managed heap memory ({} MB)." ,
						fraction , relativeMemSize >> 20);
				} else {
					LOG.info("Limiting managed memory to {} of the currently free heap space ({} MB), " +
						"memory will be allocated lazily." , fraction , relativeMemSize >> 20);
				}
				memorySize = relativeMemSize;
			} else if (memType == MemoryType.OFF_HEAP) {
				// The maximum heap memory has been adjusted according to the fraction
				long maxMemory = EnvironmentInformation.getMaxJvmHeapMemory();
				long directMemorySize = (long) (maxMemory / (1.0 - fraction) * fraction);
				if (preAllocateMemory) {
					LOG.info("Using {} of the maximum memory size for managed off-heap memory ({} MB)." ,
						fraction, directMemorySize >> 20);
				} else {
					LOG.info("Limiting managed memory to {} of the maximum memory size ({} MB)," +
						" memory will be allocated lazily.", fraction, directMemorySize >> 20);
				}
				memorySize = directMemorySize;
			} else {
				throw new RuntimeException("No supported memory type detected.");
			}
		}

		// now start the memory manager
		final MemoryManager memoryManager;
		try {
			memoryManager = new MemoryManager(
				memorySize,
				taskExecutorConfig.getNumberOfSlots(),
				taskExecutorConfig.getNetworkConfig().networkBufferSize(),
				memType,
				preAllocateMemory);
		} catch (OutOfMemoryError e) {
			if (memType == MemoryType.HEAP) {
				throw new Exception("OutOfMemory error (" + e.getMessage() +
					") while allocating the TaskManager heap memory (" + memorySize + " bytes).", e);
			} else if (memType == MemoryType.OFF_HEAP) {
				throw new Exception("OutOfMemory error (" + e.getMessage() +
					") while allocating the TaskManager off-heap memory (" + memorySize +
					" bytes).Try increasing the maximum direct memory (-XX:MaxDirectMemorySize)", e);
			} else {
				throw e;
			}
		}

		// start the I/O manager, it will create some temp directories.
		final IOManager ioManager = new IOManagerAsync(taskExecutorConfig.getTmpDirPaths());

		final TaskExecutor taskExecutor = new TaskExecutor(
			taskExecutorConfig,
			resourceID,
			memoryManager,
			ioManager,
			network,
			taskExecutorConfig.getNumberOfSlots(),
			rpcService,
			haServices);

		return taskExecutor;
	}

	// --------------------------------------------------------------------------
	//  Parsing and checking the TaskManager Configuration
	// --------------------------------------------------------------------------

	/**
	 * Utility method to extract TaskManager config parameters from the configuration and to
	 * sanity check them.
	 *
	 * @param configuration                 The configuration.
	 * @param taskManagerHostname           The host name under which the TaskManager communicates.
	 * @param localTaskManagerCommunication             True, to skip initializing the network stack.
	 *                                      Use only in cases where only one task manager runs.
	 * @return TaskExecutorConfiguration that wrappers InstanceConnectionInfo, NetworkEnvironmentConfiguration, etc.
	 */
	private static TaskExecutorConfiguration parseTaskManagerConfiguration(
		Configuration configuration,
		String taskManagerHostname,
		boolean localTaskManagerCommunication) throws Exception {

		// ------- read values from the config and check them ---------
		//                      (a lot of them)

		// ----> hosts / ports for communication and data exchange

		int dataport = configuration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);
		if (dataport == 0) {
			dataport = NetUtils.getAvailablePort();
		}
		checkConfigParameter(dataport > 0, dataport, ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
			"Leave config parameter empty or use 0 to let the system choose a port automatically.");

		InetAddress taskManagerAddress = InetAddress.getByName(taskManagerHostname);
		final InstanceConnectionInfo connectionInfo = new InstanceConnectionInfo(taskManagerAddress, dataport);

		// ----> memory / network stack (shuffles/broadcasts), task slots, temp directories

		// we need this because many configs have been written with a "-1" entry
		int slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
		if (slots == -1) {
			slots = 1;
		}
		checkConfigParameter(slots >= 1, slots, ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
			"Number of task slots must be at least one.");

		final int numNetworkBuffers = configuration.getInteger(
			ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS);
		checkConfigParameter(numNetworkBuffers > 0, numNetworkBuffers,
			ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, "");

		final int pageSize = configuration.getInteger(
			ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE);
		// check page size of for minimum size
		checkConfigParameter(pageSize >= MemoryManager.MIN_PAGE_SIZE, pageSize,
			ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
			"Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);
		// check page size for power of two
		checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
			ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
			"Memory segment size must be a power of 2.");

		// check whether we use heap or off-heap memory
		final MemoryType memType;
		if (configuration.getBoolean(ConfigConstants.TASK_MANAGER_MEMORY_OFF_HEAP_KEY, false)) {
			memType = MemoryType.OFF_HEAP;
		} else {
			memType = MemoryType.HEAP;
		}

		// initialize the memory segment factory accordingly
		if (memType == MemoryType.HEAP) {
			if (!MemorySegmentFactory.initializeIfNotInitialized(HeapMemorySegment.FACTORY)) {
				throw new Exception("Memory type is set to heap memory, but memory segment " +
					"factory has been initialized for off-heap memory segments");
			}
		} else {
			if (!MemorySegmentFactory.initializeIfNotInitialized(HybridMemorySegment.FACTORY)) {
				throw new Exception("Memory type is set to off-heap memory, but memory segment " +
					"factory has been initialized for heap memory segments");
			}
		}

		final String[] tmpDirs = configuration.getString(
			ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(",|" + File.pathSeparator);

		final NettyConfig nettyConfig;
		if (!localTaskManagerCommunication) {
			nettyConfig = new NettyConfig(connectionInfo.address(), connectionInfo.dataPort(), pageSize, slots, configuration);
		} else {
			nettyConfig = null;
		}

		// Default spill I/O mode for intermediate results
		final String syncOrAsync = configuration.getString(
			ConfigConstants.TASK_MANAGER_NETWORK_DEFAULT_IO_MODE,
			ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE);

		final IOMode ioMode;
		if (syncOrAsync.equals("async")) {
			ioMode = IOManager.IOMode.ASYNC;
		} else {
			ioMode = IOManager.IOMode.SYNC;
		}

		final int queryServerPort =  configuration.getInteger(
			ConfigConstants.QUERYABLE_STATE_SERVER_PORT,
			ConfigConstants.DEFAULT_QUERYABLE_STATE_SERVER_PORT);

		final int queryServerNetworkThreads =  configuration.getInteger(
			ConfigConstants.QUERYABLE_STATE_SERVER_NETWORK_THREADS,
			ConfigConstants.DEFAULT_QUERYABLE_STATE_SERVER_NETWORK_THREADS);

		final int queryServerQueryThreads =  configuration.getInteger(
			ConfigConstants.QUERYABLE_STATE_SERVER_QUERY_THREADS,
			ConfigConstants.DEFAULT_QUERYABLE_STATE_SERVER_QUERY_THREADS);

		final NetworkEnvironmentConfiguration networkConfig = new NetworkEnvironmentConfiguration(
			numNetworkBuffers,
			pageSize,
			memType,
			ioMode,
			queryServerPort,
			queryServerNetworkThreads,
			queryServerQueryThreads,
			localTaskManagerCommunication ? Option.<NettyConfig>empty() : new Some<>(nettyConfig),
			new Tuple2<>(500, 3000));

		// ----> timeouts, library caching, profiling

		final FiniteDuration timeout;
		try {
			timeout = AkkaUtils.getTimeout(configuration);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"Invalid format for '" + ConfigConstants.AKKA_ASK_TIMEOUT +
					"'.Use formats like '50 s' or '1 min' to specify the timeout.");
		}
		LOG.info("Messages between TaskManager and JobManager have a max timeout of " + timeout);

		final long cleanupInterval = configuration.getLong(
			ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
			ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000;

		final FiniteDuration finiteRegistrationDuration;
		try {
			Duration maxRegistrationDuration = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION,
				ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION));
			if (maxRegistrationDuration.isFinite()) {
				finiteRegistrationDuration = new FiniteDuration(maxRegistrationDuration.toSeconds(), TimeUnit.SECONDS);
			} else {
				finiteRegistrationDuration = null;
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION, e);
		}

		final FiniteDuration initialRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE,
				ConfigConstants.DEFAULT_TASK_MANAGER_INITIAL_REGISTRATION_PAUSE));
			if (pause.isFinite()) {
				initialRegistrationPause = new FiniteDuration(pause.toSeconds(), TimeUnit.SECONDS);
			} else {
				throw new IllegalArgumentException("The initial registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE, e);
		}

		final FiniteDuration maxRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_MAX_REGISTARTION_PAUSE,
				ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_PAUSE));
			if (pause.isFinite()) {
				maxRegistrationPause = new FiniteDuration(pause.toSeconds(), TimeUnit.SECONDS);
			} else {
				throw new IllegalArgumentException("The maximum registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE, e);
		}

		final FiniteDuration refusedRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_REFUSED_REGISTRATION_PAUSE,
				ConfigConstants.DEFAULT_TASK_MANAGER_REFUSED_REGISTRATION_PAUSE));
			if (pause.isFinite()) {
				refusedRegistrationPause = new FiniteDuration(pause.toSeconds(), TimeUnit.SECONDS);
			} else {
				throw new IllegalArgumentException("The refused registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE, e);
		}

		return new TaskExecutorConfiguration(
			tmpDirs,
			cleanupInterval,
			connectionInfo,
			networkConfig,
			timeout,
			finiteRegistrationDuration,
			slots,
			configuration,
			initialRegistrationPause,
			maxRegistrationPause,
			refusedRegistrationPause);
	}

	/**
	 * Validates a condition for a config parameter and displays a standard exception, if the
	 * the condition does not hold.
	 *
	 * @param condition    The condition that must hold. If the condition is false, an exception is thrown.
	 * @param parameter    The parameter value. Will be shown in the exception message.
	 * @param name         The name of the config parameter. Will be shown in the exception message.
	 * @param errorMessage The optional custom error message to append to the exception message.
	 */
	private static void checkConfigParameter(
		boolean condition,
		Object parameter,
		String name,
		String errorMessage) {
		if (!condition) {
			throw new IllegalConfigurationException("Invalid configuration value for " + name + " : " + parameter + " - " + errorMessage);
		}
	}

	/**
	 * Validates that all the directories denoted by the strings do actually exist, are proper
	 * directories (not files), and are writable.
	 *
	 * @param tmpDirs The array of directory paths to check.
	 * @throws Exception Thrown if any of the directories does not exist or is not writable
	 *                   or is a file, rather than a directory.
	 */
	private static void checkTempDirs(String[] tmpDirs) throws IOException {
		for (String dir : tmpDirs) {
			if (dir != null && !dir.equals("")) {
				File file = new File(dir);
				if (!file.exists()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " does not exist.");
				}
				if (!file.isDirectory()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not a directory.");
				}
				if (!file.canWrite()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not writable.");
				}

				if (LOG.isInfoEnabled()) {
					long totalSpaceGb = file.getTotalSpace() >> 30;
					long usableSpaceGb = file.getUsableSpace() >> 30;
					double usablePercentage = (double)usableSpaceGb / totalSpaceGb * 100;
					String path = file.getAbsolutePath();
					LOG.info(String.format("Temporary file directory '%s': total %d GB, " + "usable %d GB (%.2f%% usable)",
						path, totalSpaceGb, usableSpaceGb, usablePercentage));
				}
			} else {
				throw new IllegalArgumentException("Temporary file directory #$id is null.");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public ResourceID getResourceID() {
		return resourceID;
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 * This method should be used when asynchronous threads want to notify the
	 * TaskExecutor of a fatal error.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalErrorAsync(final Throwable t) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				onFatalError(t);
			}
		});
	}

	/**
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 * This method must only be called from within the TaskExecutor's main thread.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalError(Throwable t) {
		// to be determined, probably delegate to a fatal error handler that 
		// would either log (mini cluster) ot kill the process (yarn, mesos, ...)
		log.error("FATAL ERROR", t);
	}

	// ------------------------------------------------------------------------
	//  Access to fields for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	TaskExecutorToResourceManagerConnection getResourceManagerConnection() {
		return resourceManagerConnection;
	}

	// ------------------------------------------------------------------------
	//  Utility classes
	// ------------------------------------------------------------------------

	/**
	 * The listener for leader changes of the resource manager
	 */
	private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
			getSelf().notifyOfNewResourceManagerLeader(leaderAddress, leaderSessionID);
		}

		@Override
		public void handleError(Exception exception) {
			onFatalErrorAsync(exception);
		}
	}
}
