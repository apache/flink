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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;

import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;
import static org.apache.flink.util.MathUtils.checkedDownCast;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration for the task manager services such as the network environment, the memory manager,
 * the io manager and the metric registry.
 */
public class TaskManagerServicesConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerServicesConfiguration.class);

	private final InetAddress taskManagerAddress;

	private final String[] tmpDirPaths;

	private final String[] localRecoveryStateRootDirectories;

	private final int numberOfSlots;

	private final NetworkEnvironmentConfiguration networkConfig;

	private final QueryableStateConfiguration queryableStateConfig;

	/**
	 * Managed memory (in megabytes).
	 *
	 * @see TaskManagerOptions#MANAGED_MEMORY_SIZE
	 */
	private final long configuredMemory;

	private final MemoryType memoryType;

	private final boolean preAllocateMemory;

	private final float memoryFraction;

	private final long timerServiceShutdownTimeout;

	private final boolean localRecoveryEnabled;

	public TaskManagerServicesConfiguration(
			InetAddress taskManagerAddress,
			String[] tmpDirPaths,
			String[] localRecoveryStateRootDirectories,
			boolean localRecoveryEnabled,
			NetworkEnvironmentConfiguration networkConfig,
			QueryableStateConfiguration queryableStateConfig,
			int numberOfSlots,
			long configuredMemory,
			MemoryType memoryType,
			boolean preAllocateMemory,
			float memoryFraction,
			long timerServiceShutdownTimeout) {

		this.taskManagerAddress = checkNotNull(taskManagerAddress);
		this.tmpDirPaths = checkNotNull(tmpDirPaths);
		this.localRecoveryStateRootDirectories = checkNotNull(localRecoveryStateRootDirectories);
		this.localRecoveryEnabled = checkNotNull(localRecoveryEnabled);
		this.networkConfig = checkNotNull(networkConfig);
		this.queryableStateConfig = checkNotNull(queryableStateConfig);
		this.numberOfSlots = checkNotNull(numberOfSlots);

		this.configuredMemory = configuredMemory;
		this.memoryType = checkNotNull(memoryType);
		this.preAllocateMemory = preAllocateMemory;
		this.memoryFraction = memoryFraction;

		checkArgument(timerServiceShutdownTimeout >= 0L, "The timer " +
			"service shutdown timeout must be greater or equal to 0.");
		this.timerServiceShutdownTimeout = timerServiceShutdownTimeout;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public InetAddress getTaskManagerAddress() {
		return taskManagerAddress;
	}

	public String[] getTmpDirPaths() {
		return tmpDirPaths;
	}

	public String[] getLocalRecoveryStateRootDirectories() {
		return localRecoveryStateRootDirectories;
	}

	public boolean isLocalRecoveryEnabled() {
		return localRecoveryEnabled;
	}

	public NetworkEnvironmentConfiguration getNetworkConfig() {
		return networkConfig;
	}

	public QueryableStateConfiguration getQueryableStateConfig() {
		return queryableStateConfig;
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	public float getMemoryFraction() {
		return memoryFraction;
	}

	/**
	 * Returns the memory type to use.
	 *
	 * @return on-heap or off-heap memory
	 */
	public MemoryType getMemoryType() {
		return memoryType;
	}

	/**
	 * Returns the size of the managed memory (in megabytes), if configured.
	 *
	 * @return managed memory or a default value (currently <tt>-1</tt>) if not configured
	 *
	 * @see TaskManagerOptions#MANAGED_MEMORY_SIZE
	 */
	public long getConfiguredMemory() {
		return configuredMemory;
	}

	public boolean isPreAllocateMemory() {
		return preAllocateMemory;
	}

	public long getTimerServiceShutdownTimeout() {
		return timerServiceShutdownTimeout;
	}

	// --------------------------------------------------------------------------------------------
	//  Parsing of Flink configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * Utility method to extract TaskManager config parameters from the configuration and to
	 * sanity check them.
	 *
	 * @param configuration The configuration.
	 * @param remoteAddress identifying the IP address under which the TaskManager will be accessible
	 * @param localCommunication True, to skip initializing the network stack.
	 *                                      Use only in cases where only one task manager runs.
	 * @return TaskExecutorConfiguration that wrappers InstanceConnectionInfo, NetworkEnvironmentConfiguration, etc.
	 */
	public static TaskManagerServicesConfiguration fromConfiguration(
			Configuration configuration,
			InetAddress remoteAddress,
			boolean localCommunication) throws Exception {

		// we need this because many configs have been written with a "-1" entry
		int slots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		if (slots == -1) {
			slots = 1;
		}

		final String[] tmpDirs = ConfigurationUtils.parseTempDirectories(configuration);
		String[] localStateRootDir = ConfigurationUtils.parseLocalStateDirectories(configuration);

		if (localStateRootDir.length == 0) {
			// default to temp dirs.
			localStateRootDir = tmpDirs;
		}

		boolean localRecoveryMode = configuration.getBoolean(
			CheckpointingOptions.LOCAL_RECOVERY.key(),
			CheckpointingOptions.LOCAL_RECOVERY.defaultValue());

		final NetworkEnvironmentConfiguration networkConfig = parseNetworkEnvironmentConfiguration(
			configuration,
			localCommunication,
			remoteAddress,
			slots);

		final QueryableStateConfiguration queryableStateConfig =
				parseQueryableStateConfiguration(configuration);

		// extract memory settings
		long configuredMemory;
		String managedMemorySizeDefaultVal = TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue();
		if (!configuration.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE).equals(managedMemorySizeDefaultVal)) {
			try {
				configuredMemory = MemorySize.parse(configuration.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE), MEGA_BYTES).getMebiBytes();
			} catch (IllegalArgumentException e) {
				throw new IllegalConfigurationException(
					"Could not read " + TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), e);
			}
		} else {
			configuredMemory = Long.valueOf(managedMemorySizeDefaultVal);
		}

		checkConfigParameter(
			configuration.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE).equals(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()) ||
				configuredMemory > 0, configuredMemory,
			TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
			"MemoryManager needs at least one MB of memory. " +
				"If you leave this config parameter empty, the system automatically " +
				"pick a fraction of the available memory.");

		// check whether we use heap or off-heap memory
		final MemoryType memType;
		if (configuration.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP)) {
			memType = MemoryType.OFF_HEAP;
		} else {
			memType = MemoryType.HEAP;
		}

		boolean preAllocateMemory = configuration.getBoolean(TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE);

		float memoryFraction = configuration.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
		checkConfigParameter(memoryFraction > 0.0f && memoryFraction < 1.0f, memoryFraction,
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
			"MemoryManager fraction of the free memory must be between 0.0 and 1.0");

		long timerServiceShutdownTimeout = AkkaUtils.getTimeout(configuration).toMillis();

		return new TaskManagerServicesConfiguration(
			remoteAddress,
			tmpDirs,
			localStateRootDir,
			localRecoveryMode,
			networkConfig,
			queryableStateConfig,
			slots,
			configuredMemory,
			memType,
			preAllocateMemory,
			memoryFraction,
			timerServiceShutdownTimeout);
	}

	// --------------------------------------------------------------------------
	//  Parsing and checking the TaskManager Configuration
	// --------------------------------------------------------------------------

	/**
	 * Creates the {@link NetworkEnvironmentConfiguration} from the given {@link Configuration}.
	 *
	 * @param configuration to create the network environment configuration from
	 * @param localTaskManagerCommunication true if task manager communication is local
	 * @param taskManagerAddress address of the task manager
	 * @param slots to start the task manager with
	 * @return Network environment configuration
	 */
	@SuppressWarnings("deprecation")
	private static NetworkEnvironmentConfiguration parseNetworkEnvironmentConfiguration(
		Configuration configuration,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress,
		int slots) throws Exception {

		// ----> hosts / ports for communication and data exchange

		int dataport = configuration.getInteger(TaskManagerOptions.DATA_PORT);

		checkConfigParameter(dataport >= 0, dataport, TaskManagerOptions.DATA_PORT.key(),
			"Leave config parameter empty or use 0 to let the system choose a port automatically.");

		checkConfigParameter(slots >= 1, slots, TaskManagerOptions.NUM_TASK_SLOTS.key(),
			"Number of task slots must be at least one.");

		final int pageSize = checkedDownCast(MemorySize.parse(configuration.getString(TaskManagerOptions.MEMORY_SEGMENT_SIZE)).getBytes());

		// check page size of for minimum size
		checkConfigParameter(pageSize >= MemoryManager.MIN_PAGE_SIZE, pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);

		// check page size for power of two
		checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Memory segment size must be a power of 2.");

		// network buffer memory fraction

		float networkBufFraction = configuration.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
		long networkBufMin = MemorySize.parse(configuration.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN)).getBytes();
		long networkBufMax = MemorySize.parse(configuration.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX)).getBytes();
		checkNetworkBufferConfig(pageSize, networkBufFraction, networkBufMin, networkBufMax);

		// fallback: number of network buffers
		final int numNetworkBuffers = configuration.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);
		checkNetworkConfigOld(numNetworkBuffers);

		if (!hasNewNetworkBufConf(configuration)) {
			// map old config to new one:
			networkBufMin = networkBufMax = ((long) numNetworkBuffers) * pageSize;
		} else {
			if (configuration.contains(TaskManagerOptions.NETWORK_NUM_BUFFERS)) {
				LOG.info("Ignoring old (but still present) network buffer configuration via {}.",
					TaskManagerOptions.NETWORK_NUM_BUFFERS.key());
			}
		}

		final NettyConfig nettyConfig;
		if (!localTaskManagerCommunication) {
			final InetSocketAddress taskManagerInetSocketAddress = new InetSocketAddress(taskManagerAddress, dataport);

			nettyConfig = new NettyConfig(taskManagerInetSocketAddress.getAddress(),
				taskManagerInetSocketAddress.getPort(), pageSize, slots, configuration);
		} else {
			nettyConfig = null;
		}

		// Default spill I/O mode for intermediate results
		final String syncOrAsync = configuration.getString(
			ConfigConstants.TASK_MANAGER_NETWORK_DEFAULT_IO_MODE,
			ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE);

		final IOManager.IOMode ioMode;
		if (syncOrAsync.equals("async")) {
			ioMode = IOManager.IOMode.ASYNC;
		} else {
			ioMode = IOManager.IOMode.SYNC;
		}

		int initialRequestBackoff = configuration.getInteger(
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
		int maxRequestBackoff = configuration.getInteger(
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX);

		int buffersPerChannel = configuration.getInteger(
			TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL);
		int extraBuffersPerGate = configuration.getInteger(
			TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

		return new NetworkEnvironmentConfiguration(
			networkBufFraction,
			networkBufMin,
			networkBufMax,
			pageSize,
			ioMode,
			initialRequestBackoff,
			maxRequestBackoff,
			buffersPerChannel,
			extraBuffersPerGate,
			nettyConfig);
	}

	/**
	 * Validates the (old) network buffer configuration.
	 *
	 * @param numNetworkBuffers		number of buffers used in the network stack
	 *
	 * @throws IllegalConfigurationException if the condition does not hold
	 */
	@SuppressWarnings("deprecation")
	protected static void checkNetworkConfigOld(final int numNetworkBuffers) {
		checkConfigParameter(numNetworkBuffers > 0, numNetworkBuffers,
			TaskManagerOptions.NETWORK_NUM_BUFFERS.key(),
			"Must have at least one network buffer");
	}

	/**
	 * Validates the (new) network buffer configuration.
	 *
	 * @param pageSize 				size of memory buffers
	 * @param networkBufFraction	fraction of JVM memory to use for network buffers
	 * @param networkBufMin 		minimum memory size for network buffers (in bytes)
	 * @param networkBufMax 		maximum memory size for network buffers (in bytes)
	 *
	 * @throws IllegalConfigurationException if the condition does not hold
	 */
	protected static void checkNetworkBufferConfig(
			final int pageSize, final float networkBufFraction, final long networkBufMin,
			final long networkBufMax) throws IllegalConfigurationException {

		checkConfigParameter(networkBufFraction > 0.0f && networkBufFraction < 1.0f, networkBufFraction,
			TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(),
			"Network buffer memory fraction of the free memory must be between 0.0 and 1.0");

		checkConfigParameter(networkBufMin >= pageSize, networkBufMin,
			TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key(),
			"Minimum memory for network buffers must allow at least one network " +
				"buffer with respect to the memory segment size");

		checkConfigParameter(networkBufMax >= pageSize, networkBufMax,
			TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key(),
			"Maximum memory for network buffers must allow at least one network " +
				"buffer with respect to the memory segment size");

		checkConfigParameter(networkBufMax >= networkBufMin, networkBufMax,
			TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key(),
			"Maximum memory for network buffers must not be smaller than minimum memory (" +
				TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ": " + networkBufMin + ")");
	}

	/**
	 * Returns whether the new network buffer memory configuration is present in the configuration
	 * object, i.e. at least one new parameter is given or the old one is not present.
	 *
	 * @param config configuration object
	 * @return <tt>true</tt> if the new configuration method is used, <tt>false</tt> otherwise
	 */
	@SuppressWarnings("deprecation")
	public static boolean hasNewNetworkBufConf(final Configuration config) {
		return config.contains(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION) ||
			config.contains(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN) ||
			config.contains(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX) ||
			!config.contains(TaskManagerOptions.NETWORK_NUM_BUFFERS);
	}

	/**
	 * Creates the {@link QueryableStateConfiguration} from the given Configuration.
	 */
	private static QueryableStateConfiguration parseQueryableStateConfiguration(Configuration config) {

		final Iterator<Integer> proxyPorts = NetUtils.getPortRangeFromString(
				config.getString(QueryableStateOptions.PROXY_PORT_RANGE));
		final Iterator<Integer> serverPorts = NetUtils.getPortRangeFromString(
				config.getString(QueryableStateOptions.SERVER_PORT_RANGE));

		final int numProxyServerNetworkThreads = config.getInteger(QueryableStateOptions.PROXY_NETWORK_THREADS);
		final int numProxyServerQueryThreads = config.getInteger(QueryableStateOptions.PROXY_ASYNC_QUERY_THREADS);

		final int numStateServerNetworkThreads = config.getInteger(QueryableStateOptions.SERVER_NETWORK_THREADS);
		final int numStateServerQueryThreads = config.getInteger(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS);

		return new QueryableStateConfiguration(
				proxyPorts,
				serverPorts,
				numProxyServerNetworkThreads,
				numProxyServerQueryThreads,
				numStateServerNetworkThreads,
				numStateServerQueryThreads);
	}

	/**
	 * Validates a condition for a config parameter and displays a standard exception, if the
	 * the condition does not hold.
	 *
	 * @param condition             The condition that must hold. If the condition is false, an exception is thrown.
	 * @param parameter         The parameter value. Will be shown in the exception message.
	 * @param name              The name of the config parameter. Will be shown in the exception message.
	 * @param errorMessage  The optional custom error message to append to the exception message.
	 *
	 * @throws IllegalConfigurationException if the condition does not hold
	 */
	static void checkConfigParameter(boolean condition, Object parameter, String name, String errorMessage)
			throws IllegalConfigurationException {
		if (!condition) {
			throw new IllegalConfigurationException("Invalid configuration value for " +
					name + " : " + parameter + " - " + errorMessage);
		}
	}
}
