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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Optional;

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

	@Nullable
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

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	private Optional<Time> systemResourceMetricsProbingInterval;

	public TaskManagerServicesConfiguration(
			InetAddress taskManagerAddress,
			String[] tmpDirPaths,
			String[] localRecoveryStateRootDirectories,
			boolean localRecoveryEnabled,
			NetworkEnvironmentConfiguration networkConfig,
			@Nullable QueryableStateConfiguration queryableStateConfig,
			int numberOfSlots,
			long configuredMemory,
			MemoryType memoryType,
			boolean preAllocateMemory,
			float memoryFraction,
			long timerServiceShutdownTimeout,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			Optional<Time> systemResourceMetricsProbingInterval) {

		this.taskManagerAddress = checkNotNull(taskManagerAddress);
		this.tmpDirPaths = checkNotNull(tmpDirPaths);
		this.localRecoveryStateRootDirectories = checkNotNull(localRecoveryStateRootDirectories);
		this.localRecoveryEnabled = checkNotNull(localRecoveryEnabled);
		this.networkConfig = checkNotNull(networkConfig);
		this.queryableStateConfig = queryableStateConfig;
		this.numberOfSlots = checkNotNull(numberOfSlots);

		this.configuredMemory = configuredMemory;
		this.memoryType = checkNotNull(memoryType);
		this.preAllocateMemory = preAllocateMemory;
		this.memoryFraction = memoryFraction;

		checkArgument(timerServiceShutdownTimeout >= 0L, "The timer " +
			"service shutdown timeout must be greater or equal to 0.");
		this.timerServiceShutdownTimeout = timerServiceShutdownTimeout;
		this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

		this.systemResourceMetricsProbingInterval = checkNotNull(systemResourceMetricsProbingInterval);
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

	public Optional<Time> getSystemResourceMetricsProbingInterval() {
		return systemResourceMetricsProbingInterval;
	}

	public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
		return retryingRegistrationConfiguration;
	}

	// --------------------------------------------------------------------------------------------
	//  Parsing of Flink configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * Utility method to extract TaskManager config parameters from the configuration and to
	 * sanity check them.
	 *
	 * @param configuration The configuration.
	 * @param maxJvmHeapMemory The maximum JVM heap size, in bytes.
	 * @param remoteAddress identifying the IP address under which the TaskManager will be accessible
	 * @param localCommunication True, to skip initializing the network stack.
	 *                                      Use only in cases where only one task manager runs.
	 * @return TaskExecutorConfiguration that wrappers InstanceConnectionInfo, NetworkEnvironmentConfiguration, etc.
	 */
	public static TaskManagerServicesConfiguration fromConfiguration(
			Configuration configuration,
			long maxJvmHeapMemory,
			InetAddress remoteAddress,
			boolean localCommunication) {

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

		boolean localRecoveryMode = configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY);

		final NetworkEnvironmentConfiguration networkConfig = parseNetworkEnvironmentConfiguration(
			configuration,
			maxJvmHeapMemory,
			localCommunication,
			remoteAddress,
			slots);

		final QueryableStateConfiguration queryableStateConfig = parseQueryableStateConfiguration(configuration);

		boolean preAllocateMemory = configuration.getBoolean(TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE);

		long timerServiceShutdownTimeout = AkkaUtils.getTimeout(configuration).toMillis();

		final RetryingRegistrationConfiguration retryingRegistrationConfiguration = RetryingRegistrationConfiguration.fromConfiguration(configuration);

		return new TaskManagerServicesConfiguration(
			remoteAddress,
			tmpDirs,
			localStateRootDir,
			localRecoveryMode,
			networkConfig,
			queryableStateConfig,
			slots,
			getManagedMemorySize(configuration),
			getMemoryType(configuration),
			preAllocateMemory,
			getManagedMemoryFraction(configuration),
			timerServiceShutdownTimeout,
			retryingRegistrationConfiguration,
			ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));
	}

	// --------------------------------------------------------------------------
	//  Parsing and checking the TaskManager Configuration
	// --------------------------------------------------------------------------

	/**
	 * Creates the {@link NetworkEnvironmentConfiguration} from the given {@link Configuration}.
	 *
	 * @param configuration to create the network environment configuration from
	 * @param maxJvmHeapMemory The maximum JVM heap size, in bytes
	 * @param localTaskManagerCommunication true if task manager communication is local
	 * @param taskManagerAddress address of the task manager
	 * @param slots to start the task manager with
	 * @return Network environment configuration
	 */
	@SuppressWarnings("deprecation")
	private static NetworkEnvironmentConfiguration parseNetworkEnvironmentConfiguration(
		Configuration configuration,
		long maxJvmHeapMemory,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress,
		int slots) {

		// ----> hosts / ports for communication and data exchange

		int dataport = configuration.getInteger(TaskManagerOptions.DATA_PORT);
		checkConfigParameter(dataport >= 0, dataport, TaskManagerOptions.DATA_PORT.key(),
			"Leave config parameter empty or use 0 to let the system choose a port automatically.");

		checkConfigParameter(slots >= 1, slots, TaskManagerOptions.NUM_TASK_SLOTS.key(),
			"Number of task slots must be at least one.");

		final int pageSize = getPageSize(configuration);

		final int numNetworkBuffers;
		if (!hasNewNetworkBufConf(configuration)) {
			// fallback: number of network buffers
			numNetworkBuffers = configuration.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);

			checkNetworkConfigOld(numNetworkBuffers);
		} else {
			if (configuration.contains(TaskManagerOptions.NETWORK_NUM_BUFFERS)) {
				LOG.info("Ignoring old (but still present) network buffer configuration via {}.",
					TaskManagerOptions.NETWORK_NUM_BUFFERS.key());
			}

			final long networkMemorySize = calculateNetworkBufferMemory(configuration, maxJvmHeapMemory);

			// tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
			long numNetworkBuffersLong = networkMemorySize / pageSize;
			if (numNetworkBuffersLong > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("The given number of memory bytes (" + networkMemorySize
					+ ") corresponds to more than MAX_INT pages.");
			}
			numNetworkBuffers = (int) numNetworkBuffersLong;
		}

		final NettyConfig nettyConfig;
		if (!localTaskManagerCommunication) {
			final InetSocketAddress taskManagerInetSocketAddress = new InetSocketAddress(taskManagerAddress, dataport);

			nettyConfig = new NettyConfig(taskManagerInetSocketAddress.getAddress(),
				taskManagerInetSocketAddress.getPort(), pageSize, slots, configuration);
		} else {
			nettyConfig = null;
		}

		int initialRequestBackoff = configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
		int maxRequestBackoff = configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX);

		int buffersPerChannel = configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL);
		int extraBuffersPerGate = configuration.getInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

		boolean isCreditBased = configuration.getBoolean(TaskManagerOptions.NETWORK_CREDIT_MODEL);

		return new NetworkEnvironmentConfiguration(
			numNetworkBuffers,
			pageSize,
			initialRequestBackoff,
			maxRequestBackoff,
			buffersPerChannel,
			extraBuffersPerGate,
			isCreditBased,
			nettyConfig);
	}

	/**
	 * Calculates the amount of memory used for network buffers inside the current JVM instance
	 * based on the available heap or the max heap size and the according configuration parameters.
	 *
	 * <p>For containers or when started via scripts, if started with a memory limit and set to use
	 * off-heap memory, the maximum heap size for the JVM is adjusted accordingly and we are able
	 * to extract the intended values from this.
	 *
	 * <p>The following configuration parameters are involved:
	 * <ul>
	 *  <li>{@link TaskManagerOptions#MANAGED_MEMORY_FRACTION},</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}, and</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_NUM_BUFFERS} (fallback if the ones above do not exist)</li>
	 * </ul>.
	 *
	 * @param configuration configuration object
	 * @param maxJvmHeapMemory the maximum JVM heap size
	 *
	 * @return memory to use for network buffers (in bytes)
	 */
	@VisibleForTesting
	static long calculateNetworkBufferMemory(Configuration configuration, long maxJvmHeapMemory) {
		// The maximum heap memory has been adjusted as in TaskManagerServices#calculateHeapSizeMB
		// and we need to invert these calculations.
		final long jvmHeapNoNet;
		final MemoryType memoryType = getMemoryType(configuration);
		if (memoryType == MemoryType.HEAP) {
			jvmHeapNoNet = maxJvmHeapMemory;
		} else if (memoryType == MemoryType.OFF_HEAP) {
			long configuredMemory = getManagedMemorySize(configuration) << 20; // megabytes to bytes
			if (configuredMemory > 0) {
				// The maximum heap memory has been adjusted according to configuredMemory, i.e.
				// maxJvmHeap = jvmHeapNoNet - configuredMemory
				jvmHeapNoNet = maxJvmHeapMemory + configuredMemory;
			} else {
				// The maximum heap memory has been adjusted according to the fraction, i.e.
				// maxJvmHeap = jvmHeapNoNet - jvmHeapNoNet * managedFraction = jvmHeapNoNet * (1 - managedFraction)
				jvmHeapNoNet = (long) (maxJvmHeapMemory / (1.0 - getManagedMemoryFraction(configuration)));
			}
		} else {
			throw new RuntimeException("No supported memory type detected.");
		}

		float networkBufFraction = configuration.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
		long networkBufMin = MemorySize.parse(configuration.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN)).getBytes();
		long networkBufMax = MemorySize.parse(configuration.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX)).getBytes();
		checkNetworkBufferConfig(getPageSize(configuration), networkBufFraction, networkBufMin, networkBufMax);

		// finally extract the network buffer memory size again from:
		// jvmHeapNoNet = jvmHeap - networkBufBytes
		//              = jvmHeap - Math.min(networkBufMax, Math.max(networkBufMin, jvmHeap * netFraction)
		long networkMemorySize = Math.min(networkBufMax, Math.max(networkBufMin,
			(long) (jvmHeapNoNet / (1.0 - networkBufFraction) * networkBufFraction)));

		TaskManagerServicesConfiguration.checkConfigParameter(networkMemorySize < maxJvmHeapMemory,
			"(" + networkBufFraction + ", " + networkBufMin + ", " + networkBufMax + ")",
			"(" + TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key() + ", " +
				TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key() + ", " +
				TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ")",
			"Network buffer memory size too large: " + networkMemorySize + " >= " +
				maxJvmHeapMemory + "(maximum JVM heap size)");

		return networkMemorySize;
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
		if (!config.getBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER)) {
			return null;
		}

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

	/**
	 * Parses the configuration to get the managed memory size and validates the value.
	 *
	 * @param configuration configuration object
	 * @return managed memory size (in megabytes)
	 */
	private static long getManagedMemorySize(Configuration configuration) {
		long managedMemorySize;
		String managedMemorySizeDefaultVal = TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue();
		if (!configuration.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE).equals(managedMemorySizeDefaultVal)) {
			try {
				managedMemorySize = MemorySize.parse(configuration.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE), MEGA_BYTES).getMebiBytes();
			} catch (IllegalArgumentException e) {
				throw new IllegalConfigurationException("Could not read " + TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), e);
			}
		} else {
			managedMemorySize = Long.valueOf(managedMemorySizeDefaultVal);
		}

		checkConfigParameter(
			configuration.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE).equals(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()) ||
				managedMemorySize > 0, managedMemorySize,
			TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
			"MemoryManager needs at least one MB of memory. " +
				"If you leave this config parameter empty, the system automatically pick a fraction of the available memory.");

		return managedMemorySize;
	}

	/**
	 * Parses the configuration to get the fraction of managed memory and validates the value.
	 *
	 * @param configuration configuration object
	 * @return fraction of managed memory
	 */
	private static float getManagedMemoryFraction(Configuration configuration) {
		float managedMemoryFraction = configuration.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);

		checkConfigParameter(managedMemoryFraction > 0.0f && managedMemoryFraction < 1.0f, managedMemoryFraction,
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
			"MemoryManager fraction of the free memory must be between 0.0 and 1.0");

		return managedMemoryFraction;
	}

	/**
	 * Parses the configuration to get the type of memory.
	 *
	 * @param configuration configuration object
	 * @return type of memory
	 */
	private static MemoryType getMemoryType(Configuration configuration) {
		// check whether we use heap or off-heap memory
		final MemoryType memType;
		if (configuration.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP)) {
			memType = MemoryType.OFF_HEAP;
		} else {
			memType = MemoryType.HEAP;
		}
		return memType;
	}

	/**
	 * Parses the configuration to get the page size and validates the value.
	 *
	 * @param configuration configuration object
	 * @return size of memory segment
	 */
	private static int getPageSize(Configuration configuration) {
		final int pageSize = checkedDownCast(MemorySize.parse(
			configuration.getString(TaskManagerOptions.MEMORY_SEGMENT_SIZE)).getBytes());

		// check page size of for minimum size
		checkConfigParameter(pageSize >= MemoryManager.MIN_PAGE_SIZE, pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);
		// check page size for power of two
		checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Memory segment size must be a power of 2.");

		return pageSize;
	}
}
