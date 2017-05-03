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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.util.MathUtils;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration for the task manager services such as the network environment, the memory manager,
 * the io manager and the metric registry
 */
public class TaskManagerServicesConfiguration {

	private final InetAddress taskManagerAddress;

	private final String[] tmpDirPaths;

	private final int numberOfSlots;

	private final NetworkEnvironmentConfiguration networkConfig;

	private final QueryableStateConfiguration queryableStateConfig;

	private final long configuredMemory;

	private final boolean preAllocateMemory;

	private final float memoryFraction;

	private final MetricRegistryConfiguration metricRegistryConfiguration;

	private final long timerServiceShutdownTimeout;

	public TaskManagerServicesConfiguration(
			InetAddress taskManagerAddress,
			String[] tmpDirPaths,
			NetworkEnvironmentConfiguration networkConfig,
			QueryableStateConfiguration queryableStateConfig,
			int numberOfSlots,
			long configuredMemory,
			boolean preAllocateMemory,
			float memoryFraction,
			MetricRegistryConfiguration metricRegistryConfiguration,
			long timerServiceShutdownTimeout) {

		this.taskManagerAddress = checkNotNull(taskManagerAddress);
		this.tmpDirPaths = checkNotNull(tmpDirPaths);
		this.networkConfig = checkNotNull(networkConfig);
		this.queryableStateConfig = checkNotNull(queryableStateConfig);
		this.numberOfSlots = checkNotNull(numberOfSlots);

		this.configuredMemory = configuredMemory;
		this.preAllocateMemory = preAllocateMemory;
		this.memoryFraction = memoryFraction;

		this.metricRegistryConfiguration = checkNotNull(metricRegistryConfiguration);

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

	public long getConfiguredMemory() {
		return configuredMemory;
	}

	public boolean isPreAllocateMemory() {
		return preAllocateMemory;
	}

	public MetricRegistryConfiguration getMetricRegistryConfiguration() {
		return metricRegistryConfiguration;
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
		int slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
		if (slots == -1) {
			slots = 1;
		}

		final String[] tmpDirs = configuration.getString(
			ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(",|" + File.pathSeparator);

		final NetworkEnvironmentConfiguration networkConfig = parseNetworkEnvironmentConfiguration(
			configuration,
			localCommunication,
			remoteAddress,
			slots);

		final QueryableStateConfiguration queryableStateConfig = localCommunication ?
				QueryableStateConfiguration.disabled() :
				parseQueryableStateConfiguration(configuration);

		// extract memory settings
		long configuredMemory = configuration.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);
		checkConfigParameter(
			configuredMemory == TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue() ||
				configuredMemory > 0, configuredMemory,
			TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
			"MemoryManager needs at least one MB of memory. " +
				"If you leave this config parameter empty, the system automatically " +
				"pick a fraction of the available memory.");

		boolean preAllocateMemory = configuration.getBoolean(TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE);

		float memoryFraction = configuration.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
		checkConfigParameter(memoryFraction > 0.0f && memoryFraction < 1.0f, memoryFraction,
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
			"MemoryManager fraction of the free memory must be between 0.0 and 1.0");

		final MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(configuration);

		long timerServiceShutdownTimeout = AkkaUtils.getTimeout(configuration).toMillis();

		return new TaskManagerServicesConfiguration(
			remoteAddress,
			tmpDirs,
			networkConfig,
			queryableStateConfig,
			slots,
			configuredMemory,
			preAllocateMemory,
			memoryFraction,
			metricRegistryConfiguration,
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
	private static NetworkEnvironmentConfiguration parseNetworkEnvironmentConfiguration(
		Configuration configuration,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress,
		int slots) throws Exception {

		// ----> hosts / ports for communication and data exchange

		int dataport = configuration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);

		checkConfigParameter(dataport >= 0, dataport, ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
			"Leave config parameter empty or use 0 to let the system choose a port automatically.");

		checkConfigParameter(slots >= 1, slots, ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
			"Number of task slots must be at least one.");

		final int numNetworkBuffers = configuration.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);

		checkConfigParameter(numNetworkBuffers > 0, numNetworkBuffers,
			TaskManagerOptions.NETWORK_NUM_BUFFERS.key(), "");

		final int pageSize = configuration.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);

		// check page size of for minimum size
		checkConfigParameter(pageSize >= MemoryManager.MIN_PAGE_SIZE, pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);

		// check page size for power of two
		checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Memory segment size must be a power of 2.");

		// check whether we use heap or off-heap memory
		final MemoryType memType;
		if (configuration.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP)) {
			memType = MemoryType.OFF_HEAP;
		} else {
			memType = MemoryType.HEAP;
		}

		// initialize the memory segment factory accordingly
		// TODO - this should be in the TaskManager, not the configuration
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
			numNetworkBuffers,
			pageSize,
			memType,
			ioMode,
			initialRequestBackoff,
			maxRequestBackoff,
			buffersPerChannel,
			extraBuffersPerGate,
			nettyConfig);
	}

	/**
	 * Creates the {@link QueryableStateConfiguration} from the given Configuration.
	 */
	private static QueryableStateConfiguration parseQueryableStateConfiguration(Configuration config) {
		final boolean enabled = config.getBoolean(QueryableStateOptions.SERVER_ENABLE);

		if (enabled) {
			int port = config.getInteger(QueryableStateOptions.SERVER_PORT);
			int numNetworkThreads = config.getInteger(QueryableStateOptions.SERVER_NETWORK_THREADS);
			int numQueryThreads = config.getInteger(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS);
			return new QueryableStateConfiguration(true, port, numNetworkThreads, numQueryThreads);
		}
		else {
			return QueryableStateConfiguration.disabled();
		}
	}

	/**
	 * Validates a condition for a config parameter and displays a standard exception, if the
	 * the condition does not hold.
	 *
	 * @param condition             The condition that must hold. If the condition is false, an exception is thrown.
	 * @param parameter         The parameter value. Will be shown in the exception message.
	 * @param name              The name of the config parameter. Will be shown in the exception message.
	 * @param errorMessage  The optional custom error message to append to the exception message.
	 */
	private static void checkConfigParameter(boolean condition, Object parameter, String name, String errorMessage) {
		if (!condition) {
			throw new IllegalConfigurationException("Invalid configuration value for " + 
					name + " : " + parameter + " - " + errorMessage);
		}
	}
}

