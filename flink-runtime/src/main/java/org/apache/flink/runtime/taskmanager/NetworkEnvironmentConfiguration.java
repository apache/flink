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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.flink.util.MathUtils.checkedDownCast;

/**
 * Configuration object for the network stack.
 */
public class NetworkEnvironmentConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(NetworkEnvironmentConfiguration.class);

	private final int numNetworkBuffers;

	private final int networkBufferSize;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	/** Number of network buffers to use for each outgoing/incoming channel (subpartition/input channel). */
	private final int networkBuffersPerChannel;

	/** Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate). */
	private final int floatingNetworkBuffersPerGate;

	private final boolean isCreditBased;

	private final boolean isNetworkDetailedMetrics;

	private final NettyConfig nettyConfig;

	public NetworkEnvironmentConfiguration(
			int numNetworkBuffers,
			int networkBufferSize,
			int partitionRequestInitialBackoff,
			int partitionRequestMaxBackoff,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			boolean isCreditBased,
			boolean isNetworkDetailedMetrics,
			@Nullable NettyConfig nettyConfig) {

		this.numNetworkBuffers = numNetworkBuffers;
		this.networkBufferSize = networkBufferSize;
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.isCreditBased = isCreditBased;
		this.isNetworkDetailedMetrics = isNetworkDetailedMetrics;
		this.nettyConfig = nettyConfig;
	}

	// ------------------------------------------------------------------------

	public int numNetworkBuffers() {
		return numNetworkBuffers;
	}

	public int networkBufferSize() {
		return networkBufferSize;
	}

	public int partitionRequestInitialBackoff() {
		return partitionRequestInitialBackoff;
	}

	public int partitionRequestMaxBackoff() {
		return partitionRequestMaxBackoff;
	}

	public int networkBuffersPerChannel() {
		return networkBuffersPerChannel;
	}

	public int floatingNetworkBuffersPerGate() {
		return floatingNetworkBuffersPerGate;
	}

	public NettyConfig nettyConfig() {
		return nettyConfig;
	}

	public boolean isCreditBased() {
		return isCreditBased;
	}

	public boolean isNetworkDetailedMetrics() {
		return isNetworkDetailedMetrics;
	}

	// ------------------------------------------------------------------------

	/**
	 * Utility method to extract network related parameters from the configuration and to
	 * sanity check them.
	 *
	 * @param configuration configuration object
	 * @param maxJvmHeapMemory the maximum JVM heap size (in bytes)
	 * @param localTaskManagerCommunication true, to skip initializing the network stack
	 * @param taskManagerAddress identifying the IP address under which the TaskManager will be accessible
	 * @return NetworkEnvironmentConfiguration
	 */
	public static NetworkEnvironmentConfiguration fromConfiguration(
		Configuration configuration,
		long maxJvmHeapMemory,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress) {

		final int dataport = getDataport(configuration);

		final int pageSize = getPageSize(configuration);

		final int numberOfNetworkBuffers = calculateNumberOfNetworkBuffers(configuration, maxJvmHeapMemory);

		final NettyConfig nettyConfig = createNettyConfig(configuration, localTaskManagerCommunication, taskManagerAddress, dataport);

		int initialRequestBackoff = configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
		int maxRequestBackoff = configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX);

		int buffersPerChannel = configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL);
		int extraBuffersPerGate = configuration.getInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

		boolean isCreditBased = nettyConfig != null && configuration.getBoolean(TaskManagerOptions.NETWORK_CREDIT_MODEL);

		boolean isNetworkDetailedMetrics = configuration.getBoolean(TaskManagerOptions.NETWORK_DETAILED_METRICS);

		return new NetworkEnvironmentConfiguration(
			numberOfNetworkBuffers,
			pageSize,
			initialRequestBackoff,
			maxRequestBackoff,
			buffersPerChannel,
			extraBuffersPerGate,
			isCreditBased,
			isNetworkDetailedMetrics,
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
	 *  <li>{@link TaskManagerOptions#MANAGED_MEMORY_SIZE},</li>
	 *  <li>{@link TaskManagerOptions#MANAGED_MEMORY_FRACTION},</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}, and</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_NUM_BUFFERS} (fallback if the ones above do not exist)</li>
	 * </ul>.
	 *
	 * @param config configuration object
	 * @param maxJvmHeapMemory the maximum JVM heap size (in bytes)
	 *
	 * @return memory to use for network buffers (in bytes)
	 */
	@VisibleForTesting
	public static long calculateNewNetworkBufferMemory(Configuration config, long maxJvmHeapMemory) {
		// The maximum heap memory has been adjusted as in TaskManagerServices#calculateHeapSizeMB
		// and we need to invert these calculations.
		final long jvmHeapNoNet;
		final MemoryType memoryType = ConfigurationParserUtils.getMemoryType(config);
		if (memoryType == MemoryType.HEAP) {
			jvmHeapNoNet = maxJvmHeapMemory;
		} else if (memoryType == MemoryType.OFF_HEAP) {
			long configuredMemory = ConfigurationParserUtils.getManagedMemorySize(config) << 20; // megabytes to bytes
			if (configuredMemory > 0) {
				// The maximum heap memory has been adjusted according to configuredMemory, i.e.
				// maxJvmHeap = jvmHeapNoNet - configuredMemory
				jvmHeapNoNet = maxJvmHeapMemory + configuredMemory;
			} else {
				// The maximum heap memory has been adjusted according to the fraction, i.e.
				// maxJvmHeap = jvmHeapNoNet - jvmHeapNoNet * managedFraction = jvmHeapNoNet * (1 - managedFraction)
				jvmHeapNoNet = (long) (maxJvmHeapMemory / (1.0 - ConfigurationParserUtils.getManagedMemoryFraction(config)));
			}
		} else {
			throw new RuntimeException("No supported memory type detected.");
		}

		// finally extract the network buffer memory size again from:
		// jvmHeapNoNet = jvmHeap - networkBufBytes
		//              = jvmHeap - Math.min(networkBufMax, Math.max(networkBufMin, jvmHeap * netFraction)
		// jvmHeap = jvmHeapNoNet / (1.0 - networkBufFraction)
		float networkBufFraction = config.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
		long networkBufSize = (long) (jvmHeapNoNet / (1.0 - networkBufFraction) * networkBufFraction);
		return calculateNewNetworkBufferMemory(config, networkBufSize, maxJvmHeapMemory);
	}

	/**
	 * Calculates the amount of memory used for network buffers based on the total memory to use and
	 * the according configuration parameters.
	 *
	 * <p>The following configuration parameters are involved:
	 * <ul>
	 *  <li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}, and</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_NUM_BUFFERS} (fallback if the ones above do not exist)</li>
	 * </ul>.
	 *
	 * @param totalJavaMemorySize overall available memory to use (in bytes)
	 * @param config configuration object
	 *
	 * @return memory to use for network buffers (in bytes)
	 */
	@SuppressWarnings("deprecation")
	public static long calculateNetworkBufferMemory(long totalJavaMemorySize, Configuration config) {
		final int segmentSize = getPageSize(config);

		final long networkBufBytes;
		if (hasNewNetworkConfig(config)) {
			float networkBufFraction = config.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
			long networkBufSize = (long) (totalJavaMemorySize * networkBufFraction);
			networkBufBytes = calculateNewNetworkBufferMemory(config, networkBufSize, totalJavaMemorySize);
		} else {
			// use old (deprecated) network buffers parameter
			int numNetworkBuffers = config.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);
			networkBufBytes = (long) numNetworkBuffers * (long) segmentSize;

			checkOldNetworkConfig(numNetworkBuffers);

			ConfigurationParserUtils.checkConfigParameter(networkBufBytes < totalJavaMemorySize,
				networkBufBytes, TaskManagerOptions.NETWORK_NUM_BUFFERS.key(),
				"Network buffer memory size too large: " + networkBufBytes + " >= " +
					totalJavaMemorySize + " (total JVM memory size)");
		}

		return networkBufBytes;
	}

	/**
	 * Calculates the amount of memory used for network buffers based on the total memory to use and
	 * the according configuration parameters.
	 *
	 * <p>The following configuration parameters are involved:
	 * <ul>
	 *  <li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}</li>
	 * </ul>.
	 *
	 * @param config configuration object
	 * @param networkBufSize memory of network buffers based on JVM memory size and network fraction
	 * @param maxJvmHeapMemory maximum memory used for checking the results of network memory
	 *
	 * @return memory to use for network buffers (in bytes)
	 */
	private static long calculateNewNetworkBufferMemory(Configuration config, long networkBufSize, long maxJvmHeapMemory) {
		float networkBufFraction = config.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
		long networkBufMin = MemorySize.parse(config.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN)).getBytes();
		long networkBufMax = MemorySize.parse(config.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX)).getBytes();

		int pageSize = getPageSize(config);

		checkNewNetworkConfig(pageSize, networkBufFraction, networkBufMin, networkBufMax);

		long networkBufBytes = Math.min(networkBufMax, Math.max(networkBufMin, networkBufSize));

		ConfigurationParserUtils.checkConfigParameter(networkBufBytes < maxJvmHeapMemory,
			"(" + networkBufFraction + ", " + networkBufMin + ", " + networkBufMax + ")",
			"(" + TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key() + ", " +
				TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key() + ", " +
				TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ")",
			"Network buffer memory size too large: " + networkBufBytes + " >= " +
				maxJvmHeapMemory + " (maximum JVM memory size)");

		return networkBufBytes;
	}

	/**
	 * Validates the (old) network buffer configuration.
	 *
	 * @param numNetworkBuffers	number of buffers used in the network stack
	 *
	 * @throws IllegalConfigurationException if the condition does not hold
	 */
	@SuppressWarnings("deprecation")
	private static void checkOldNetworkConfig(final int numNetworkBuffers) {
		ConfigurationParserUtils.checkConfigParameter(numNetworkBuffers > 0, numNetworkBuffers,
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
	private static void checkNewNetworkConfig(
		final int pageSize,
		final float networkBufFraction,
		final long networkBufMin,
		final long networkBufMax) throws IllegalConfigurationException {

		ConfigurationParserUtils.checkConfigParameter(networkBufFraction > 0.0f && networkBufFraction < 1.0f, networkBufFraction,
			TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(),
			"Network buffer memory fraction of the free memory must be between 0.0 and 1.0");

		ConfigurationParserUtils.checkConfigParameter(networkBufMin >= pageSize, networkBufMin,
			TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key(),
			"Minimum memory for network buffers must allow at least one network " +
				"buffer with respect to the memory segment size");

		ConfigurationParserUtils.checkConfigParameter(networkBufMax >= pageSize, networkBufMax,
			TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key(),
			"Maximum memory for network buffers must allow at least one network " +
				"buffer with respect to the memory segment size");

		ConfigurationParserUtils.checkConfigParameter(networkBufMax >= networkBufMin, networkBufMax,
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
	@VisibleForTesting
	public static boolean hasNewNetworkConfig(final Configuration config) {
		return config.contains(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION) ||
			config.contains(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN) ||
			config.contains(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX) ||
			!config.contains(TaskManagerOptions.NETWORK_NUM_BUFFERS);
	}

	/**
	 * Parses the hosts / ports for communication and data exchange from configuration.
	 *
	 * @param configuration configuration object
	 * @return the data port
	 */
	private static int getDataport(Configuration configuration) {
		final int dataport = configuration.getInteger(TaskManagerOptions.DATA_PORT);
		ConfigurationParserUtils.checkConfigParameter(dataport >= 0, dataport, TaskManagerOptions.DATA_PORT.key(),
			"Leave config parameter empty or use 0 to let the system choose a port automatically.");

		return dataport;
	}

	/**
	 * Calculates the number of network buffers based on configuration and jvm heap size.
	 *
	 * @param configuration configuration object
	 * @param maxJvmHeapMemory the maximum JVM heap size (in bytes)
	 * @return the number of network buffers
	 */
	@SuppressWarnings("deprecation")
	private static int calculateNumberOfNetworkBuffers(Configuration configuration, long maxJvmHeapMemory) {
		final int numberOfNetworkBuffers;
		if (!hasNewNetworkConfig(configuration)) {
			// fallback: number of network buffers
			numberOfNetworkBuffers = configuration.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);

			checkOldNetworkConfig(numberOfNetworkBuffers);
		} else {
			if (configuration.contains(TaskManagerOptions.NETWORK_NUM_BUFFERS)) {
				LOG.info("Ignoring old (but still present) network buffer configuration via {}.",
					TaskManagerOptions.NETWORK_NUM_BUFFERS.key());
			}

			final long networkMemorySize = calculateNewNetworkBufferMemory(configuration, maxJvmHeapMemory);

			// tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
			long numberOfNetworkBuffersLong = networkMemorySize / getPageSize(configuration);
			if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("The given number of memory bytes (" + networkMemorySize
					+ ") corresponds to more than MAX_INT pages.");
			}
			numberOfNetworkBuffers = (int) numberOfNetworkBuffersLong;
		}

		return numberOfNetworkBuffers;
	}

	/**
	 * Generates {@link NettyConfig} from Flink {@link Configuration}.
	 *
	 * @param configuration configuration object
	 * @param localTaskManagerCommunication true, to skip initializing the network stack
	 * @param taskManagerAddress identifying the IP address under which the TaskManager will be accessible
	 * @param dataport data port for communication and data exchange
	 * @return the netty configuration or {@code null} if communication is in the same task manager
	 */
	@Nullable
	private static NettyConfig createNettyConfig(
		Configuration configuration,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress,
		int dataport) {

		final NettyConfig nettyConfig;
		if (!localTaskManagerCommunication) {
			final InetSocketAddress taskManagerInetSocketAddress = new InetSocketAddress(taskManagerAddress, dataport);

			nettyConfig = new NettyConfig(taskManagerInetSocketAddress.getAddress(), taskManagerInetSocketAddress.getPort(),
				getPageSize(configuration), ConfigurationParserUtils.getSlot(configuration), configuration);
		} else {
			nettyConfig = null;
		}

		return nettyConfig;
	}

	/**
	 * Parses the configuration to get the page size and validates the value.
	 *
	 * @param configuration configuration object
	 * @return size of memory segment
	 */
	public static int getPageSize(Configuration configuration) {
		final int pageSize = checkedDownCast(MemorySize.parse(
			configuration.getString(TaskManagerOptions.MEMORY_SEGMENT_SIZE)).getBytes());

		// check page size of for minimum size
		ConfigurationParserUtils.checkConfigParameter(pageSize >= MemoryManager.MIN_PAGE_SIZE, pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);
		// check page size for power of two
		ConfigurationParserUtils.checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Memory segment size must be a power of 2.");

		return pageSize;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + numNetworkBuffers;
		result = 31 * result + networkBufferSize;
		result = 31 * result + partitionRequestInitialBackoff;
		result = 31 * result + partitionRequestMaxBackoff;
		result = 31 * result + networkBuffersPerChannel;
		result = 31 * result + floatingNetworkBuffersPerGate;
		result = 31 * result + (isCreditBased ? 1 : 0);
		result = 31 * result + (nettyConfig != null ? nettyConfig.hashCode() : 0);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		else {
			final NetworkEnvironmentConfiguration that = (NetworkEnvironmentConfiguration) obj;

			return this.numNetworkBuffers == that.numNetworkBuffers &&
					this.networkBufferSize == that.networkBufferSize &&
					this.partitionRequestInitialBackoff == that.partitionRequestInitialBackoff &&
					this.partitionRequestMaxBackoff == that.partitionRequestMaxBackoff &&
					this.networkBuffersPerChannel == that.networkBuffersPerChannel &&
					this.floatingNetworkBuffersPerGate == that.floatingNetworkBuffersPerGate &&
					this.isCreditBased == that.isCreditBased &&
					(nettyConfig != null ? nettyConfig.equals(that.nettyConfig) : that.nettyConfig == null);
		}
	}

	@Override
	public String toString() {
		return "NetworkEnvironmentConfiguration{" +
				", numNetworkBuffers=" + numNetworkBuffers +
				", networkBufferSize=" + networkBufferSize +
				", partitionRequestInitialBackoff=" + partitionRequestInitialBackoff +
				", partitionRequestMaxBackoff=" + partitionRequestMaxBackoff +
				", networkBuffersPerChannel=" + networkBuffersPerChannel +
				", floatingNetworkBuffersPerGate=" + floatingNetworkBuffersPerGate +
				", isCreditBased=" + isCreditBased +
				", nettyConfig=" + nettyConfig +
				'}';
	}
}
