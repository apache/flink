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
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;

/**
 * Configuration object for the network stack.
 */
public class NettyShuffleEnvironmentConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleEnvironmentConfiguration.class);

	private final int numNetworkBuffers;

	private final int networkBufferSize;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	/** Number of network buffers to use for each outgoing/incoming channel (subpartition/input channel). */
	private final int networkBuffersPerChannel;

	/** Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate). */
	private final int floatingNetworkBuffersPerGate;

	private final Duration requestSegmentsTimeout;

	private final boolean isCreditBased;

	private final boolean isNetworkDetailedMetrics;

	private final NettyConfig nettyConfig;

	private final String[] tempDirs;

	private final BoundedBlockingSubpartitionType blockingSubpartitionType;

	private final boolean forcePartitionReleaseOnConsumption;

	public NettyShuffleEnvironmentConfiguration(
			int numNetworkBuffers,
			int networkBufferSize,
			int partitionRequestInitialBackoff,
			int partitionRequestMaxBackoff,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			Duration requestSegmentsTimeout,
			boolean isCreditBased,
			boolean isNetworkDetailedMetrics,
			@Nullable NettyConfig nettyConfig,
			String[] tempDirs,
			BoundedBlockingSubpartitionType blockingSubpartitionType,
			boolean forcePartitionReleaseOnConsumption) {

		this.numNetworkBuffers = numNetworkBuffers;
		this.networkBufferSize = networkBufferSize;
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.requestSegmentsTimeout = Preconditions.checkNotNull(requestSegmentsTimeout);
		this.isCreditBased = isCreditBased;
		this.isNetworkDetailedMetrics = isNetworkDetailedMetrics;
		this.nettyConfig = nettyConfig;
		this.tempDirs = Preconditions.checkNotNull(tempDirs);
		this.blockingSubpartitionType = Preconditions.checkNotNull(blockingSubpartitionType);
		this.forcePartitionReleaseOnConsumption = forcePartitionReleaseOnConsumption;
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

	public Duration getRequestSegmentsTimeout() {
		return requestSegmentsTimeout;
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

	public String[] getTempDirs() {
		return tempDirs;
	}

	public BoundedBlockingSubpartitionType getBlockingSubpartitionType() {
		return blockingSubpartitionType;
	}

	public boolean isForcePartitionReleaseOnConsumption() {
		return forcePartitionReleaseOnConsumption;
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
	 * @return NettyShuffleEnvironmentConfiguration
	 */
	public static NettyShuffleEnvironmentConfiguration fromConfiguration(
		Configuration configuration,
		long maxJvmHeapMemory,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress) {

		final int dataport = getDataport(configuration);

		final int pageSize = ConfigurationParserUtils.getPageSize(configuration);

		final int numberOfNetworkBuffers = calculateNumberOfNetworkBuffers(configuration, maxJvmHeapMemory);

		final NettyConfig nettyConfig = createNettyConfig(configuration, localTaskManagerCommunication, taskManagerAddress, dataport);

		int initialRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
		int maxRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);

		int buffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
		int extraBuffersPerGate = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

		boolean isCreditBased = nettyConfig != null && configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL);

		boolean isNetworkDetailedMetrics = configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_DETAILED_METRICS);

		String[] tempDirs = ConfigurationUtils.parseTempDirectories(configuration);

		Duration requestSegmentsTimeout = Duration.ofMillis(configuration.getLong(
				NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));

		BoundedBlockingSubpartitionType blockingSubpartitionType = getBlockingSubpartitionType(configuration);

		boolean forcePartitionReleaseOnConsumption =
			configuration.getBoolean(NettyShuffleEnvironmentOptions.FORCE_PARTITION_RELEASE_ON_CONSUMPTION);

		return new NettyShuffleEnvironmentConfiguration(
			numberOfNetworkBuffers,
			pageSize,
			initialRequestBackoff,
			maxRequestBackoff,
			buffersPerChannel,
			extraBuffersPerGate,
			requestSegmentsTimeout,
			isCreditBased,
			isNetworkDetailedMetrics,
			nettyConfig,
			tempDirs,
			blockingSubpartitionType,
			forcePartitionReleaseOnConsumption);
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
	 *  <li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MAX}, and</li>
	 *  <li>{@link NettyShuffleEnvironmentOptions#NETWORK_NUM_BUFFERS} (fallback if the ones above do not exist)</li>
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
		final long heapAndManagedMemory;
		final MemoryType memoryType = ConfigurationParserUtils.getMemoryType(config);
		if (memoryType == MemoryType.HEAP) {
			heapAndManagedMemory = maxJvmHeapMemory;
		} else if (memoryType == MemoryType.OFF_HEAP) {
			long configuredMemory = ConfigurationParserUtils.getManagedMemorySize(config) << 20; // megabytes to bytes
			if (configuredMemory > 0) {
				// The maximum heap memory has been adjusted according to configuredMemory, i.e.
				// maxJvmHeap = jvmHeapNoNet - configuredMemory
				heapAndManagedMemory = maxJvmHeapMemory + configuredMemory;
			} else {
				// The maximum heap memory has been adjusted according to the fraction, i.e.
				// maxJvmHeap = jvmHeapNoNet - jvmHeapNoNet * managedFraction = jvmHeapNoNet * (1 - managedFraction)
				heapAndManagedMemory = (long) (maxJvmHeapMemory / (1.0 - ConfigurationParserUtils.getManagedMemoryFraction(config)));
			}
		} else {
			throw new RuntimeException("No supported memory type detected.");
		}

		// finally extract the network buffer memory size again from:
		// heapAndManagedMemory = totalProcessMemory - networkReservedMemory
		//                      = totalProcessMemory - Math.min(networkBufMax, Math.max(networkBufMin, totalProcessMemory * networkBufFraction)
		// totalProcessMemory = heapAndManagedMemory / (1.0 - networkBufFraction)
		float networkBufFraction = config.getFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION);

		// Converts to double for higher precision. Converting via string achieve higher precision for those
		// numbers can not be represented preciously by float, like 0.4f.
		double heapAndManagedFraction = 1.0 - Double.valueOf(Float.toString(networkBufFraction));
		long totalProcessMemory = (long) (heapAndManagedMemory / heapAndManagedFraction);
		long networkMemoryByFraction = (long) (totalProcessMemory * networkBufFraction);

		// Do not need to check the maximum allowed memory since the computed total memory should always
		// be larger than the computed network buffer memory as long as the fraction is less than 1.
		return calculateNewNetworkBufferMemory(config, networkMemoryByFraction, Long.MAX_VALUE);
	}

	/**
	 * Calculates the amount of memory used for network buffers based on the total memory to use and
	 * the according configuration parameters.
	 *
	 * <p>The following configuration parameters are involved:
	 * <ul>
	 *  <li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MAX}, and</li>
	 *  <li>{@link NettyShuffleEnvironmentOptions#NETWORK_NUM_BUFFERS} (fallback if the ones above do not exist)</li>
	 * </ul>.
	 *
	 * @param totalProcessMemory overall available memory to use (in bytes)
	 * @param config configuration object
	 *
	 * @return memory to use for network buffers (in bytes)
	 */
	@SuppressWarnings("deprecation")
	public static long calculateNetworkBufferMemory(long totalProcessMemory, Configuration config) {
		final int segmentSize = ConfigurationParserUtils.getPageSize(config);

		final long networkReservedMemory;
		if (hasNewNetworkConfig(config)) {
			float networkBufFraction = config.getFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
			long networkMemoryByFraction = (long) (totalProcessMemory * networkBufFraction);
			networkReservedMemory = calculateNewNetworkBufferMemory(config, networkMemoryByFraction, totalProcessMemory);
		} else {
			// use old (deprecated) network buffers parameter
			int numNetworkBuffers = config.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
			networkReservedMemory = (long) numNetworkBuffers * (long) segmentSize;

			checkOldNetworkConfig(numNetworkBuffers);

			ConfigurationParserUtils.checkConfigParameter(networkReservedMemory < totalProcessMemory,
				networkReservedMemory, NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key(),
				"Network buffer memory size too large: " + networkReservedMemory + " >= " +
					totalProcessMemory + " (total JVM memory size)");
		}

		return networkReservedMemory;
	}

	/**
	 * Calculates the amount of memory used for network buffers based on the total memory to use and
	 * the according configuration parameters.
	 *
	 * <p>The following configuration parameters are involved:
	 * <ul>
	 *  <li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MAX}</li>
	 * </ul>.
	 *
	 * @param config configuration object
	 * @param networkMemoryByFraction memory of network buffers based on JVM memory size and network fraction
	 * @param maxAllowedMemory maximum memory used for checking the results of network memory
	 *
	 * @return memory to use for network buffers (in bytes)
	 */
	private static long calculateNewNetworkBufferMemory(Configuration config, long networkMemoryByFraction, long maxAllowedMemory) {
		float networkBufFraction = config.getFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
		long networkBufMin = MemorySize.parse(config.getString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN)).getBytes();
		long networkBufMax = MemorySize.parse(config.getString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX)).getBytes();

		int pageSize = ConfigurationParserUtils.getPageSize(config);

		checkNewNetworkConfig(pageSize, networkBufFraction, networkBufMin, networkBufMax);

		long networkBufBytes = Math.min(networkBufMax, Math.max(networkBufMin, networkMemoryByFraction));

		ConfigurationParserUtils.checkConfigParameter(networkBufBytes < maxAllowedMemory,
			"(" + networkBufFraction + ", " + networkBufMin + ", " + networkBufMax + ")",
			"(" + NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key() + ", " +
				NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key() + ", " +
				NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ")",
			"Network buffer memory size too large: " + networkBufBytes + " >= " +
				maxAllowedMemory + " (maximum JVM memory size)");

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
			NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key(),
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
			NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(),
			"Network buffer memory fraction of the free memory must be between 0.0 and 1.0");

		ConfigurationParserUtils.checkConfigParameter(networkBufMin >= pageSize, networkBufMin,
			NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key(),
			"Minimum memory for network buffers must allow at least one network " +
				"buffer with respect to the memory segment size");

		ConfigurationParserUtils.checkConfigParameter(networkBufMax >= pageSize, networkBufMax,
			NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key(),
			"Maximum memory for network buffers must allow at least one network " +
				"buffer with respect to the memory segment size");

		ConfigurationParserUtils.checkConfigParameter(networkBufMax >= networkBufMin, networkBufMax,
			NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key(),
			"Maximum memory for network buffers must not be smaller than minimum memory (" +
				NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ": " + networkBufMin + ")");
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
		return config.contains(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION) ||
			config.contains(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN) ||
			config.contains(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX) ||
			!config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
	}

	/**
	 * Parses the hosts / ports for communication and data exchange from configuration.
	 *
	 * @param configuration configuration object
	 * @return the data port
	 */
	private static int getDataport(Configuration configuration) {
		final int dataport = configuration.getInteger(NettyShuffleEnvironmentOptions.DATA_PORT);
		ConfigurationParserUtils.checkConfigParameter(dataport >= 0, dataport, NettyShuffleEnvironmentOptions.DATA_PORT.key(),
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
			numberOfNetworkBuffers = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);

			checkOldNetworkConfig(numberOfNetworkBuffers);
		} else {
			if (configuration.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS)) {
				LOG.info("Ignoring old (but still present) network buffer configuration via {}.",
					NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key());
			}

			final long networkMemorySize = calculateNewNetworkBufferMemory(configuration, maxJvmHeapMemory);

			// tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
			long numberOfNetworkBuffersLong = networkMemorySize / ConfigurationParserUtils.getPageSize(configuration);
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

			nettyConfig = new NettyConfig(
				taskManagerInetSocketAddress.getAddress(),
				taskManagerInetSocketAddress.getPort(),
				ConfigurationParserUtils.getPageSize(configuration),
				ConfigurationParserUtils.getSlot(configuration),
				configuration);
		} else {
			nettyConfig = null;
		}

		return nettyConfig;
	}

	private static BoundedBlockingSubpartitionType getBlockingSubpartitionType(Configuration config) {
		String transport = config.getString(NettyShuffleEnvironmentOptions.NETWORK_BOUNDED_BLOCKING_SUBPARTITION_TYPE);

		switch (transport) {
			case "mmap":
				return BoundedBlockingSubpartitionType.FILE_MMAP;
			case "file":
				return BoundedBlockingSubpartitionType.FILE;
			default:
				return BoundedBlockingSubpartitionType.AUTO;
		}
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
		result = 31 * result + requestSegmentsTimeout.hashCode();
		result = 31 * result + (isCreditBased ? 1 : 0);
		result = 31 * result + (nettyConfig != null ? nettyConfig.hashCode() : 0);
		result = 31 * result + Arrays.hashCode(tempDirs);
		result = 31 * result + (forcePartitionReleaseOnConsumption ? 1 : 0);
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
			final NettyShuffleEnvironmentConfiguration that = (NettyShuffleEnvironmentConfiguration) obj;

			return this.numNetworkBuffers == that.numNetworkBuffers &&
					this.networkBufferSize == that.networkBufferSize &&
					this.partitionRequestInitialBackoff == that.partitionRequestInitialBackoff &&
					this.partitionRequestMaxBackoff == that.partitionRequestMaxBackoff &&
					this.networkBuffersPerChannel == that.networkBuffersPerChannel &&
					this.floatingNetworkBuffersPerGate == that.floatingNetworkBuffersPerGate &&
					this.requestSegmentsTimeout.equals(that.requestSegmentsTimeout) &&
					this.isCreditBased == that.isCreditBased &&
					(nettyConfig != null ? nettyConfig.equals(that.nettyConfig) : that.nettyConfig == null) &&
					Arrays.equals(this.tempDirs, that.tempDirs) &&
					this.forcePartitionReleaseOnConsumption == that.forcePartitionReleaseOnConsumption;
		}
	}

	@Override
	public String toString() {
		return "NettyShuffleEnvironmentConfiguration{" +
				", numNetworkBuffers=" + numNetworkBuffers +
				", networkBufferSize=" + networkBufferSize +
				", partitionRequestInitialBackoff=" + partitionRequestInitialBackoff +
				", partitionRequestMaxBackoff=" + partitionRequestMaxBackoff +
				", networkBuffersPerChannel=" + networkBuffersPerChannel +
				", floatingNetworkBuffersPerGate=" + floatingNetworkBuffersPerGate +
				", requestSegmentsTimeout=" + requestSegmentsTimeout +
				", isCreditBased=" + isCreditBased +
				", nettyConfig=" + nettyConfig +
				", tempDirs=" + Arrays.toString(tempDirs) +
				", forcePartitionReleaseOnConsumption=" + forcePartitionReleaseOnConsumption +
				'}';
	}
}
