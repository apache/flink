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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
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
import java.util.Objects;

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

	private final boolean isNetworkDetailedMetrics;

	private final NettyConfig nettyConfig;

	private final String[] tempDirs;

	private final BoundedBlockingSubpartitionType blockingSubpartitionType;

	private final boolean blockingShuffleCompressionEnabled;

	private final String compressionCodec;

	private final int maxBuffersPerChannel;

	public NettyShuffleEnvironmentConfiguration(
			int numNetworkBuffers,
			int networkBufferSize,
			int partitionRequestInitialBackoff,
			int partitionRequestMaxBackoff,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			Duration requestSegmentsTimeout,
			boolean isNetworkDetailedMetrics,
			@Nullable NettyConfig nettyConfig,
			String[] tempDirs,
			BoundedBlockingSubpartitionType blockingSubpartitionType,
			boolean blockingShuffleCompressionEnabled,
			String compressionCodec,
			int maxBuffersPerChannel) {

		this.numNetworkBuffers = numNetworkBuffers;
		this.networkBufferSize = networkBufferSize;
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.requestSegmentsTimeout = Preconditions.checkNotNull(requestSegmentsTimeout);
		this.isNetworkDetailedMetrics = isNetworkDetailedMetrics;
		this.nettyConfig = nettyConfig;
		this.tempDirs = Preconditions.checkNotNull(tempDirs);
		this.blockingSubpartitionType = Preconditions.checkNotNull(blockingSubpartitionType);
		this.blockingShuffleCompressionEnabled = blockingShuffleCompressionEnabled;
		this.compressionCodec = Preconditions.checkNotNull(compressionCodec);
		this.maxBuffersPerChannel = maxBuffersPerChannel;
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

	public boolean isNetworkDetailedMetrics() {
		return isNetworkDetailedMetrics;
	}

	public String[] getTempDirs() {
		return tempDirs;
	}

	public BoundedBlockingSubpartitionType getBlockingSubpartitionType() {
		return blockingSubpartitionType;
	}

	public boolean isBlockingShuffleCompressionEnabled() {
		return blockingShuffleCompressionEnabled;
	}

	public String getCompressionCodec() {
		return compressionCodec;
	}

	public int getMaxBuffersPerChannel() {
		return maxBuffersPerChannel;
	}

	// ------------------------------------------------------------------------

	/**
	 * Utility method to extract network related parameters from the configuration and to
	 * sanity check them.
	 *
	 * @param configuration configuration object
	 * @param networkMemorySize the size of memory reserved for shuffle environment
	 * @param localTaskManagerCommunication true, to skip initializing the network stack
	 * @param taskManagerAddress identifying the IP address under which the TaskManager will be accessible
	 * @return NettyShuffleEnvironmentConfiguration
	 */
	public static NettyShuffleEnvironmentConfiguration fromConfiguration(
		Configuration configuration,
		MemorySize networkMemorySize,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress) {

		final int dataBindPort = getDataBindPort(configuration);

		final int pageSize = ConfigurationParserUtils.getPageSize(configuration);

		final NettyConfig nettyConfig = createNettyConfig(configuration, localTaskManagerCommunication, taskManagerAddress, dataBindPort);

		final int numberOfNetworkBuffers = calculateNumberOfNetworkBuffers(
			configuration,
			networkMemorySize,
			pageSize);

		int initialRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
		int maxRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);

		int buffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
		int extraBuffersPerGate = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

		int maxBuffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_MAX_BUFFERS_PER_CHANNEL);

		boolean isNetworkDetailedMetrics = configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_DETAILED_METRICS);

		String[] tempDirs = ConfigurationUtils.parseTempDirectories(configuration);

		Duration requestSegmentsTimeout = Duration.ofMillis(configuration.getLong(
				NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));

		BoundedBlockingSubpartitionType blockingSubpartitionType = getBlockingSubpartitionType(configuration);

		boolean blockingShuffleCompressionEnabled =
			configuration.get(NettyShuffleEnvironmentOptions.BLOCKING_SHUFFLE_COMPRESSION_ENABLED);
		String compressionCodec = configuration.getString(NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC);

		return new NettyShuffleEnvironmentConfiguration(
			numberOfNetworkBuffers,
			pageSize,
			initialRequestBackoff,
			maxRequestBackoff,
			buffersPerChannel,
			extraBuffersPerGate,
			requestSegmentsTimeout,
			isNetworkDetailedMetrics,
			nettyConfig,
			tempDirs,
			blockingSubpartitionType,
			blockingShuffleCompressionEnabled,
			compressionCodec,
			maxBuffersPerChannel);
	}

	/**
	 * Parses the hosts / ports for communication and data exchange from configuration.
	 *
	 * @param configuration configuration object
	 * @return the data port
	 */
	private static int getDataBindPort(Configuration configuration) {
		final int dataBindPort;
		if (configuration.contains(NettyShuffleEnvironmentOptions.DATA_BIND_PORT)) {
			dataBindPort = configuration.getInteger(NettyShuffleEnvironmentOptions.DATA_BIND_PORT);
			ConfigurationParserUtils.checkConfigParameter(
				dataBindPort >= 0, dataBindPort, NettyShuffleEnvironmentOptions.DATA_BIND_PORT.key(),
				"Leave config parameter empty to fallback to '" +
					NettyShuffleEnvironmentOptions.DATA_PORT.key() + "' automatically.");
		} else {
			dataBindPort = configuration.getInteger(NettyShuffleEnvironmentOptions.DATA_PORT);
			ConfigurationParserUtils.checkConfigParameter(
				dataBindPort >= 0, dataBindPort, NettyShuffleEnvironmentOptions.DATA_PORT.key(),
				"Leave config parameter empty or use 0 to let the system choose a port automatically.");
		}
		return dataBindPort;
	}

	/**
	 * Calculates the number of network buffers based on configuration and jvm heap size.
	 *
	 * @param configuration configuration object
	 * @param networkMemorySize the size of memory reserved for shuffle environment
	 * @param pageSize size of memory segment
	 * @return the number of network buffers
	 */
	private static int calculateNumberOfNetworkBuffers(
		Configuration configuration,
		MemorySize networkMemorySize,
		int pageSize) {

		logIfIgnoringOldConfigs(configuration);

		// tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
		long numberOfNetworkBuffersLong = networkMemorySize.getBytes() / pageSize;
		if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("The given number of memory bytes (" + networkMemorySize.getBytes()
				+ ") corresponds to more than MAX_INT pages.");
		}

		return (int) numberOfNetworkBuffersLong;
	}

	@SuppressWarnings("deprecation")
	private static void logIfIgnoringOldConfigs(Configuration configuration) {
		if (configuration.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS)) {
			LOG.info("Ignoring old (but still present) network buffer configuration via {}.",
				NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key());
		}
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
		String transport = config.getString(NettyShuffleEnvironmentOptions.NETWORK_BLOCKING_SHUFFLE_TYPE);

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
		result = 31 * result + (nettyConfig != null ? nettyConfig.hashCode() : 0);
		result = 31 * result + Arrays.hashCode(tempDirs);
		result = 31 * result + (blockingShuffleCompressionEnabled ? 1 : 0);
		result = 31 * result + Objects.hashCode(compressionCodec);
		result = 31 * result + maxBuffersPerChannel;
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
					(nettyConfig != null ? nettyConfig.equals(that.nettyConfig) : that.nettyConfig == null) &&
					Arrays.equals(this.tempDirs, that.tempDirs) &&
					this.blockingShuffleCompressionEnabled == that.blockingShuffleCompressionEnabled &&
					this.maxBuffersPerChannel == that.maxBuffersPerChannel &&
					Objects.equals(this.compressionCodec, that.compressionCodec);
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
				", nettyConfig=" + nettyConfig +
				", tempDirs=" + Arrays.toString(tempDirs) +
				", blockingShuffleCompressionEnabled=" + blockingShuffleCompressionEnabled +
				", compressionCodec=" + compressionCodec +
				", maxBuffersPerChannel=" + maxBuffersPerChannel +
				'}';
	}
}
