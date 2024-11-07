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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.PortRange;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE;
import static org.apache.flink.configuration.ExecutionOptions.BATCH_SHUFFLE_MODE;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Configuration object for the network stack. */
public class NettyShuffleEnvironmentConfiguration {
    private static final Logger LOG =
            LoggerFactory.getLogger(NettyShuffleEnvironmentConfiguration.class);

    private final int numNetworkBuffers;

    private final int networkBufferSize;

    private final int startingBufferSize;

    private final int partitionRequestInitialBackoff;

    private final int partitionRequestMaxBackoff;

    private final int partitionRequestListenerTimeout;

    /**
     * Number of network buffers to use for each outgoing/incoming channel (subpartition/input
     * channel).
     */
    private final int networkBuffersPerChannel;

    /**
     * Number of extra network buffers to use for each outgoing/incoming gate (result
     * partition/input gate).
     */
    private final int floatingNetworkBuffersPerGate;

    private final Optional<Integer> maxRequiredBuffersPerGate;

    private final int sortShuffleMinBuffers;

    private final int sortShuffleMinParallelism;

    /** Size of direct memory to be allocated for blocking shuffle data read. */
    private final long batchShuffleReadMemoryBytes;

    private final Duration requestSegmentsTimeout;

    private final boolean isNetworkDetailedMetrics;

    private final NettyConfig nettyConfig;

    private final String[] tempDirs;

    private final BoundedBlockingSubpartitionType blockingSubpartitionType;

    private final boolean batchShuffleCompressionEnabled;

    private final CompressionCodec compressionCodec;

    private final int maxBuffersPerChannel;

    private final BufferDebloatConfiguration debloatConfiguration;

    private final boolean connectionReuseEnabled;

    private final int maxOverdraftBuffersPerGate;

    private final TieredStorageConfiguration tieredStorageConfiguration;

    public NettyShuffleEnvironmentConfiguration(
            int numNetworkBuffers,
            int networkBufferSize,
            int startingBufferSize,
            int partitionRequestInitialBackoff,
            int partitionRequestMaxBackoff,
            int partitionRequestListenerTimeout,
            int networkBuffersPerChannel,
            int floatingNetworkBuffersPerGate,
            Optional<Integer> maxRequiredBuffersPerGate,
            Duration requestSegmentsTimeout,
            boolean isNetworkDetailedMetrics,
            @Nullable NettyConfig nettyConfig,
            String[] tempDirs,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            boolean batchShuffleCompressionEnabled,
            CompressionCodec compressionCodec,
            int maxBuffersPerChannel,
            long batchShuffleReadMemoryBytes,
            int sortShuffleMinBuffers,
            int sortShuffleMinParallelism,
            BufferDebloatConfiguration debloatConfiguration,
            boolean connectionReuseEnabled,
            int maxOverdraftBuffersPerGate,
            @Nullable TieredStorageConfiguration tieredStorageConfiguration) {

        this.numNetworkBuffers = numNetworkBuffers;
        this.networkBufferSize = networkBufferSize;
        this.startingBufferSize = startingBufferSize;
        this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
        this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
        this.partitionRequestListenerTimeout = partitionRequestListenerTimeout;
        this.networkBuffersPerChannel = networkBuffersPerChannel;
        this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
        this.maxRequiredBuffersPerGate = maxRequiredBuffersPerGate;
        this.requestSegmentsTimeout = Preconditions.checkNotNull(requestSegmentsTimeout);
        this.isNetworkDetailedMetrics = isNetworkDetailedMetrics;
        this.nettyConfig = nettyConfig;
        this.tempDirs = Preconditions.checkNotNull(tempDirs);
        this.blockingSubpartitionType = Preconditions.checkNotNull(blockingSubpartitionType);
        this.batchShuffleCompressionEnabled = batchShuffleCompressionEnabled;
        this.compressionCodec = Preconditions.checkNotNull(compressionCodec);
        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.batchShuffleReadMemoryBytes = batchShuffleReadMemoryBytes;
        this.sortShuffleMinBuffers = sortShuffleMinBuffers;
        this.sortShuffleMinParallelism = sortShuffleMinParallelism;
        this.debloatConfiguration = debloatConfiguration;
        this.connectionReuseEnabled = connectionReuseEnabled;
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;
        this.tieredStorageConfiguration = tieredStorageConfiguration;
    }

    // ------------------------------------------------------------------------

    public int numNetworkBuffers() {
        return numNetworkBuffers;
    }

    public int networkBufferSize() {
        return networkBufferSize;
    }

    public int startingBufferSize() {
        return startingBufferSize;
    }

    public int partitionRequestInitialBackoff() {
        return partitionRequestInitialBackoff;
    }

    public int partitionRequestMaxBackoff() {
        return partitionRequestMaxBackoff;
    }

    public int getPartitionRequestListenerTimeout() {
        return partitionRequestListenerTimeout;
    }

    public int networkBuffersPerChannel() {
        return networkBuffersPerChannel;
    }

    public int floatingNetworkBuffersPerGate() {
        return floatingNetworkBuffersPerGate;
    }

    public Optional<Integer> maxRequiredBuffersPerGate() {
        return maxRequiredBuffersPerGate;
    }

    public long batchShuffleReadMemoryBytes() {
        return batchShuffleReadMemoryBytes;
    }

    public int sortShuffleMinBuffers() {
        return sortShuffleMinBuffers;
    }

    public int sortShuffleMinParallelism() {
        return sortShuffleMinParallelism;
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

    public boolean isConnectionReuseEnabled() {
        return connectionReuseEnabled;
    }

    public BoundedBlockingSubpartitionType getBlockingSubpartitionType() {
        return blockingSubpartitionType;
    }

    public boolean isBatchShuffleCompressionEnabled() {
        return batchShuffleCompressionEnabled;
    }

    public BufferDebloatConfiguration getDebloatConfiguration() {
        return debloatConfiguration;
    }

    public boolean isSSLEnabled() {
        return nettyConfig != null && nettyConfig.getSSLEnabled();
    }

    public CompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public int getMaxBuffersPerChannel() {
        return maxBuffersPerChannel;
    }

    public int getMaxOverdraftBuffersPerGate() {
        return maxOverdraftBuffersPerGate;
    }

    public TieredStorageConfiguration getTieredStorageConfiguration() {
        return tieredStorageConfiguration;
    }

    // ------------------------------------------------------------------------

    /**
     * Utility method to extract network related parameters from the configuration and to sanity
     * check them.
     *
     * @param configuration configuration object
     * @param networkMemorySize the size of memory reserved for shuffle environment
     * @param localTaskManagerCommunication true, to skip initializing the network stack
     * @param taskManagerAddress identifying the IP address under which the TaskManager will be
     *     accessible
     * @return NettyShuffleEnvironmentConfiguration
     */
    public static NettyShuffleEnvironmentConfiguration fromConfiguration(
            Configuration configuration,
            MemorySize networkMemorySize,
            boolean localTaskManagerCommunication,
            InetAddress taskManagerAddress) {

        final PortRange dataBindPortRange = getDataBindPortRange(configuration);

        final int pageSize = ConfigurationParserUtils.getPageSize(configuration);

        int startingBufferSize = Integer.MAX_VALUE;
        if (configuration.get(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED)) {
            startingBufferSize =
                    (int)
                            configuration
                                    .get(TaskManagerOptions.STARTING_MEMORY_SEGMENT_SIZE)
                                    .getBytes();
        }

        final NettyConfig nettyConfig =
                createNettyConfig(
                        configuration,
                        localTaskManagerCommunication,
                        taskManagerAddress,
                        dataBindPortRange);

        final int numberOfNetworkBuffers =
                calculateNumberOfNetworkBuffers(networkMemorySize, pageSize);

        int initialRequestBackoff =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
        int maxRequestBackoff =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);
        int listenerTimeout =
                (int)
                        configuration
                                .get(
                                        NettyShuffleEnvironmentOptions
                                                .NETWORK_PARTITION_REQUEST_TIMEOUT)
                                .toMillis();

        int buffersPerChannel = 2;
        int extraBuffersPerGate = 8;

        Optional<Integer> maxRequiredBuffersPerGate =
                configuration.getOptional(
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE);

        int maxBuffersPerChannel = 10;

        long batchShuffleReadMemoryBytes =
                configuration.get(TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY).getBytes();

        int sortShuffleMinBuffers =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        int sortShuffleMinParallelism = 1;

        boolean isNetworkDetailedMetrics =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_DETAILED_METRICS);

        String[] tempDirs = ConfigurationUtils.parseTempDirectories(configuration);
        // Shuffle the data directories to make it fairer for directory selection between different
        // TaskManagers, which is good for load balance especially when there are multiple disks.
        List<String> shuffleDirs = Arrays.asList(tempDirs);
        Collections.shuffle(shuffleDirs);

        Duration requestSegmentsTimeout =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_REQUEST_TIMEOUT);

        BoundedBlockingSubpartitionType blockingSubpartitionType =
                BoundedBlockingSubpartitionType.FILE;

        CompressionCodec compressionCodec =
                configuration.get(NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC);

        boolean batchShuffleCompressionEnabled = compressionCodec != CompressionCodec.NONE;

        boolean connectionReuseEnabled =
                configuration.get(
                        NettyShuffleEnvironmentOptions.TCP_CONNECTION_REUSE_ACROSS_JOBS_ENABLED);

        checkArgument(
                !maxRequiredBuffersPerGate.isPresent() || maxRequiredBuffersPerGate.get() >= 1,
                String.format(
                        "At least one buffer is required for each gate, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE
                                .key()));

        TieredStorageConfiguration tieredStorageConfiguration = null;
        if ((configuration.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_FULL
                || configuration.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_SELECTIVE)) {
            tieredStorageConfiguration =
                    TieredStorageConfiguration.fromConfiguration(configuration);
        }
        return new NettyShuffleEnvironmentConfiguration(
                numberOfNetworkBuffers,
                pageSize,
                startingBufferSize,
                initialRequestBackoff,
                maxRequestBackoff,
                listenerTimeout,
                buffersPerChannel,
                extraBuffersPerGate,
                maxRequiredBuffersPerGate,
                requestSegmentsTimeout,
                isNetworkDetailedMetrics,
                nettyConfig,
                shuffleDirs.toArray(tempDirs),
                blockingSubpartitionType,
                batchShuffleCompressionEnabled,
                compressionCodec,
                maxBuffersPerChannel,
                batchShuffleReadMemoryBytes,
                sortShuffleMinBuffers,
                sortShuffleMinParallelism,
                BufferDebloatConfiguration.fromConfiguration(configuration),
                connectionReuseEnabled,
                20,
                tieredStorageConfiguration);
    }

    /**
     * Parses the hosts / ports for communication and data exchange from configuration.
     *
     * @param configuration configuration object
     * @return the data port
     */
    private static PortRange getDataBindPortRange(Configuration configuration) {
        if (configuration.contains(NettyShuffleEnvironmentOptions.DATA_BIND_PORT)) {
            String dataBindPort = configuration.get(NettyShuffleEnvironmentOptions.DATA_BIND_PORT);

            return new PortRange(dataBindPort);
        }

        int dataBindPort = configuration.get(NettyShuffleEnvironmentOptions.DATA_PORT);
        ConfigurationParserUtils.checkConfigParameter(
                dataBindPort >= 0,
                dataBindPort,
                NettyShuffleEnvironmentOptions.DATA_PORT.key(),
                "Leave config parameter empty or use 0 to let the system choose a port automatically.");

        return new PortRange(dataBindPort);
    }

    /**
     * Calculates the number of network buffers based on configuration and jvm heap size.
     *
     * @param networkMemorySize the size of memory reserved for shuffle environment
     * @param pageSize size of memory segment
     * @return the number of network buffers
     */
    private static int calculateNumberOfNetworkBuffers(MemorySize networkMemorySize, int pageSize) {

        // tolerate offcuts between intended and allocated memory due to segmentation (will be
        // available to the user-space memory)
        long numberOfNetworkBuffersLong = networkMemorySize.getBytes() / pageSize;
        if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The given number of memory bytes ("
                            + networkMemorySize.getBytes()
                            + ") corresponds to more than MAX_INT pages.");
        }

        return (int) numberOfNetworkBuffersLong;
    }

    /**
     * Generates {@link NettyConfig} from Flink {@link Configuration}.
     *
     * @param configuration configuration object
     * @param localTaskManagerCommunication true, to skip initializing the network stack
     * @param taskManagerAddress identifying the IP address under which the TaskManager will be
     *     accessible
     * @param dataPortRange data port range for communication and data exchange
     * @return the netty configuration or {@code null} if communication is in the same task manager
     */
    @Nullable
    private static NettyConfig createNettyConfig(
            Configuration configuration,
            boolean localTaskManagerCommunication,
            InetAddress taskManagerAddress,
            PortRange dataPortRange) {

        final NettyConfig nettyConfig;
        if (!localTaskManagerCommunication) {
            final InetSocketAddress taskManagerInetSocketAddress =
                    new InetSocketAddress(taskManagerAddress, 0);

            nettyConfig =
                    new NettyConfig(
                            taskManagerInetSocketAddress.getAddress(),
                            dataPortRange,
                            ConfigurationParserUtils.getPageSize(configuration),
                            ConfigurationParserUtils.getSlot(configuration),
                            configuration);
        } else {
            nettyConfig = null;
        }

        return nettyConfig;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + numNetworkBuffers;
        result = 31 * result + networkBufferSize;
        result = 31 * result + partitionRequestInitialBackoff;
        result = 31 * result + partitionRequestMaxBackoff;
        result = 31 * result + partitionRequestListenerTimeout;
        result = 31 * result + networkBuffersPerChannel;
        result = 31 * result + floatingNetworkBuffersPerGate;
        result = 31 * result + requestSegmentsTimeout.hashCode();
        result = 31 * result + (nettyConfig != null ? nettyConfig.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(tempDirs);
        result = 31 * result + (batchShuffleCompressionEnabled ? 1 : 0);
        result = 31 * result + Objects.hashCode(compressionCodec);
        result = 31 * result + maxBuffersPerChannel;
        result = 31 * result + Objects.hashCode(batchShuffleReadMemoryBytes);
        result = 31 * result + sortShuffleMinBuffers;
        result = 31 * result + sortShuffleMinParallelism;
        result = 31 * result + (connectionReuseEnabled ? 1 : 0);
        result = 31 * result + maxOverdraftBuffersPerGate;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            final NettyShuffleEnvironmentConfiguration that =
                    (NettyShuffleEnvironmentConfiguration) obj;

            return this.numNetworkBuffers == that.numNetworkBuffers
                    && this.networkBufferSize == that.networkBufferSize
                    && this.partitionRequestInitialBackoff == that.partitionRequestInitialBackoff
                    && this.partitionRequestMaxBackoff == that.partitionRequestMaxBackoff
                    && this.networkBuffersPerChannel == that.networkBuffersPerChannel
                    && this.floatingNetworkBuffersPerGate == that.floatingNetworkBuffersPerGate
                    && this.batchShuffleReadMemoryBytes == that.batchShuffleReadMemoryBytes
                    && this.sortShuffleMinBuffers == that.sortShuffleMinBuffers
                    && this.sortShuffleMinParallelism == that.sortShuffleMinParallelism
                    && this.requestSegmentsTimeout.equals(that.requestSegmentsTimeout)
                    && (nettyConfig != null
                            ? nettyConfig.equals(that.nettyConfig)
                            : that.nettyConfig == null)
                    && Arrays.equals(this.tempDirs, that.tempDirs)
                    && this.batchShuffleCompressionEnabled == that.batchShuffleCompressionEnabled
                    && this.maxBuffersPerChannel == that.maxBuffersPerChannel
                    && this.partitionRequestListenerTimeout == that.partitionRequestListenerTimeout
                    && Objects.equals(this.compressionCodec, that.compressionCodec)
                    && this.connectionReuseEnabled == that.connectionReuseEnabled
                    && this.maxOverdraftBuffersPerGate == that.maxOverdraftBuffersPerGate;
        }
    }

    @Override
    public String toString() {
        return "NettyShuffleEnvironmentConfiguration{"
                + ", numNetworkBuffers="
                + numNetworkBuffers
                + ", networkBufferSize="
                + networkBufferSize
                + ", partitionRequestInitialBackoff="
                + partitionRequestInitialBackoff
                + ", partitionRequestMaxBackoff="
                + partitionRequestMaxBackoff
                + ", networkBuffersPerChannel="
                + networkBuffersPerChannel
                + ", floatingNetworkBuffersPerGate="
                + floatingNetworkBuffersPerGate
                + ", requestSegmentsTimeout="
                + requestSegmentsTimeout
                + ", nettyConfig="
                + nettyConfig
                + ", tempDirs="
                + Arrays.toString(tempDirs)
                + ", blockingShuffleCompressionEnabled="
                + batchShuffleCompressionEnabled
                + ", compressionCodec="
                + compressionCodec
                + ", maxBuffersPerChannel="
                + maxBuffersPerChannel
                + ", partitionRequestListenerTimeout"
                + partitionRequestListenerTimeout
                + ", batchShuffleReadMemoryBytes="
                + batchShuffleReadMemoryBytes
                + ", sortShuffleMinBuffers="
                + sortShuffleMinBuffers
                + ", sortShuffleMinParallelism="
                + sortShuffleMinParallelism
                + ", connectionReuseEnabled="
                + connectionReuseEnabled
                + ", maxOverdraftBuffersPerGate="
                + maxOverdraftBuffersPerGate
                + '}';
    }
}
