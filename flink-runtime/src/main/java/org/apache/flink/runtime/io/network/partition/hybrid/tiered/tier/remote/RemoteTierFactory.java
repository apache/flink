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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.buildBufferCompressor;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getRemoteTierName;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link TierFactory} for remote tier. */
public class RemoteTierFactory implements TierFactory {

    private static final String DEFAULT_REMOTE_STORAGE_BASE_PATH = null;

    private static final int DEFAULT_REMOTE_TIER_EXCLUSIVE_BUFFERS = 1;

    private static final int DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT = 16 * 32 * 1024;

    private int bufferSizeBytes = -1;

    private String remoteStoragePath = DEFAULT_REMOTE_STORAGE_BASE_PATH;

    @Nullable private Configuration conf;

    @Override
    public void setup(Configuration configuration) {
        this.bufferSizeBytes = ConfigurationParserUtils.getPageSize(configuration);
        this.conf = checkNotNull(configuration);
        this.remoteStoragePath =
                checkNotNull(
                        configuration.get(
                                NettyShuffleEnvironmentOptions
                                        .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH));
    }

    @Override
    public TieredStorageMemorySpec getMasterAgentMemorySpec() {
        return new TieredStorageMemorySpec(getRemoteTierName(), 0);
    }

    @Override
    public TieredStorageMemorySpec getProducerAgentMemorySpec() {
        return new TieredStorageMemorySpec(
                getRemoteTierName(), DEFAULT_REMOTE_TIER_EXCLUSIVE_BUFFERS);
    }

    @Override
    public TieredStorageMemorySpec getConsumerAgentMemorySpec() {
        return new TieredStorageMemorySpec(getRemoteTierName(), 0);
    }

    @Override
    public TierMasterAgent createMasterAgent(TieredStorageResourceRegistry resourceRegistry) {
        return new RemoteTierMasterAgent(remoteStoragePath, resourceRegistry);
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numPartitions,
            int numSubpartitions,
            TieredStoragePartitionId partitionID,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            List<TierShuffleDescriptor> shuffleDescriptors,
            int maxRequestedBuffers) {
        checkState(bufferSizeBytes > 0);
        checkNotNull(remoteStoragePath);

        PartitionFileWriter partitionFileWriter =
                SegmentPartitionFile.createPartitionFileWriter(remoteStoragePath, numSubpartitions);
        return new RemoteTierProducerAgent(
                partitionID,
                numSubpartitions,
                DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT,
                bufferSizeBytes,
                isBroadcastOnly,
                partitionFileWriter,
                storageMemoryManager,
                resourceRegistry,
                buildBufferCompressor(bufferSizeBytes, checkNotNull(conf)));
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<TierShuffleDescriptor> shuffleDescriptors,
            TieredStorageNettyService nettyService) {
        PartitionFileReader partitionFileReader =
                SegmentPartitionFile.createPartitionFileReader(remoteStoragePath);
        RemoteStorageScanner remoteStorageScanner = new RemoteStorageScanner(remoteStoragePath);
        return new RemoteTierConsumerAgent(
                tieredStorageConsumerSpecs,
                remoteStorageScanner,
                partitionFileReader,
                bufferSizeBytes);
    }
}
