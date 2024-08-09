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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.NoOpMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.buildBufferCompressor;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getDiskTierName;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile.DATA_FILE_SUFFIX;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile.INDEX_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link TierFactory} for disk tier. */
public class DiskTierFactory implements TierFactory {

    private static final int DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT = 16 * 32 * 1024;

    private static final int DEFAULT_REGION_GROUP_SIZE_IN_BYTES = 1024;

    private static final int DEFAULT_MAX_CACHED_BYTES_BEFORE_FLUSH = 512 * 1024;

    private static final long DEFAULT_MAX_REGION_NUM_RETAINED_IN_MEMORY = 1024 * 1024L;

    private static final float DEFAULT_MIN_RESERVE_DISK_SPACE_FRACTION = 0.05f;

    private static final int DEFAULT_DISK_TIER_EXCLUSIVE_BUFFERS = 1;

    private static final Duration DEFAULT_DISK_TIER_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private int bufferSizeBytes = -1;

    @Nullable private Configuration conf;

    @Override
    public void setup(Configuration configuration) {
        this.bufferSizeBytes = ConfigurationParserUtils.getPageSize(configuration);
        this.conf = checkNotNull(configuration);
    }

    @Override
    public TieredStorageMemorySpec getMasterAgentMemorySpec() {
        return new TieredStorageMemorySpec(getDiskTierName(), 0);
    }

    @Override
    public TieredStorageMemorySpec getProducerAgentMemorySpec() {
        return new TieredStorageMemorySpec(getDiskTierName(), DEFAULT_DISK_TIER_EXCLUSIVE_BUFFERS);
    }

    @Override
    public TieredStorageMemorySpec getConsumerAgentMemorySpec() {
        return new TieredStorageMemorySpec(getDiskTierName(), 0);
    }

    @Override
    public TierMasterAgent createMasterAgent(TieredStorageResourceRegistry resourceRegistry) {
        return NoOpMasterAgent.INSTANCE;
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numPartitions,
            int numSubpartitions,
            TieredStoragePartitionId partitionId,
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

        ProducerMergedPartitionFileIndex partitionFileIndex =
                new ProducerMergedPartitionFileIndex(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        Paths.get(dataFileBasePath + INDEX_FILE_SUFFIX),
                        DEFAULT_REGION_GROUP_SIZE_IN_BYTES,
                        DEFAULT_MAX_REGION_NUM_RETAINED_IN_MEMORY);
        Path dataFilePath = Paths.get(dataFileBasePath + DATA_FILE_SUFFIX);
        ProducerMergedPartitionFileWriter partitionFileWriter =
                ProducerMergedPartitionFile.createPartitionFileWriter(
                        dataFilePath, partitionFileIndex);
        ProducerMergedPartitionFileReader partitionFileReader =
                ProducerMergedPartitionFile.createPartitionFileReader(
                        dataFilePath, partitionFileIndex);
        return new DiskTierProducerAgent(
                partitionId,
                numSubpartitions,
                DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT,
                bufferSizeBytes,
                DEFAULT_MAX_CACHED_BYTES_BEFORE_FLUSH,
                dataFilePath,
                DEFAULT_MIN_RESERVE_DISK_SPACE_FRACTION,
                isBroadcastOnly,
                partitionFileWriter,
                partitionFileReader,
                storageMemoryManager,
                nettyService,
                resourceRegistry,
                bufferPool,
                ioExecutor,
                maxRequestedBuffers,
                DEFAULT_DISK_TIER_BUFFER_REQUEST_TIMEOUT,
                buildBufferCompressor(bufferSizeBytes, checkNotNull(conf)));
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<TierShuffleDescriptor> shuffleDescriptors,
            TieredStorageNettyService nettyService) {
        return new DiskTierConsumerAgent(tieredStorageConsumerSpecs, nettyService);
    }
}
