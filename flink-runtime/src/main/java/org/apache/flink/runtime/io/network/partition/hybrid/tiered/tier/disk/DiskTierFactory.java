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

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.NoOpMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile.DATA_FILE_SUFFIX;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile.INDEX_FILE_SUFFIX;

/** The implementation of {@link TierFactory} for disk tier. */
public class DiskTierFactory implements TierFactory {

    private final int numBytesPerSegment;

    private final int bufferSizeBytes;

    private final float minReservedDiskSpaceFraction;

    private final int regionGroupSizeInBytes;

    private final int maxCachedBytesBeforeFlush;

    private final long numRetainedInMemoryRegionsMax;

    public DiskTierFactory(
            int numBytesPerSegment,
            int bufferSizeBytes,
            float minReservedDiskSpaceFraction,
            int regionGroupSizeInBytes,
            int maxCachedBytesBeforeFlush,
            long numRetainedInMemoryRegionsMax) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.bufferSizeBytes = bufferSizeBytes;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.regionGroupSizeInBytes = regionGroupSizeInBytes;
        this.maxCachedBytesBeforeFlush = maxCachedBytesBeforeFlush;
        this.numRetainedInMemoryRegionsMax = numRetainedInMemoryRegionsMax;
    }

    @Override
    public TierMasterAgent createMasterAgent(TieredStorageResourceRegistry resourceRegistry) {
        return NoOpMasterAgent.INSTANCE;
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numSubpartitions,
            TieredStoragePartitionId partitionId,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead) {
        ProducerMergedPartitionFileIndex partitionFileIndex =
                new ProducerMergedPartitionFileIndex(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        Paths.get(dataFileBasePath + INDEX_FILE_SUFFIX),
                        regionGroupSizeInBytes,
                        numRetainedInMemoryRegionsMax);
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
                numBytesPerSegment,
                bufferSizeBytes,
                maxCachedBytesBeforeFlush,
                dataFilePath,
                minReservedDiskSpaceFraction,
                isBroadcastOnly,
                partitionFileWriter,
                partitionFileReader,
                storageMemoryManager,
                nettyService,
                resourceRegistry,
                bufferPool,
                ioExecutor,
                maxRequestedBuffers,
                bufferRequestTimeout,
                maxBufferReadAhead);
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        return new DiskTierConsumerAgent(tieredStorageConsumerSpecs, nettyService);
    }
}
