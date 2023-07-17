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

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getTieredStoragePath;

/** The implementation of {@link TierFactory} for remote tier. */
public class RemoteTierFactory implements TierFactory {

    private final int numBytesPerSegment;

    private final int bufferSizeBytes;

    private final String remoteStoragePath;

    public RemoteTierFactory(
            int numBytesPerSegment, int bufferSizeBytes, String remoteStorageBasePath) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.bufferSizeBytes = bufferSizeBytes;
        this.remoteStoragePath = getTieredStoragePath(remoteStorageBasePath);
    }

    @Override
    public TierMasterAgent createMasterAgent(TieredStorageResourceRegistry resourceRegistry) {
        return new RemoteTierMasterAgent(remoteStoragePath, resourceRegistry);
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numSubpartitions,
            TieredStoragePartitionId partitionID,
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
        PartitionFileWriter partitionFileWriter =
                SegmentPartitionFile.createPartitionFileWriter(remoteStoragePath, numSubpartitions);
        return new RemoteTierProducerAgent(
                partitionID,
                numSubpartitions,
                numBytesPerSegment,
                bufferSizeBytes,
                isBroadcastOnly,
                partitionFileWriter,
                storageMemoryManager,
                resourceRegistry);
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        PartitionFileReader partitionFileReader =
                SegmentPartitionFile.createPartitionFileReader(remoteStoragePath);
        RemoteStorageScanner remoteStorageScanner = new RemoteStorageScanner(remoteStoragePath);
        return new RemoteTierConsumerAgent(
                remoteStorageScanner, partitionFileReader, bufferSizeBytes);
    }
}
