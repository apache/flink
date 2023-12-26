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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** The data client is used to fetch data from remote tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final RemoteStorageScanner remoteStorageScanner;

    private final PartitionFileReader partitionFileReader;

    /**
     * The current reading buffer indexes and segment ids stored in map.
     *
     * <p>The key is partition id and subpartition id. The value is buffer index and segment id.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<Integer, Integer>>>
            currentBufferIndexAndSegmentIds;

    private final int bufferSizeBytes;

    public RemoteTierConsumerAgent(
            RemoteStorageScanner remoteStorageScanner,
            PartitionFileReader partitionFileReader,
            int bufferSizeBytes) {
        this.remoteStorageScanner = remoteStorageScanner;
        this.currentBufferIndexAndSegmentIds = new HashMap<>();
        this.partitionFileReader = partitionFileReader;
        this.bufferSizeBytes = bufferSizeBytes;
    }

    @Override
    public void start() {
        remoteStorageScanner.start();
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        // Get current segment id and buffer index.
        Tuple2<Integer, Integer> bufferIndexAndSegmentId =
                currentBufferIndexAndSegmentIds
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, Tuple2.of(0, -1));
        int currentBufferIndex = bufferIndexAndSegmentId.f0;
        int currentSegmentId = bufferIndexAndSegmentId.f1;
        if (segmentId != currentSegmentId) {
            remoteStorageScanner.watchSegment(partitionId, subpartitionId, segmentId);
        }

        // Read buffer from the partition file in remote storage.
        MemorySegment memorySegment = MemorySegmentFactory.allocateUnpooledSegment(bufferSizeBytes);
        PartitionFileReader.ReadBufferResult readBufferResult = null;
        try {
            readBufferResult =
                    partitionFileReader.readBuffer(
                            partitionId,
                            subpartitionId,
                            segmentId,
                            currentBufferIndex,
                            memorySegment,
                            FreeingBufferRecycler.INSTANCE,
                            null,
                            null);
        } catch (IOException e) {
            memorySegment.free();
            ExceptionUtils.rethrow(e, "Failed to read buffer from partition file.");
        }
        if (readBufferResult != null && !readBufferResult.getReadBuffers().isEmpty()) {
            List<Buffer> readBuffers = readBufferResult.getReadBuffers();
            checkState(readBuffers.size() == 1);
            Buffer buffer = readBuffers.get(0);
            currentBufferIndexAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(++currentBufferIndex, segmentId));
            return Optional.of(buffer);
        } else {
            memorySegment.free();
        }
        return Optional.empty();
    }

    @Override
    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        remoteStorageScanner.registerAvailabilityAndPriorityNotifier(notifier);
    }

    @Override
    public void close() throws IOException {
        remoteStorageScanner.close();
    }
}
