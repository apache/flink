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
import org.apache.flink.runtime.io.network.partition.DeduplicatedQueue;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** The data client is used to fetch data from remote tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent, AvailabilityNotifier {

    private final List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs;

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

    /** The indexes of all the subpartitions with data available stored in FIFO order. */
    private final Map<TieredStoragePartitionId, DeduplicatedQueue<TieredStorageSubpartitionId>>
            availableSubpartitionsQueues = new HashMap<>();

    private final int bufferSizeBytes;

    private AvailabilityNotifier notifier;

    public RemoteTierConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            RemoteStorageScanner remoteStorageScanner,
            PartitionFileReader partitionFileReader,
            int bufferSizeBytes) {
        this.tieredStorageConsumerSpecs = tieredStorageConsumerSpecs;
        this.remoteStorageScanner = remoteStorageScanner;
        this.currentBufferIndexAndSegmentIds = new HashMap<>();
        this.partitionFileReader = partitionFileReader;
        this.bufferSizeBytes = bufferSizeBytes;
        this.remoteStorageScanner.registerAvailabilityAndPriorityNotifier(this);
        for (TieredStorageConsumerSpec spec : tieredStorageConsumerSpecs) {
            availableSubpartitionsQueues.putIfAbsent(
                    spec.getPartitionId(), new DeduplicatedQueue<>());
        }
    }

    @Override
    public void start() {
        remoteStorageScanner.start();
        for (TieredStorageConsumerSpec spec : tieredStorageConsumerSpecs) {
            for (int subpartitionId : spec.getSubpartitionIds().values()) {
                remoteStorageScanner.watchSegment(
                        spec.getPartitionId(), new TieredStorageSubpartitionId(subpartitionId), 0);
            }
        }
    }

    @Override
    public int peekNextBufferSubpartitionId(
            TieredStoragePartitionId partitionId, ResultSubpartitionIndexSet indexSet)
            throws IOException {
        synchronized (availableSubpartitionsQueues) {
            for (TieredStorageSubpartitionId subpartitionId :
                    availableSubpartitionsQueues.get(partitionId).values()) {
                if (indexSet.contains(subpartitionId.getSubpartitionId())) {
                    return subpartitionId.getSubpartitionId();
                }
            }
            return -1;
        }
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
                        .getOrDefault(subpartitionId, Tuple2.of(0, 0));
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
        synchronized (availableSubpartitionsQueues) {
            availableSubpartitionsQueues.get(partitionId).remove(subpartitionId);
        }
        return Optional.empty();
    }

    @Override
    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        Preconditions.checkState(this.notifier == null);
        this.notifier = notifier;
    }

    @Override
    public void close() throws IOException {
        remoteStorageScanner.close();
    }

    @Override
    public void notifyAvailable(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        synchronized (availableSubpartitionsQueues) {
            if (!availableSubpartitionsQueues.get(partitionId).add(subpartitionId)) {
                return;
            }
        }
        this.notifier.notifyAvailable(partitionId, subpartitionId);
    }

    @Override
    public void notifyAvailable(
            TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId) {
        throw new UnsupportedOperationException("This method should not be invoked.");
    }
}
