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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The hash implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator} receives
 * the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The accumulated buffers will be transferred to the corresponding
 * tier dynamically.
 *
 * <p>To avoid the buffer waiting deadlock between the subpartitions, the {@link
 * HashBufferAccumulator} requires at least n buffers (n is the number of subpartitions) to make
 * sure that each subpartition has at least one buffer to accumulate the receiving data. Once an
 * accumulated buffer is finished, the buffer will be flushed immediately.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 */
public class HashBufferAccumulator
        implements BufferAccumulator, HashSubpartitionBufferAccumulatorContext {

    private final TieredStorageMemoryManager memoryManager;

    private final HashSubpartitionBufferAccumulator[] hashSubpartitionBufferAccumulators;

    /**
     * The {@link HashBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    @Nullable
    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher;

    public HashBufferAccumulator(
            int numSubpartitions, int bufferSize, TieredStorageMemoryManager memoryManager) {
        this.memoryManager = memoryManager;
        this.hashSubpartitionBufferAccumulators =
                new HashSubpartitionBufferAccumulator[numSubpartitions];
        for (int i = 0; i < numSubpartitions; i++) {
            hashSubpartitionBufferAccumulators[i] =
                    new HashSubpartitionBufferAccumulator(
                            new TieredStorageSubpartitionId(i), bufferSize, this);
        }
    }

    @Override
    public void setup(
            BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher) {
        this.accumulatedBufferFlusher = accumulatedBufferFlusher;
    }

    @Override
    public void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {
        getSubpartitionAccumulator(subpartitionId).append(record, dataType);
    }

    @Override
    public void close() {
        Arrays.stream(hashSubpartitionBufferAccumulators)
                .forEach(HashSubpartitionBufferAccumulator::close);
    }

    @Override
    public BufferBuilder requestBufferBlocking() {
        return memoryManager.requestBufferBlocking(this);
    }

    @Override
    public void flushAccumulatedBuffers(
            TieredStorageSubpartitionId subpartitionId, List<Buffer> accumulatedBuffers) {
        checkNotNull(accumulatedBufferFlusher).accept(subpartitionId, accumulatedBuffers);
    }

    private HashSubpartitionBufferAccumulator getSubpartitionAccumulator(
            TieredStorageSubpartitionId subpartitionId) {
        return hashSubpartitionBufferAccumulators[subpartitionId.getSubpartitionId()];
    }
}
