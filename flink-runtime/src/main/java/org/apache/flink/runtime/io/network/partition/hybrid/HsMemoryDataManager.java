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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategy.Decision;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** This class is responsible for managing data in memory. */
public class HsMemoryDataManager implements HsSpillingInfoProvider, HsMemoryDataManagerOperation {

    private final int numSubpartitions;

    private final HsSubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private final HsMemoryDataSpiller spiller;

    private final HsSpillingStrategy spillStrategy;

    private final HsFileDataIndex fileDataIndex;

    private final BufferPool bufferPool;

    private final Lock lock;

    private final AtomicInteger numRequestedBuffers = new AtomicInteger(0);

    private final AtomicInteger numUnSpillBuffers = new AtomicInteger(0);

    public HsMemoryDataManager(
            int numSubpartitions,
            int bufferSize,
            BufferPool bufferPool,
            HsSpillingStrategy spillStrategy,
            HsFileDataIndex fileDataIndex,
            FileChannel dataFileChannel) {
        this.numSubpartitions = numSubpartitions;
        this.bufferPool = bufferPool;
        this.spiller = new HsMemoryDataSpiller(dataFileChannel);
        this.spillStrategy = spillStrategy;
        this.fileDataIndex = fileDataIndex;
        this.subpartitionMemoryDataManagers = new HsSubpartitionMemoryDataManager[numSubpartitions];

        ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
        this.lock = readWriteLock.writeLock();

        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionMemoryDataManagers[subpartitionId] =
                    new HsSubpartitionMemoryDataManager(
                            subpartitionId, bufferSize, readWriteLock.readLock(), this);
        }
    }

    // ------------------------------------
    //          For ResultPartition
    // ------------------------------------

    /**
     * Append record to {@link HsMemoryDataManager}, It will be managed by {@link
     * HsSubpartitionMemoryDataManager} witch it belongs to.
     *
     * @param record to be managed by this class.
     * @param targetChannel target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void append(ByteBuffer record, int targetChannel, Buffer.DataType dataType)
            throws IOException {
        try {
            getSubpartitionMemoryDataManager(targetChannel).append(record, dataType);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    // ------------------------------------
    //        For Spilling Strategy
    // ------------------------------------

    @Override
    public int getPoolSize() {
        return bufferPool.getNumBuffers();
    }

    @Override
    public int getNumSubpartitions() {
        return numSubpartitions;
    }

    @Override
    public int getNumTotalRequestedBuffers() {
        return numRequestedBuffers.get();
    }

    @Override
    public int getNumTotalUnSpillBuffers() {
        return numUnSpillBuffers.get();
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public Deque<BufferIndexAndChannel> getBuffersInOrder(
            int subpartitionId, SpillStatus spillStatus, ConsumeStatus consumeStatus) {
        HsSubpartitionMemoryDataManager targetSubpartitionDataManager =
                getSubpartitionMemoryDataManager(subpartitionId);
        return targetSubpartitionDataManager.getBuffersSatisfyStatus(spillStatus, consumeStatus);
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public List<Integer> getNextBufferIndexToConsume() {
        // TODO implements this logical when subpartition view is implemented.
        return Collections.emptyList();
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public void markBufferReadableFromFile(int subpartitionId, int bufferIndex) {
        fileDataIndex.markBufferReadable(subpartitionId, bufferIndex);
    }

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        MemorySegment segment = bufferPool.requestMemorySegmentBlocking();
        Optional<Decision> decisionOpt =
                spillStrategy.onMemoryUsageChanged(
                        numRequestedBuffers.incrementAndGet(), getPoolSize());

        handleDecision(decisionOpt);
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    @Override
    public void onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        Optional<Decision> decision = spillStrategy.onBufferConsumed(consumedBuffer);
        handleDecision(decision);
    }

    @Override
    public void onBufferFinished() {
        Optional<Decision> decision =
                spillStrategy.onBufferFinished(numUnSpillBuffers.incrementAndGet());
        handleDecision(decision);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    // Attention: Do not call this method within the read lock and subpartition lock, otherwise
    // deadlock may occur as this method maybe acquire write lock and other subpartition's lock
    // inside.
    private void handleDecision(
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
                    Optional<Decision> decisionOpt) {
        Decision decision =
                decisionOpt.orElseGet(
                        () -> callWithLock(() -> spillStrategy.decideActionWithGlobalInfo(this)));

        if (!decision.getBufferToSpill().isEmpty()) {
            spillBuffers(decision.getBufferToSpill());
        }
        if (!decision.getBufferToRelease().isEmpty()) {
            releaseBuffers(decision.getBufferToRelease());
        }
    }

    /**
     * Spill buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toSpill All buffers that need to be spilled in a decision.
     */
    private void spillBuffers(Map<Integer, List<BufferIndexAndChannel>> toSpill) {
        CompletableFuture<Void> spillingCompleteFuture = new CompletableFuture<>();
        List<BufferWithIdentity> bufferWithIdentities = new ArrayList<>();
        toSpill.forEach(
                (subpartitionId, bufferIndexAndChannels) -> {
                    HsSubpartitionMemoryDataManager subpartitionDataManager =
                            getSubpartitionMemoryDataManager(subpartitionId);
                    bufferWithIdentities.addAll(
                            subpartitionDataManager.spillSubpartitionBuffers(
                                    bufferIndexAndChannels, spillingCompleteFuture));
                    // decrease numUnSpillBuffers as this subpartition's buffer is spill.
                    numUnSpillBuffers.getAndAdd(-bufferIndexAndChannels.size());
                });

        spiller.spillAsync(bufferWithIdentities)
                .thenAccept(
                        spilledBuffers -> {
                            fileDataIndex.addBuffers(spilledBuffers);
                            spillingCompleteFuture.complete(null);
                        });
    }

    /**
     * Release buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toRelease All buffers that need to be released in a decision.
     */
    private void releaseBuffers(Map<Integer, List<BufferIndexAndChannel>> toRelease) {
        toRelease.forEach(
                (subpartitionId, subpartitionBuffers) ->
                        getSubpartitionMemoryDataManager(subpartitionId)
                                .releaseSubpartitionBuffers(subpartitionBuffers));
    }

    private HsSubpartitionMemoryDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionMemoryDataManagers[targetChannel];
    }

    private void recycleBuffer(MemorySegment buffer) {
        numRequestedBuffers.decrementAndGet();
        bufferPool.recycle(buffer);
    }

    public <T, R extends Exception> T callWithLock(SupplierWithException<T, R> callable) throws R {
        try {
            lock.lock();
            return callable.get();
        } finally {
            lock.unlock();
        }
    }

    /** Integrate the buffer and dataType of next buffer. */
    public static class BufferAndNextDataType {
        private final Buffer buffer;

        private final Buffer.DataType nextDataType;

        public BufferAndNextDataType(Buffer buffer, Buffer.DataType nextDataType) {
            this.buffer = buffer;
            this.nextDataType = nextDataType;
        }

        public Buffer getBuffer() {
            return buffer;
        }

        public Buffer.DataType getNextDataType() {
            return nextDataType;
        }
    }
}
