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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferWithSubpartition;
import org.apache.flink.runtime.io.network.partition.DataBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.TriConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The sort-based implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator}
 * receives the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The accumulated buffers will be transferred to the corresponding
 * tier dynamically.
 *
 * <p>The {@link SortBufferAccumulator} can help use less buffers to accumulate data, which
 * decouples the buffer usage with the number of parallelism. The number of buffers used by the
 * {@link SortBufferAccumulator} will be numExpectedBuffers at most. Once the {@link DataBuffer} is
 * full, or switching from broadcast to non-broadcast(or vice versa), the buffer in the sort buffer
 * will be flushed to the tiers.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 */
public class SortBufferAccumulator implements BufferAccumulator {

    /** The number of the subpartitions. */
    private final int numSubpartitions;

    /** The number of the expected buffers used by the {@link SortBufferAccumulator}. */
    private final int numExpectedBuffers;

    /** The byte size of one single buffer. */
    private final int bufferSizeBytes;

    /** The empty buffers without storing data. */
    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    /** The memory manager of the tiered storage. */
    private final TieredStorageMemoryManager memoryManager;

    private final boolean isPartialRecordAllowed;

    /**
     * The {@link DataBuffer} is utilized to accumulate the incoming records. Whenever there is a
     * transition from broadcast to non-broadcast (or vice versa), the buffer is flushed to ensure
     * data integrity. Note that this can be null before using it to store records, and this {@link
     * DataBuffer} will be released once flushed.
     */
    @Nullable private TieredStorageSortBuffer currentDataBuffer;

    /**
     * The buffer recycler. Note that this can be null before requesting buffers from the memory
     * manager.
     */
    @Nullable private BufferRecycler bufferRecycler;

    /**
     * The {@link SortBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    @Nullable
    private TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> accumulatedBufferFlusher;

    /**
     * An executor to periodically check the size of buffer pool. If the size is changed, the
     * accumulated buffers should be flushed to release the buffers.
     */
    private final ScheduledExecutorService periodicalAccumulatorFlusher =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("hybrid-shuffle-periodical-accumulator-flusher"));

    private final long poolSizeCheckInterval;

    private AtomicInteger poolSize;

    /** Whether the current {@link DataBuffer} is a broadcast sort buffer. */
    private boolean isBroadcastDataBuffer;

    public SortBufferAccumulator(
            int numSubpartitions,
            int numExpectedBuffers,
            int bufferSizeBytes,
            long poolSizeCheckInterval,
            TieredStorageMemoryManager memoryManager,
            boolean isPartialRecordAllowed) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSizeBytes = bufferSizeBytes;
        this.numExpectedBuffers = numExpectedBuffers;
        this.poolSizeCheckInterval = poolSizeCheckInterval;
        this.memoryManager = memoryManager;
        this.isPartialRecordAllowed = isPartialRecordAllowed;
        this.poolSize = new AtomicInteger(-1);
    }

    @Override
    public void setup(TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> bufferFlusher) {
        this.accumulatedBufferFlusher = bufferFlusher;

        if (poolSizeCheckInterval > 0) {
            periodicalAccumulatorFlusher.scheduleWithFixedDelay(
                    () -> {
                        int newSize = this.memoryManager.getBufferPoolSize();
                        int oldSize = poolSize.getAndSet(newSize);
                        if (oldSize > newSize) {
                            if (!returnFreeSegments(oldSize - newSize)) {
                                flushCurrentDataBuffer();
                            }
                        }
                    },
                    poolSizeCheckInterval,
                    poolSizeCheckInterval,
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {
        int targetSubpartition = subpartitionId.getSubpartitionId();
        switchCurrentDataBufferIfNeeded(isBroadcast);
        if (!checkNotNull(currentDataBuffer).append(record, targetSubpartition, dataType)) {
            return;
        }

        // The sort buffer is empty, but we failed to write the record into it, which indicates the
        // record is larger than the sort buffer can hold. So the record is written into multiple
        // buffers directly.
        if (!currentDataBuffer.hasRemaining()) {
            currentDataBuffer.release();
            writeLargeRecord(record, targetSubpartition, dataType);
            return;
        }

        flushDataBuffer();
        checkState(record.hasRemaining(), "Empty record.");
        receive(record, subpartitionId, dataType, isBroadcast);
    }

    @Override
    public void close() {
        if (periodicalAccumulatorFlusher != null) {
            periodicalAccumulatorFlusher.shutdown();
            try {
                if (!periodicalAccumulatorFlusher.awaitTermination(5L, TimeUnit.MINUTES)) {
                    throw new TimeoutException(
                            "Timeout for shutting down the periodical accumulator flusher.");
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
        }
        flushCurrentDataBuffer();
        releaseFreeBuffers();
        if (currentDataBuffer != null) {
            currentDataBuffer.release();
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private synchronized void switchCurrentDataBufferIfNeeded(boolean isBroadcast) {
        if (isBroadcast == isBroadcastDataBuffer
                && currentDataBuffer != null
                && !currentDataBuffer.isReleased()
                && !currentDataBuffer.isFinished()) {
            return;
        }
        isBroadcastDataBuffer = isBroadcast;
        flushCurrentDataBuffer();
        currentDataBuffer = createNewDataBuffer();
    }

    private TieredStorageSortBuffer createNewDataBuffer() {
        requestBuffers();

        // Use the half of the buffers for writing, and the other half for reading
        int numBuffersForSort = freeSegments.size() / 2;
        return new TieredStorageSortBuffer(
                freeSegments,
                this::recycleBuffer,
                numSubpartitions,
                bufferSizeBytes,
                numBuffersForSort,
                isPartialRecordAllowed);
    }

    private void requestBuffers() {
        while (freeSegments.size()
                < Math.min(numExpectedBuffers, memoryManager.getBufferPoolSize() - 1)) {
            Buffer buffer = requestBuffer();
            freeSegments.add(checkNotNull(buffer).getMemorySegment());
            if (bufferRecycler == null) {
                bufferRecycler = buffer.getRecycler();
            }
        }
    }

    private synchronized void flushDataBuffer() {
        if (currentDataBuffer == null
                || currentDataBuffer.isReleased()
                || !currentDataBuffer.hasRemaining()) {
            return;
        }
        currentDataBuffer.finish();

        while (currentDataBuffer.hasRemaining()) {
            MemorySegment freeSegment = getFreeSegment();
            BufferWithSubpartition bufferWithSubpartition =
                    currentDataBuffer.getNextBuffer(freeSegment);
            if (bufferWithSubpartition == null) {
                break;
            }
            int numRemainingConsecutiveBuffers =
                    (int)
                            Math.ceil(
                                    ((double) currentDataBuffer.getRecordRemainingBytes())
                                            / bufferSizeBytes);
            flushBuffer(bufferWithSubpartition, numRemainingConsecutiveBuffers);
        }

        releaseFreeBuffers();
        currentDataBuffer.release();
    }

    private synchronized void flushCurrentDataBuffer() {
        if (currentDataBuffer != null) {
            flushDataBuffer();
            currentDataBuffer = null;
        }
    }

    private synchronized boolean returnFreeSegments(int numSegments) {
        if (currentDataBuffer == null
                || currentDataBuffer.isReleased()
                || !currentDataBuffer.hasRemaining()) {
            return false;
        } else {
            return currentDataBuffer.returnFreeSegments(numSegments);
        }
    }

    private void writeLargeRecord(ByteBuffer record, int subpartitionId, Buffer.DataType dataType) {

        checkState(dataType != Buffer.DataType.EVENT_BUFFER);
        while (record.hasRemaining()) {
            int toCopy = Math.min(record.remaining(), bufferSizeBytes);
            MemorySegment writeBuffer = requestBuffer().getMemorySegment();
            writeBuffer.put(0, record, toCopy);

            int numRemainingConsecutiveBuffers =
                    (int) Math.ceil(((double) record.remaining()) / bufferSizeBytes);
            if (numRemainingConsecutiveBuffers == 0) {
                dataType = Buffer.DataType.DATA_BUFFER_WITH_CLEAR_END;
            }

            flushBuffer(
                    new BufferWithSubpartition(
                            new NetworkBuffer(
                                    writeBuffer, checkNotNull(bufferRecycler), dataType, toCopy),
                            subpartitionId),
                    numRemainingConsecutiveBuffers);
        }

        releaseFreeBuffers();
    }

    private MemorySegment getFreeSegment() {
        MemorySegment freeSegment = freeSegments.poll();
        if (freeSegment == null) {
            freeSegment = requestBuffer().getMemorySegment();
        }
        return freeSegment;
    }

    private void flushBuffer(
            BufferWithSubpartition bufferWithSubpartition, int numRemainingConsecutiveBuffers) {
        checkNotNull(accumulatedBufferFlusher)
                .accept(
                        new TieredStorageSubpartitionId(
                                bufferWithSubpartition.getSubpartitionIndex()),
                        bufferWithSubpartition.getBuffer(),
                        numRemainingConsecutiveBuffers);
    }

    private Buffer requestBuffer() {
        BufferBuilder bufferBuilder = memoryManager.requestBufferBlocking(this);
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        bufferBuilder.close();
        bufferConsumer.close();
        return buffer;
    }

    private void releaseFreeBuffers() {
        freeSegments.forEach(this::recycleBuffer);
        freeSegments.clear();
    }

    private void recycleBuffer(MemorySegment memorySegment) {
        checkNotNull(bufferRecycler).recycle(memorySegment);
    }
}
