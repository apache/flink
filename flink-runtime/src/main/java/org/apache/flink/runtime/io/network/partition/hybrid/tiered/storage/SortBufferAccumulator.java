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
import org.apache.flink.util.function.TriConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

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
 * {@link SortBufferAccumulator} will be numBuffers at most. Once the {@link DataBuffer} is full, or
 * switching from broadcast to non-broadcast(or vice versa), the buffer in the sort buffer will be
 * flushed to the tiers.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 */
public class SortBufferAccumulator implements BufferAccumulator {

    /** The number of the subpartitions. */
    private final int numSubpartitions;

    /** The total number of the buffers used by the {@link SortBufferAccumulator}. */
    private final int numBuffers;

    /** The byte size of one single buffer. */
    private final int bufferSizeBytes;

    /** The empty buffers without storing data. */
    @GuardedBy("lock")
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
    @GuardedBy("lock")
    @Nullable
    private TieredStorageSortBuffer currentDataBuffer;

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

    /** Whether the current {@link DataBuffer} is a broadcast sort buffer. */
    private boolean isBroadcastDataBuffer;

    @GuardedBy("lock")
    private boolean isDataBufferReleased;

    private final Object lock = new Object();

    public SortBufferAccumulator(
            int numSubpartitions,
            int numBuffers,
            int bufferSizeBytes,
            TieredStorageMemoryManager memoryManager,
            boolean isPartialRecordAllowed) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSizeBytes = bufferSizeBytes;
        this.numBuffers = numBuffers;
        this.memoryManager = memoryManager;
        this.isPartialRecordAllowed = isPartialRecordAllowed;
    }

    @Override
    public void setup(TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> bufferFlusher) {
        this.accumulatedBufferFlusher = bufferFlusher;
    }

    @Override
    public void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {
        int targetSubpartition = subpartitionId.getSubpartitionId();
        synchronized (lock) {
            switchCurrentDataBufferIfNeeded(isBroadcast);
            if (!checkNotNull(currentDataBuffer).append(record, targetSubpartition, dataType)) {
                return;
            }

            // The sort buffer is empty, but we failed to write the record into it, which indicates
            // the record is larger than the sort buffer can hold. So the record is written into
            // multiple buffers directly.
            if (!currentDataBuffer.hasRemaining()) {
                isDataBufferReleased = true;
                currentDataBuffer.release();
                writeLargeRecord(record, targetSubpartition, dataType);
                return;
            }
            flushDataBuffer();
        }

        checkState(record.hasRemaining(), "Empty record.");
        receive(record, subpartitionId, dataType, isBroadcast);
    }

    @Override
    public void close() {
        synchronized (lock) {
            flushCurrentDataBuffer();
            isDataBufferReleased = true;
            releaseFreeBuffers();
            if (currentDataBuffer != null) {
                currentDataBuffer.release();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------
    @GuardedBy("lock")
    private void switchCurrentDataBufferIfNeeded(boolean isBroadcast) {
        if (isBroadcast == isBroadcastDataBuffer
                && currentDataBuffer != null
                && !currentDataBuffer.isReleased()
                && !currentDataBuffer.isFinished()) {
            return;
        }
        isBroadcastDataBuffer = isBroadcast;
        flushCurrentDataBuffer();
        isDataBufferReleased = true;
        currentDataBuffer = createNewDataBuffer();
        isDataBufferReleased = false;
    }

    @GuardedBy("lock")
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

    @GuardedBy("lock")
    private void requestBuffers() {
        while (freeSegments.size() < numBuffers) {
            Buffer buffer = requestBuffer();
            freeSegments.add(checkNotNull(buffer).getMemorySegment());
            if (bufferRecycler == null) {
                bufferRecycler = buffer.getRecycler();
            }
        }
    }

    @GuardedBy("lock")
    private void flushDataBuffer() {
        if (currentDataBuffer == null
                || currentDataBuffer.isReleased()
                || !currentDataBuffer.hasRemaining()) {
            return;
        }
        currentDataBuffer.finish();

        do {
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
        } while (true);

        isDataBufferReleased = true;
        releaseFreeBuffers();
        currentDataBuffer.release();
    }

    private void flushCurrentDataBuffer() {
        synchronized (lock) {
            if (currentDataBuffer != null) {
                flushDataBuffer();
                currentDataBuffer = null;
            }
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
        synchronized (lock) {
            MemorySegment freeSegment = freeSegments.poll();
            if (freeSegment == null) {
                freeSegment = requestBuffer().getMemorySegment();
            }
            return freeSegment;
        }
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
        synchronized (lock) {
            isDataBufferReleased = true;
            freeSegments.forEach(this::recycleBuffer);
            freeSegments.clear();
        }
    }

    private void recycleBuffer(MemorySegment memorySegment) {
        synchronized (lock) {
            if (!isDataBufferReleased
                    && currentDataBuffer != null
                    && !currentDataBuffer.isReleased()) {
                freeSegments.add(memorySegment);
            } else {
                checkNotNull(bufferRecycler).recycle(memorySegment);
            }
        }
    }
}
