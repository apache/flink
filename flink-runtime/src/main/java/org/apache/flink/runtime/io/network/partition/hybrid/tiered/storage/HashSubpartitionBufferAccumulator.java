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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link HashSubpartitionBufferAccumulator} accumulates the records in a subpartition.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 */
public class HashSubpartitionBufferAccumulator {

    private final TieredStorageSubpartitionId subpartitionId;

    private final int bufferSize;

    private final HashSubpartitionBufferAccumulatorContext bufferAccumulatorContext;

    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    private final boolean isPartialRecordAllowed;

    public HashSubpartitionBufferAccumulator(
            TieredStorageSubpartitionId subpartitionId,
            int bufferSize,
            HashSubpartitionBufferAccumulatorContext bufferAccumulatorContext,
            boolean isPartialRecordAllowed) {
        this.subpartitionId = subpartitionId;
        this.bufferSize = bufferSize;
        this.bufferAccumulatorContext = bufferAccumulatorContext;
        this.isPartialRecordAllowed = isPartialRecordAllowed;
    }

    // ------------------------------------------------------------------------
    //  Called by HashBufferAccumulator
    // ------------------------------------------------------------------------

    public void append(ByteBuffer record, Buffer.DataType dataType) throws IOException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType);
        } else {
            writeRecord(record, dataType);
        }
    }

    public void close() {
        finishCurrentWritingBufferIfNotEmpty();
        while (!unfinishedBuffers.isEmpty()) {
            unfinishedBuffers.poll().close();
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, Buffer.DataType dataType) {
        checkArgument(dataType.isEvent());

        // Each event should take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty();

        // Store the events in the heap segments to improve network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        flushFinishedBuffer(
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size()), 0);
    }

    private void writeRecord(ByteBuffer record, Buffer.DataType dataType) {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record);
    }

    private void ensureCapacityForRecord(ByteBuffer record) {
        final int numRecordBytes = record.remaining();

        if (!isPartialRecordAllowed
                && !unfinishedBuffers.isEmpty()
                && unfinishedBuffers.peek().getWritableBytes() < numRecordBytes) {
            finishCurrentWritingBufferIfNotEmpty();
        }

        int availableBytes =
                Optional.ofNullable(unfinishedBuffers.peek())
                        .map(
                                currentWritingBuffer ->
                                        currentWritingBuffer.getWritableBytes()
                                                + bufferSize * (unfinishedBuffers.size() - 1))
                        .orElse(0);

        while (availableBytes < numRecordBytes) {
            BufferBuilder bufferBuilder = bufferAccumulatorContext.requestBufferBlocking();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(ByteBuffer record) {
        boolean needFinalFlush = false;
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer = checkNotNull(unfinishedBuffers.peek());
            currentWritingBuffer.append(record);
            if (currentWritingBuffer.isFull()) {
                int numRemainingConsecutiveBuffers = 0;
                if (!isPartialRecordAllowed) {
                    needFinalFlush = true;
                    numRemainingConsecutiveBuffers =
                            (int) Math.ceil(((double) record.remaining()) / bufferSize);
                }
                finishCurrentWritingBuffer(numRemainingConsecutiveBuffers);
            }
        }

        if (needFinalFlush) {
            finishCurrentWritingBuffer(0);
        }
    }

    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer(0);
    }

    private void finishCurrentWritingBuffer(int numRemainingConsecutiveBuffers) {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();
        if (currentWritingBuffer == null) {
            return;
        }
        if (currentWritingBuffer.getDataType() == Buffer.DataType.DATA_BUFFER
                && !isPartialRecordAllowed
                && numRemainingConsecutiveBuffers == 0) {
            currentWritingBuffer.setDataType(Buffer.DataType.DATA_BUFFER_WITH_CLEAR_END);
        }
        currentWritingBuffer.finish();
        BufferConsumer bufferConsumer = currentWritingBuffer.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        currentWritingBuffer.close();
        bufferConsumer.close();
        flushFinishedBuffer(buffer, numRemainingConsecutiveBuffers);
    }

    private void flushFinishedBuffer(Buffer finishedBuffer, int numRemainingConsecutiveBuffers) {
        bufferAccumulatorContext.flushAccumulatedBuffers(
                subpartitionId, finishedBuffer, numRemainingConsecutiveBuffers);
    }
}
