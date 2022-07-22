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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * * A {@link DataBuffer} implementation which sorts all appended records only by subpartition
 * index. Records of the same subpartition keep the appended order.
 *
 * <p>Different from the {@link SortBasedDataBuffer}, in this {@link DataBuffer} implementation,
 * memory segment boundary serves as the nature data boundary of different subpartitions, which
 * means that one memory segment can never contain data from different subpartitions.
 */
public class HashBasedDataBuffer implements DataBuffer {

    /** A list of {@link MemorySegment}s used to store data in memory. */
    private final LinkedList<MemorySegment> freeSegments;

    /** {@link BufferRecycler} used to recycle {@link #freeSegments}. */
    private final BufferRecycler bufferRecycler;

    /** Number of guaranteed buffers can be allocated from the buffer pool for data sort. */
    private final int numGuaranteedBuffers;

    /** Buffers containing data for all subpartitions. */
    private final ArrayDeque<BufferConsumer>[] buffers;

    /** Size of buffers requested from buffer pool. All buffers must be of the same size. */
    private final int bufferSize;

    // ---------------------------------------------------------------------------------------------
    // Statistics and states
    // ---------------------------------------------------------------------------------------------

    /** Total number of bytes already appended to this sort buffer. */
    private long numTotalBytes;

    /** Total number of records already appended to this sort buffer. */
    private long numTotalRecords;

    /** Whether this sort buffer is finished. One can only read a finished sort buffer. */
    private boolean isFinished;

    /** Whether this sort buffer is released. A released sort buffer can not be used. */
    private boolean isReleased;

    // ---------------------------------------------------------------------------------------------
    // For writing
    // ---------------------------------------------------------------------------------------------

    /** Partial buffers to be appended data for each channel. */
    private final BufferBuilder[] builders;

    /** Total number of network buffers already occupied currently by this sort buffer. */
    private int numBuffersOccupied;

    // ---------------------------------------------------------------------------------------------
    // For reading
    // ---------------------------------------------------------------------------------------------

    /** Used to index the current available channel to read data from. */
    private int readOrderIndex;

    /** Data of different subpartitions in this sort buffer will be read in this order. */
    private final int[] subpartitionReadOrder;

    /** Total number of bytes already read from this sort buffer. */
    private long numTotalBytesRead;

    public HashBasedDataBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        checkArgument(numGuaranteedBuffers > 0, "No guaranteed buffers for sort.");

        this.freeSegments = checkNotNull(freeSegments);
        this.bufferRecycler = checkNotNull(bufferRecycler);
        this.bufferSize = bufferSize;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
        checkState(numGuaranteedBuffers <= freeSegments.size(), "Wrong number of free segments.");

        this.builders = new BufferBuilder[numSubpartitions];
        this.buffers = new ArrayDeque[numSubpartitions];
        for (int channel = 0; channel < numSubpartitions; ++channel) {
            this.buffers[channel] = new ArrayDeque<>();
        }

        this.subpartitionReadOrder = new int[numSubpartitions];
        if (customReadOrder != null) {
            checkArgument(customReadOrder.length == numSubpartitions, "Illegal data read order.");
            System.arraycopy(customReadOrder, 0, this.subpartitionReadOrder, 0, numSubpartitions);
        } else {
            for (int channel = 0; channel < numSubpartitions; ++channel) {
                this.subpartitionReadOrder[channel] = channel;
            }
        }
    }

    /**
     * Partial data of the target record can be written if this {@link HashBasedDataBuffer} is full.
     * The remaining data of the target record will be written to the next data region (a new data
     * buffer or this data buffer after reset).
     */
    @Override
    public boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType)
            throws IOException {
        checkArgument(source.hasRemaining(), "Cannot append empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = source.remaining();
        if (dataType.isBuffer()) {
            writeRecord(source, targetChannel);
        } else {
            writeEvent(source, targetChannel, dataType);
        }

        if (source.hasRemaining()) {
            return true;
        }
        ++numTotalRecords;
        numTotalBytes += totalBytes - source.remaining();
        return false;
    }

    private void writeEvent(ByteBuffer source, int targetChannel, Buffer.DataType dataType) {
        BufferBuilder builder = builders[targetChannel];
        if (builder != null) {
            builder.finish();
            builder.close();
            builders[targetChannel] = null;
        }

        MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(source.remaining());
        segment.put(0, source, segment.size());
        BufferConsumer consumer =
                new BufferConsumer(
                        new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType),
                        segment.size());
        buffers[targetChannel].add(consumer);
    }

    private void writeRecord(ByteBuffer source, int targetChannel) {
        BufferBuilder builder = builders[targetChannel];
        int availableBytes = builder != null ? builder.getWritableBytes() : 0;
        if (source.remaining()
                > availableBytes
                        + (numGuaranteedBuffers - numBuffersOccupied) * (long) bufferSize) {
            return;
        }

        do {
            if (builder == null) {
                builder = new BufferBuilder(freeSegments.poll(), bufferRecycler);
                buffers[targetChannel].add(builder.createBufferConsumer());
                ++numBuffersOccupied;
                builders[targetChannel] = builder;
            }

            builder.append(source);
            if (builder.isFull()) {
                builder.finish();
                builder.close();
                builders[targetChannel] = null;
                builder = null;
            }
        } while (source.hasRemaining());
    }

    @Override
    public BufferWithChannel getNextBuffer(MemorySegment transitBuffer) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        BufferWithChannel buffer = null;
        if (!hasRemaining() || readOrderIndex >= subpartitionReadOrder.length) {
            return null;
        }

        int targetChannel = subpartitionReadOrder[readOrderIndex];
        while (buffer == null) {
            BufferConsumer consumer = buffers[targetChannel].poll();
            if (consumer != null) {
                buffer = new BufferWithChannel(consumer.build(), targetChannel);
                numBuffersOccupied -= buffer.getBuffer().isBuffer() ? 1 : 0;
                numTotalBytesRead += buffer.getBuffer().readableBytes();
                consumer.close();
            } else {
                if (++readOrderIndex >= subpartitionReadOrder.length) {
                    break;
                }
                targetChannel = subpartitionReadOrder[readOrderIndex];
            }
        }
        return buffer;
    }

    @Override
    public long numTotalRecords() {
        return numTotalRecords;
    }

    @Override
    public long numTotalBytes() {
        return numTotalBytes;
    }

    @Override
    public boolean hasRemaining() {
        return numTotalBytesRead < numTotalBytes;
    }

    @Override
    public void finish() {
        checkState(!isFinished, "DataBuffer is already finished.");

        isFinished = true;
        for (int channel = 0; channel < builders.length; ++channel) {
            BufferBuilder builder = builders[channel];
            if (builder != null) {
                builder.finish();
                builder.close();
                builders[channel] = null;
            }
        }
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;

        for (int channel = 0; channel < builders.length; ++channel) {
            BufferBuilder builder = builders[channel];
            if (builder != null) {
                builder.close();
                builders[channel] = null;
            }
        }

        for (ArrayDeque<BufferConsumer> buffer : buffers) {
            BufferConsumer consumer = buffer.poll();
            while (consumer != null) {
                consumer.close();
                consumer = buffer.poll();
            }
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }
}
