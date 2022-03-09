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
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

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

    /** A buffer pool to request memory segments from. */
    private final BufferPool bufferPool;

    /** Number of guaranteed buffers can be allocated from the buffer pool for data sort. */
    private final int numGuaranteedBuffers;

    /** Buffers containing data for all subpartitions. */
    private final ArrayDeque<BufferConsumer>[] buffers;

    // ---------------------------------------------------------------------------------------------
    // Statistics and states
    // ---------------------------------------------------------------------------------------------

    /** Total number of bytes already appended to this sort buffer. */
    private long numTotalBytes;

    /** Total number of records already appended to this sort buffer. */
    private long numTotalRecords;

    /** Whether this sort buffer is full and ready to read data from. */
    private boolean isFull;

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
            BufferPool bufferPool,
            int numSubpartitions,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        checkArgument(numGuaranteedBuffers > 0, "No guaranteed buffers for sort.");

        this.bufferPool = checkNotNull(bufferPool);
        this.numGuaranteedBuffers = numGuaranteedBuffers;

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
        checkState(!isFull, "Sort buffer is already full.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = source.remaining();
        if (dataType.isBuffer()) {
            writeRecord(source, targetChannel);
        } else {
            writeEvent(source, targetChannel, dataType);
        }

        isFull = source.hasRemaining();
        if (!isFull) {
            ++numTotalRecords;
        }
        numTotalBytes += totalBytes - source.remaining();
        return isFull;
    }

    private void writeEvent(ByteBuffer source, int targetChannel, Buffer.DataType dataType) {
        BufferBuilder builder = builders[targetChannel];
        if (builder != null) {
            builder.finish();
            buffers[targetChannel].add(builder.createBufferConsumerFromBeginning());
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

    private void writeRecord(ByteBuffer source, int targetChannel) throws IOException {
        do {
            BufferBuilder builder = builders[targetChannel];
            if (builder == null) {
                builder = requestBufferFromPool();
                if (builder == null) {
                    break;
                }
                ++numBuffersOccupied;
                builders[targetChannel] = builder;
            }

            builder.append(source);
            if (builder.isFull()) {
                builder.finish();
                buffers[targetChannel].add(builder.createBufferConsumerFromBeginning());
                builder.close();
                builders[targetChannel] = null;
            }
        } while (source.hasRemaining());
    }

    private BufferBuilder requestBufferFromPool() throws IOException {
        try {
            // blocking request buffers if there is still guaranteed memory
            if (numBuffersOccupied < numGuaranteedBuffers) {
                return bufferPool.requestBufferBuilderBlocking();
            }
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while requesting buffer.", e);
        }

        return bufferPool.requestBufferBuilder();
    }

    @Override
    public BufferWithChannel getNextBuffer(MemorySegment transitBuffer) {
        checkState(isFull, "Sort buffer is not ready to be read.");
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
    public void reset() {
        checkState(!isFinished, "Sort buffer has been finished.");
        checkState(!isReleased, "Sort buffer has been released.");

        isFull = false;
        readOrderIndex = 0;
    }

    @Override
    public void finish() {
        checkState(!isFull, "DataBuffer must not be full.");
        checkState(!isFinished, "DataBuffer is already finished.");

        isFull = true;
        isFinished = true;
        for (int channel = 0; channel < builders.length; ++channel) {
            BufferBuilder builder = builders[channel];
            if (builder != null) {
                builder.finish();
                buffers[channel].add(builder.createBufferConsumerFromBeginning());
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
