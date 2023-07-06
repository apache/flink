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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link DataBuffer} implementation which sorts all appended records only by subpartition index.
 * Records of the same subpartition keep the appended order.
 *
 * <p>It maintains a list of {@link MemorySegment}s as a joint buffer. Data will be appended to the
 * joint buffer sequentially. When writing a record, an index entry will be appended first. An index
 * entry consists of 4 fields: 4 bytes for record length, 4 bytes for {@link Buffer.DataType} and 8
 * bytes for address pointing to the next index entry of the same channel which will be used to
 * index the next record to read when coping data from this {@link DataBuffer}. For simplicity, no
 * index entry can span multiple segments. The corresponding record data is seated right after its
 * index entry and different from the index entry, records have variable length thus may span
 * multiple segments.
 */
@NotThreadSafe
public abstract class SortBuffer implements DataBuffer {

    /**
     * Size of an index entry: 4 bytes for record length, 4 bytes for data type and 8 bytes for
     * pointer to next entry.
     */
    protected static final int INDEX_ENTRY_SIZE = 4 + 4 + 8;

    /** A list of {@link MemorySegment}s used to store data in memory. */
    protected final LinkedList<MemorySegment> freeSegments;

    /** {@link BufferRecycler} used to recycle {@link #freeSegments}. */
    protected final BufferRecycler bufferRecycler;

    /** A segment list as a joint buffer which stores all records and index entries. */
    public final ArrayList<MemorySegment> segments = new ArrayList<>();

    /** Addresses of the first record's index entry for each subpartition. */
    private final long[] firstIndexEntryAddresses;

    /** Addresses of the last record's index entry for each subpartition. */
    protected final long[] lastIndexEntryAddresses;

    /** Size of buffers requested from buffer pool. All buffers must be of the same size. */
    private final int bufferSize;

    /** Number of guaranteed buffers can be allocated from the buffer pool for data sort. */
    private final int numGuaranteedBuffers;

    // ---------------------------------------------------------------------------------------------
    // Statistics and states
    // ---------------------------------------------------------------------------------------------

    /** Total number of bytes already appended to this sort buffer. */
    private long numTotalBytes;

    /** Total number of records already appended to this sort buffer. */
    private long numTotalRecords;

    /** Total number of bytes already read from this sort buffer. */
    protected long numTotalBytesRead;

    /** Whether this sort buffer is finished. One can only read a finished sort buffer. */
    protected boolean isFinished;

    /** Whether this sort buffer is released. A released sort buffer can not be used. */
    protected boolean isReleased;

    // ---------------------------------------------------------------------------------------------
    // For writing
    // ---------------------------------------------------------------------------------------------

    /** Array index in the segment list of the current available buffer for writing. */
    private int writeSegmentIndex;

    /** Next position in the current available buffer for writing. */
    private int writeSegmentOffset;

    // ---------------------------------------------------------------------------------------------
    // For reading
    // ---------------------------------------------------------------------------------------------

    /** Data of different subpartitions in this sort buffer will be read in this order. */
    protected final int[] subpartitionReadOrder;

    /** Index entry address of the current record or event to be read. */
    protected long readIndexEntryAddress;

    /** Record bytes remaining after last copy, which must be read first in next copy. */
    protected int recordRemainingBytes;

    /** Used to index the current available channel to read data from. */
    protected int readOrderIndex = -1;

    protected SortBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        checkArgument(bufferSize > INDEX_ENTRY_SIZE, "Buffer size is too small.");
        checkArgument(numGuaranteedBuffers > 0, "No guaranteed buffers for sort.");

        this.freeSegments = checkNotNull(freeSegments);
        this.bufferRecycler = checkNotNull(bufferRecycler);
        this.bufferSize = bufferSize;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
        checkState(numGuaranteedBuffers <= freeSegments.size(), "Wrong number of free segments.");
        this.firstIndexEntryAddresses = new long[numSubpartitions];
        this.lastIndexEntryAddresses = new long[numSubpartitions];

        // initialized with -1 means the corresponding channel has no data
        Arrays.fill(firstIndexEntryAddresses, -1L);
        Arrays.fill(lastIndexEntryAddresses, -1L);

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
     * No partial record will be written to this {@link SortBasedDataBuffer}, which means that
     * either all data of target record will be written or nothing will be written.
     */
    @Override
    public boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType)
            throws IOException {
        checkArgument(source.hasRemaining(), "Cannot append empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = source.remaining();

        // return true directly if it can not allocate enough buffers for the given record
        if (!allocateBuffersForRecord(totalBytes)) {
            return true;
        }

        // write the index entry and record or event data
        writeIndex(targetChannel, totalBytes, dataType);
        writeRecord(source);

        ++numTotalRecords;
        numTotalBytes += totalBytes;

        return false;
    }

    private void writeIndex(int channelIndex, int numRecordBytes, Buffer.DataType dataType) {
        MemorySegment segment = segments.get(writeSegmentIndex);

        // record length takes the high 32 bits and data type takes the low 32 bits
        segment.putLong(writeSegmentOffset, ((long) numRecordBytes << 32) | dataType.ordinal());

        // segment index takes the high 32 bits and segment offset takes the low 32 bits
        long indexEntryAddress = ((long) writeSegmentIndex << 32) | writeSegmentOffset;

        long lastIndexEntryAddress = lastIndexEntryAddresses[channelIndex];
        lastIndexEntryAddresses[channelIndex] = indexEntryAddress;

        if (lastIndexEntryAddress >= 0) {
            // link the previous index entry of the given channel to the new index entry
            segment = segments.get(getSegmentIndexFromPointer(lastIndexEntryAddress));
            segment.putLong(
                    getSegmentOffsetFromPointer(lastIndexEntryAddress) + 8, indexEntryAddress);
        } else {
            firstIndexEntryAddresses[channelIndex] = indexEntryAddress;
        }

        // move the write position forward so as to write the corresponding record
        updateWriteSegmentIndexAndOffset(INDEX_ENTRY_SIZE);
    }

    private void writeRecord(ByteBuffer source) {
        while (source.hasRemaining()) {
            MemorySegment segment = segments.get(writeSegmentIndex);
            int toCopy = Math.min(bufferSize - writeSegmentOffset, source.remaining());
            segment.put(writeSegmentOffset, source, toCopy);

            // move the write position forward so as to write the remaining bytes or next record
            updateWriteSegmentIndexAndOffset(toCopy);
        }
    }

    private boolean allocateBuffersForRecord(int numRecordBytes) {
        int numBytesRequired = INDEX_ENTRY_SIZE + numRecordBytes;
        int availableBytes =
                writeSegmentIndex == segments.size() ? 0 : bufferSize - writeSegmentOffset;

        // return directly if current available bytes is adequate
        if (availableBytes >= numBytesRequired) {
            return true;
        }

        // skip the remaining free space if the available bytes is not enough for an index entry
        if (availableBytes < INDEX_ENTRY_SIZE) {
            updateWriteSegmentIndexAndOffset(availableBytes);
            availableBytes = 0;
        }

        if (availableBytes + (numGuaranteedBuffers - segments.size()) * (long) bufferSize
                < numBytesRequired) {
            return false;
        }

        // allocate exactly enough buffers for the appended record
        do {
            MemorySegment segment = freeSegments.poll();
            availableBytes += bufferSize;
            addBuffer(checkNotNull(segment));
        } while (availableBytes < numBytesRequired);

        return true;
    }

    private void addBuffer(MemorySegment segment) {
        if (segment.size() != bufferSize) {
            bufferRecycler.recycle(segment);
            throw new IllegalStateException("Illegal memory segment size.");
        }

        if (isReleased) {
            bufferRecycler.recycle(segment);
            throw new IllegalStateException("Sort buffer is already released.");
        }

        segments.add(segment);
    }

    private void updateWriteSegmentIndexAndOffset(int numBytes) {
        writeSegmentOffset += numBytes;

        // using the next available free buffer if the current is full
        if (writeSegmentOffset == bufferSize) {
            ++writeSegmentIndex;
            writeSegmentOffset = 0;
        }
    }

    protected int copyRecordOrEvent(
            MemorySegment targetSegment,
            int targetSegmentOffset,
            int sourceSegmentIndex,
            int sourceSegmentOffset,
            int recordLength) {
        if (recordRemainingBytes > 0) {
            // skip the data already read if there is remaining partial record after the previous
            // copy
            long position = (long) sourceSegmentOffset + (recordLength - recordRemainingBytes);
            sourceSegmentIndex += (position / bufferSize);
            sourceSegmentOffset = (int) (position % bufferSize);
        } else {
            recordRemainingBytes = recordLength;
        }

        int targetSegmentSize = targetSegment.size();
        int numBytesToCopy =
                Math.min(targetSegmentSize - targetSegmentOffset, recordRemainingBytes);
        do {
            // move to next data buffer if all data of the current buffer has been copied
            if (sourceSegmentOffset == bufferSize) {
                ++sourceSegmentIndex;
                sourceSegmentOffset = 0;
            }

            int sourceRemainingBytes =
                    Math.min(bufferSize - sourceSegmentOffset, recordRemainingBytes);
            int numBytes = Math.min(targetSegmentSize - targetSegmentOffset, sourceRemainingBytes);
            MemorySegment sourceSegment = segments.get(sourceSegmentIndex);
            sourceSegment.copyTo(sourceSegmentOffset, targetSegment, targetSegmentOffset, numBytes);

            recordRemainingBytes -= numBytes;
            targetSegmentOffset += numBytes;
            sourceSegmentOffset += numBytes;
        } while ((recordRemainingBytes > 0 && targetSegmentOffset < targetSegmentSize));

        return numBytesToCopy;
    }

    protected void updateReadChannelAndIndexEntryAddress() {
        // skip the channels without any data
        while (++readOrderIndex < firstIndexEntryAddresses.length) {
            int channelIndex = subpartitionReadOrder[readOrderIndex];
            if ((readIndexEntryAddress = firstIndexEntryAddresses[channelIndex]) >= 0) {
                break;
            }
        }
    }

    protected int getSegmentIndexFromPointer(long value) {
        return (int) (value >>> 32);
    }

    protected int getSegmentOffsetFromPointer(long value) {
        return (int) (value);
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

        // prepare for reading
        updateReadChannelAndIndexEntryAddress();
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

        for (MemorySegment segment : segments) {
            bufferRecycler.recycle(segment);
        }
        segments.clear();
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }
}
