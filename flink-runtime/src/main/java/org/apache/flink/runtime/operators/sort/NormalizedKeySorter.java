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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** */
public final class NormalizedKeySorter<T> implements InMemorySorter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(NormalizedKeySorter.class);

    private static final int OFFSET_LEN = 8;

    private static final int DEFAULT_MAX_NORMALIZED_KEY_LEN = 16;

    private static final int MAX_NORMALIZED_KEY_LEN_PER_ELEMENT = 8;

    private static final int MIN_REQUIRED_BUFFERS = 3;

    private static final int LARGE_RECORD_THRESHOLD = 10 * 1024 * 1024;

    private static final long LARGE_RECORD_TAG = 1L << 63;

    private static final long POINTER_MASK = LARGE_RECORD_TAG - 1;

    // ------------------------------------------------------------------------
    //                               Members
    // ------------------------------------------------------------------------

    private final byte[] swapBuffer;

    private final TypeSerializer<T> serializer;

    private final TypeComparator<T> comparator;

    private final SimpleCollectingOutputView recordCollector;

    private final RandomAccessInputView recordBuffer;

    private final RandomAccessInputView recordBufferForComparison;

    private MemorySegment currentSortIndexSegment;

    private final ArrayList<MemorySegment> freeMemory;

    private final ArrayList<MemorySegment> sortIndex;

    private final ArrayList<MemorySegment> recordBufferSegments;

    private long currentDataBufferOffset;

    private long sortIndexBytes;

    private int currentSortIndexOffset;

    private int numRecords;

    private final int numKeyBytes;

    private final int indexEntrySize;

    private final int indexEntriesPerSegment;

    private final int lastIndexEntryOffset;

    private final int segmentSize;

    private final int totalNumBuffers;

    private final boolean normalizedKeyFullyDetermines;

    private final boolean useNormKeyUninverted;

    // -------------------------------------------------------------------------
    // Constructors / Destructors
    // -------------------------------------------------------------------------

    public NormalizedKeySorter(
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator,
            List<MemorySegment> memory) {
        this(serializer, comparator, memory, DEFAULT_MAX_NORMALIZED_KEY_LEN);
    }

    public NormalizedKeySorter(
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator,
            List<MemorySegment> memory,
            int maxNormalizedKeyBytes) {
        if (serializer == null || comparator == null || memory == null) {
            throw new NullPointerException();
        }
        if (maxNormalizedKeyBytes < 0) {
            throw new IllegalArgumentException(
                    "Maximal number of normalized key bytes must not be negative.");
        }

        this.serializer = serializer;
        this.comparator = comparator;
        this.useNormKeyUninverted = !comparator.invertNormalizedKey();

        // check the size of the first buffer and record it. all further buffers must have the same
        // size.
        // the size must also be a power of 2
        this.totalNumBuffers = memory.size();
        if (this.totalNumBuffers < MIN_REQUIRED_BUFFERS) {
            throw new IllegalArgumentException(
                    "Normalized-Key sorter requires at least "
                            + MIN_REQUIRED_BUFFERS
                            + " memory buffers.");
        }
        this.segmentSize = memory.get(0).size();
        this.freeMemory = new ArrayList<MemorySegment>(memory);

        // create the buffer collections
        this.sortIndex = new ArrayList<MemorySegment>(16);
        this.recordBufferSegments = new ArrayList<MemorySegment>(16);

        // the views for the record collections
        this.recordCollector =
                new SimpleCollectingOutputView(
                        this.recordBufferSegments,
                        new ListMemorySegmentSource(this.freeMemory),
                        this.segmentSize);
        this.recordBuffer = new RandomAccessInputView(this.recordBufferSegments, this.segmentSize);
        this.recordBufferForComparison =
                new RandomAccessInputView(this.recordBufferSegments, this.segmentSize);

        // set up normalized key characteristics
        if (this.comparator.supportsNormalizedKey()) {
            // compute the max normalized key length
            int numPartialKeys;
            try {
                numPartialKeys = this.comparator.getFlatComparators().length;
            } catch (Throwable t) {
                numPartialKeys = 1;
            }

            int maxLen =
                    Math.min(
                            maxNormalizedKeyBytes,
                            MAX_NORMALIZED_KEY_LEN_PER_ELEMENT * numPartialKeys);

            this.numKeyBytes = Math.min(this.comparator.getNormalizeKeyLen(), maxLen);
            this.normalizedKeyFullyDetermines =
                    !this.comparator.isNormalizedKeyPrefixOnly(this.numKeyBytes);
        } else {
            this.numKeyBytes = 0;
            this.normalizedKeyFullyDetermines = false;
        }

        // compute the index entry size and limits
        this.indexEntrySize = this.numKeyBytes + OFFSET_LEN;
        this.indexEntriesPerSegment = this.segmentSize / this.indexEntrySize;
        this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize;
        this.swapBuffer = new byte[this.indexEntrySize];

        // set to initial state
        this.currentSortIndexSegment = nextMemorySegment();
        this.sortIndex.add(this.currentSortIndexSegment);
    }

    @Override
    public int recordSize() {
        return indexEntrySize;
    }

    @Override
    public int recordsPerSegment() {
        return indexEntriesPerSegment;
    }

    // -------------------------------------------------------------------------
    // Memory Segment
    // -------------------------------------------------------------------------

    /**
     * Resets the sort buffer back to the state where it is empty. All contained data is discarded.
     */
    @Override
    public void reset() {
        // reset all offsets
        this.numRecords = 0;
        this.currentSortIndexOffset = 0;
        this.currentDataBufferOffset = 0;
        this.sortIndexBytes = 0;

        // return all memory
        this.freeMemory.addAll(this.sortIndex);
        this.freeMemory.addAll(this.recordBufferSegments);
        this.sortIndex.clear();
        this.recordBufferSegments.clear();

        // grab first buffers
        this.currentSortIndexSegment = nextMemorySegment();
        this.sortIndex.add(this.currentSortIndexSegment);
        this.recordCollector.reset();
    }

    /**
     * Checks whether the buffer is empty.
     *
     * @return True, if no record is contained, false otherwise.
     */
    @Override
    public boolean isEmpty() {
        return this.numRecords == 0;
    }

    @Override
    public void dispose() {
        this.freeMemory.clear();
        this.recordBufferSegments.clear();
        this.sortIndex.clear();
    }

    @Override
    public long getCapacity() {
        return ((long) this.totalNumBuffers) * this.segmentSize;
    }

    @Override
    public long getOccupancy() {
        return this.currentDataBufferOffset + this.sortIndexBytes;
    }

    // -------------------------------------------------------------------------
    // Retrieving and Writing
    // -------------------------------------------------------------------------

    @Override
    public T getRecord(int logicalPosition) throws IOException {
        return getRecordFromBuffer(readPointer(logicalPosition));
    }

    @Override
    public T getRecord(T reuse, int logicalPosition) throws IOException {
        return getRecordFromBuffer(reuse, readPointer(logicalPosition));
    }

    /**
     * Writes a given record to this sort buffer. The written record will be appended and take the
     * last logical position.
     *
     * @param record The record to be written.
     * @return True, if the record was successfully written, false, if the sort buffer was full.
     * @throws IOException Thrown, if an error occurred while serializing the record into the
     *     buffers.
     */
    @Override
    public boolean write(T record) throws IOException {
        // check whether we need a new memory segment for the sort index
        if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
            if (memoryAvailable()) {
                this.currentSortIndexSegment = nextMemorySegment();
                this.sortIndex.add(this.currentSortIndexSegment);
                this.currentSortIndexOffset = 0;
                this.sortIndexBytes += this.segmentSize;
            } else {
                return false;
            }
        }

        // serialize the record into the data buffers
        try {
            this.serializer.serialize(record, this.recordCollector);
        } catch (EOFException e) {
            return false;
        }

        final long newOffset = this.recordCollector.getCurrentOffset();
        final boolean shortRecord =
                newOffset - this.currentDataBufferOffset < LARGE_RECORD_THRESHOLD;

        if (!shortRecord && LOG.isDebugEnabled()) {
            LOG.debug("Put a large record ( >" + LARGE_RECORD_THRESHOLD + " into the sort buffer");
        }

        // add the pointer and the normalized key
        this.currentSortIndexSegment.putLong(
                this.currentSortIndexOffset,
                shortRecord
                        ? this.currentDataBufferOffset
                        : (this.currentDataBufferOffset | LARGE_RECORD_TAG));

        if (this.numKeyBytes != 0) {
            this.comparator.putNormalizedKey(
                    record,
                    this.currentSortIndexSegment,
                    this.currentSortIndexOffset + OFFSET_LEN,
                    this.numKeyBytes);
        }

        this.currentSortIndexOffset += this.indexEntrySize;
        this.currentDataBufferOffset = newOffset;
        this.numRecords++;
        return true;
    }

    // ------------------------------------------------------------------------
    //                           Access Utilities
    // ------------------------------------------------------------------------

    private long readPointer(int logicalPosition) {
        if (logicalPosition < 0 || logicalPosition >= this.numRecords) {
            throw new IndexOutOfBoundsException();
        }

        final int bufferNum = logicalPosition / this.indexEntriesPerSegment;
        final int segmentOffset = logicalPosition % this.indexEntriesPerSegment;

        return (this.sortIndex.get(bufferNum).getLong(segmentOffset * this.indexEntrySize))
                & POINTER_MASK;
    }

    private T getRecordFromBuffer(T reuse, long pointer) throws IOException {
        this.recordBuffer.setReadPosition(pointer);
        return this.serializer.deserialize(reuse, this.recordBuffer);
    }

    private T getRecordFromBuffer(long pointer) throws IOException {
        this.recordBuffer.setReadPosition(pointer);
        return this.serializer.deserialize(this.recordBuffer);
    }

    private int compareRecords(long pointer1, long pointer2) {
        this.recordBuffer.setReadPosition(pointer1);
        this.recordBufferForComparison.setReadPosition(pointer2);

        try {
            return this.comparator.compareSerialized(
                    this.recordBuffer, this.recordBufferForComparison);
        } catch (IOException ioex) {
            throw new RuntimeException("Error comparing two records.", ioex);
        }
    }

    private boolean memoryAvailable() {
        return !this.freeMemory.isEmpty();
    }

    private MemorySegment nextMemorySegment() {
        return this.freeMemory.remove(this.freeMemory.size() - 1);
    }

    // -------------------------------------------------------------------------
    // Indexed Sorting
    // -------------------------------------------------------------------------

    @Override
    public int compare(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        return compare(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public int compare(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        int val =
                segI.compare(
                        segJ,
                        segmentOffsetI + OFFSET_LEN,
                        segmentOffsetJ + OFFSET_LEN,
                        this.numKeyBytes);

        if (val != 0 || this.normalizedKeyFullyDetermines) {
            return this.useNormKeyUninverted ? val : -val;
        }

        final long pointerI = segI.getLong(segmentOffsetI) & POINTER_MASK;
        final long pointerJ = segJ.getLong(segmentOffsetJ) & POINTER_MASK;

        return compareRecords(pointerI, pointerJ);
    }

    @Override
    public void swap(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        swap(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public void swap(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        segI.swapBytes(this.swapBuffer, segJ, segmentOffsetI, segmentOffsetJ, this.indexEntrySize);
    }

    @Override
    public int size() {
        return this.numRecords;
    }

    // -------------------------------------------------------------------------

    /**
     * Gets an iterator over all records in this buffer in their logical order.
     *
     * @return An iterator returning the records in their logical order.
     */
    @Override
    public final MutableObjectIterator<T> getIterator() {
        return new MutableObjectIterator<T>() {
            private final int size = size();
            private int current = 0;

            private int currentSegment = 0;
            private int currentOffset = 0;

            private MemorySegment currentIndexSegment = sortIndex.get(0);

            @Override
            public T next(T target) {
                if (this.current < this.size) {
                    this.current++;
                    if (this.currentOffset > lastIndexEntryOffset) {
                        this.currentOffset = 0;
                        this.currentIndexSegment = sortIndex.get(++this.currentSegment);
                    }

                    long pointer =
                            this.currentIndexSegment.getLong(this.currentOffset) & POINTER_MASK;
                    this.currentOffset += indexEntrySize;

                    try {
                        return getRecordFromBuffer(target, pointer);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }

            @Override
            public T next() {
                if (this.current < this.size) {
                    this.current++;
                    if (this.currentOffset > lastIndexEntryOffset) {
                        this.currentOffset = 0;
                        this.currentIndexSegment = sortIndex.get(++this.currentSegment);
                    }

                    long pointer = this.currentIndexSegment.getLong(this.currentOffset);
                    this.currentOffset += indexEntrySize;

                    try {
                        return getRecordFromBuffer(pointer);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }
        };
    }

    // ------------------------------------------------------------------------
    //                Writing to a DataOutputView
    // ------------------------------------------------------------------------

    /**
     * Writes the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     */
    @Override
    public void writeToOutput(ChannelWriterOutputView output) throws IOException {
        writeToOutput(output, null);
    }

    @Override
    public void writeToOutput(
            ChannelWriterOutputView output, LargeRecordHandler<T> largeRecordsOutput)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            if (largeRecordsOutput == null) {
                LOG.debug("Spilling sort buffer without large record handling.");
            } else {
                LOG.debug("Spilling sort buffer with large record handling.");
            }
        }

        final int numRecords = this.numRecords;
        int currentMemSeg = 0;
        int currentRecord = 0;

        while (currentRecord < numRecords) {
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);

            // go through all records in the memory segment
            for (int offset = 0;
                    currentRecord < numRecords && offset <= this.lastIndexEntryOffset;
                    currentRecord++, offset += this.indexEntrySize) {
                final long pointer = currentIndexSegment.getLong(offset);

                // small records go into the regular spill file, large records into the special code
                // path
                if (pointer >= 0 || largeRecordsOutput == null) {
                    this.recordBuffer.setReadPosition(pointer);
                    this.serializer.copy(this.recordBuffer, output);
                } else {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Spilling large record to large record fetch file.");
                    }

                    this.recordBuffer.setReadPosition(pointer & POINTER_MASK);
                    T record = this.serializer.deserialize(this.recordBuffer);
                    largeRecordsOutput.addRecord(record);
                }
            }
        }
    }

    /**
     * Writes a subset of the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @param start The logical start position of the subset.
     * @param num The number of elements to write.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     */
    @Override
    public void writeToOutput(final ChannelWriterOutputView output, final int start, int num)
            throws IOException {
        int currentMemSeg = start / this.indexEntriesPerSegment;
        int offset = (start % this.indexEntriesPerSegment) * this.indexEntrySize;

        while (num > 0) {
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);
            // check whether we have a full or partially full segment
            if (num >= this.indexEntriesPerSegment && offset == 0) {
                // full segment
                for (; offset <= this.lastIndexEntryOffset; offset += this.indexEntrySize) {
                    final long pointer = currentIndexSegment.getLong(offset) & POINTER_MASK;
                    this.recordBuffer.setReadPosition(pointer);
                    this.serializer.copy(this.recordBuffer, output);
                }
                num -= this.indexEntriesPerSegment;
            } else {
                // partially filled segment
                for (;
                        num > 0 && offset <= this.lastIndexEntryOffset;
                        num--, offset += this.indexEntrySize) {
                    final long pointer = currentIndexSegment.getLong(offset) & POINTER_MASK;
                    this.recordBuffer.setReadPosition(pointer);
                    this.serializer.copy(this.recordBuffer, output);
                }
            }
            offset = 0;
        }
    }
}
