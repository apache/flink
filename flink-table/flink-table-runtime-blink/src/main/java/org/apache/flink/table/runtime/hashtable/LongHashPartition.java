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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.BulkBlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.table.runtime.hashtable.BaseHybridHashTable.partitionLevelHash;
import static org.apache.flink.table.runtime.hashtable.LongHybridHashTable.hashLong;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Partition for {@link LongHybridHashTable}.
 *
 * <p>The layout of the buckets inside a memory segment is as follows:
 *
 * <p>Hash mode: +----------------------------- Bucket area ---------------------------- | long key
 * (8 bytes) | address (8 bytes) | | long key (8 bytes) | address (8 bytes) | | long key (8 bytes) |
 * address (8 bytes) | | ... +----------------------------- Data area -------------------------- |
 * size & address of next row with the same key (8bytes) | binary row | | size & address of next row
 * with the same key (8bytes) | binary row | | size & address of next row with the same key (8bytes)
 * | binary row | | ...
 *
 * <p>Dense mode: +----------------------------- Bucket area ---------------------------- | address1
 * (8 bytes) | address2 (8 bytes) | address3 (8 bytes) | ... Directly addressed by the index of the
 * corresponding array of key values.
 */
public class LongHashPartition extends AbstractPagedInputView implements SeekableDataInputView {

    private static final Logger LOG = LoggerFactory.getLogger(LongHashPartition.class);

    // The number of bits for size in address
    private static final int SIZE_BITS = 28;
    private static final int SIZE_MASK = 0xfffffff;

    // bucket element size in sparse mode: long key (8 bytes) + address pointer (8 bytes)
    private static final int SPARSE_BUCKET_ELEMENT_SIZE_IN_BYTES = 16;

    static final long INVALID_ADDRESS = 0x00000FFFFFFFFFL;

    private final LongHybridHashTable longTable;

    // segment size related properties
    private final int segmentSize;
    private final int segmentSizeBits;
    private final int segmentSizeMask;

    private int partitionNum;
    private final BinaryRowDataSerializer buildSideSerializer;
    private final BinaryRowData buildReuseRow;
    private int recursionLevel;

    // The minimum key
    private long minKey = Long.MAX_VALUE;

    // The maximum key
    private long maxKey = Long.MIN_VALUE;

    // The bucket area for this partition
    private MemorySegment[] buckets;
    private int numBuckets;
    private int numBucketsMask;

    // The in-memory data area for this partition
    private MemorySegment[] partitionBuffers;

    private int finalBufferLimit;
    private int currentBufferNum;
    private BuildSideBuffer buildSideWriteBuffer;
    AbstractChannelWriterOutputView probeSideBuffer;
    long probeSideRecordCounter; // number of probe-side records in this partition

    // The number of unique keys.
    private long numKeys;

    private final MatchIterator iterator;

    // the channel writer for the build side, if partition is spilled
    private BlockChannelWriter<MemorySegment> buildSideChannel;

    // number of build-side records in this partition
    private long buildSideRecordCounter;

    int probeNumBytesInLastSeg;

    /** Entrance 1: Init LongHashPartition for new insert and search. */
    LongHashPartition(
            LongHybridHashTable longTable,
            int partitionNum,
            BinaryRowDataSerializer buildSideSerializer,
            double estimatedRowCount,
            int maxSegs,
            int recursionLevel) {
        this(
                longTable,
                partitionNum,
                buildSideSerializer,
                getBucketBuffersByRowCount((long) estimatedRowCount, maxSegs, longTable.pageSize()),
                recursionLevel,
                null,
                0);
        this.buildSideWriteBuffer = new BuildSideBuffer(longTable.nextSegment());
    }

    /**
     * Entrance 2: build table from spilled partition when the partition fits entirely into main
     * memory.
     */
    LongHashPartition(
            LongHybridHashTable longTable,
            int partitionNum,
            BinaryRowDataSerializer buildSideSerializer,
            int bucketNumSegs,
            int recursionLevel,
            List<MemorySegment> buffers,
            int lastSegmentLimit) {
        this(longTable, buildSideSerializer, listToArray(buffers));
        this.partitionNum = partitionNum;
        this.recursionLevel = recursionLevel;

        int numBuckets = MathUtils.roundDownToPowerOf2(bucketNumSegs * segmentSize / 16);
        MemorySegment[] buckets = new MemorySegment[bucketNumSegs];
        for (int i = 0; i < bucketNumSegs; i++) {
            buckets[i] = longTable.nextSegment();
        }
        setNewBuckets(buckets, numBuckets);
        this.finalBufferLimit = lastSegmentLimit;
    }

    /**
     * Entrance 3: dense mode for just data search (bucket in LongHybridHashTable of dense mode).
     */
    LongHashPartition(
            LongHybridHashTable longTable,
            BinaryRowDataSerializer buildSideSerializer,
            MemorySegment[] partitionBuffers) {
        super(0);
        this.longTable = longTable;
        this.buildSideSerializer = buildSideSerializer;
        this.buildReuseRow = buildSideSerializer.createInstance();
        this.segmentSize = longTable.pageSize();
        Preconditions.checkArgument(segmentSize % 16 == 0);
        this.partitionBuffers = partitionBuffers;
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        this.segmentSizeMask = segmentSize - 1;
        this.finalBufferLimit = segmentSize;
        this.iterator = new MatchIterator();
    }

    private static MemorySegment[] listToArray(List<MemorySegment> list) {
        if (list != null) {
            return list.toArray(new MemorySegment[0]);
        }
        return null;
    }

    private static int getBucketBuffersByRowCount(long rowCount, int maxSegs, int segmentSize) {
        int minNumBuckets = (int) Math.ceil((rowCount / 0.5));
        Preconditions.checkArgument(segmentSize % 16 == 0);
        return MathUtils.roundDownToPowerOf2(
                (int)
                        Math.max(
                                1,
                                Math.min(
                                        maxSegs,
                                        Math.ceil(((double) minNumBuckets) * 16 / segmentSize))));
    }

    private void setNewBuckets(MemorySegment[] buckets, int numBuckets) {
        for (MemorySegment segment : buckets) {
            for (int i = 0; i < segmentSize; i += 16) {
                // Maybe we don't need init key, cause always verify address
                segment.putLong(i, 0);
                segment.putLong(i + 8, INVALID_ADDRESS);
            }
        }
        this.buckets = buckets;
        checkArgument(MathUtils.isPowerOf2(numBuckets));
        this.numBuckets = numBuckets;
        this.numBucketsMask = numBuckets - 1;
        this.numKeys = 0;
    }

    private static long toAddrAndLen(long address, int size) {
        return (address << SIZE_BITS) | size;
    }

    private static long toAddress(long addrAndLen) {
        return addrAndLen >>> SIZE_BITS;
    }

    private static int toLength(long addrAndLen) {
        return (int) (addrAndLen & SIZE_MASK);
    }

    /** Returns an iterator of BinaryRowData for multiple linked values. */
    MatchIterator valueIter(long address) {
        iterator.set(address);
        return iterator;
    }

    /** Returns an iterator for all the values for the given key, or null if no value found. */
    public MatchIterator get(long key, int hashCode) {
        int bucket = findBucket(hashCode);

        int bucketOffset = bucket << 4;
        MemorySegment segment = buckets[bucketOffset >>> segmentSizeBits];
        int segOffset = bucketOffset & segmentSizeMask;

        while (true) {
            long address = segment.getLong(segOffset + 8);
            if (address != INVALID_ADDRESS) {
                if (segment.getLong(segOffset) == key) {
                    return valueIter(address);
                } else {
                    bucket = (bucket + 1) & numBucketsMask;
                    if (segOffset + 16 < segmentSize) {
                        segOffset += 16;
                    } else {
                        bucketOffset = bucket << 4;
                        segOffset = bucketOffset & segmentSizeMask;
                        segment = buckets[bucketOffset >>> segmentSizeBits];
                    }
                }
            } else {
                return valueIter(INVALID_ADDRESS);
            }
        }
    }

    /** Update the address in array for given key. */
    private void updateIndex(
            long key,
            int hashCode,
            long address,
            int size,
            MemorySegment dataSegment,
            int currentPositionInSegment)
            throws IOException {
        assert (numKeys <= numBuckets / 2);
        int bucketId = findBucket(hashCode);

        // each bucket occupied 16 bytes (long key + long pointer to data address)
        int bucketOffset = bucketId * SPARSE_BUCKET_ELEMENT_SIZE_IN_BYTES;
        MemorySegment segment = buckets[bucketOffset >>> segmentSizeBits];
        int segOffset = bucketOffset & segmentSizeMask;
        long currAddress;

        while (true) {
            currAddress = segment.getLong(segOffset + 8);
            if (segment.getLong(segOffset) != key && currAddress != INVALID_ADDRESS) {
                // hash conflicts, the bucket is occupied by another key

                // TODO test Conflict resolution:
                // now:    +1 +1 +1... cache friendly but more conflict, so we set factor to 0.5
                // other1: +1 +2 +3... less conflict, factor can be 0.75
                // other2: Secondary hashCode... less and less conflict, but need compute hash again
                bucketId = (bucketId + 1) & numBucketsMask;
                if (segOffset + SPARSE_BUCKET_ELEMENT_SIZE_IN_BYTES < segmentSize) {
                    // if the new bucket still in current segment, we only need to update offset
                    // within this segment
                    segOffset += SPARSE_BUCKET_ELEMENT_SIZE_IN_BYTES;
                } else {
                    // otherwise, we should re-calculate segment and offset
                    bucketOffset = bucketId * 16;
                    segment = buckets[bucketOffset >>> segmentSizeBits];
                    segOffset = bucketOffset & segmentSizeMask;
                }
            } else {
                break;
            }
        }
        if (currAddress == INVALID_ADDRESS) {
            // this is the first value for this key, put the address in array.
            segment.putLong(segOffset, key);
            segment.putLong(segOffset + 8, address);
            numKeys += 1;
            // dataSegment may be null if we only have to rehash bucket area
            if (dataSegment != null) {
                dataSegment.putLong(currentPositionInSegment, toAddrAndLen(INVALID_ADDRESS, size));
            }
            if (numKeys * 2 > numBuckets) {
                resize();
            }
        } else {
            // there are some values for this key, put the address in the front of them.
            dataSegment.putLong(currentPositionInSegment, toAddrAndLen(currAddress, size));
            segment.putLong(segOffset + 8, address);
        }
    }

    private int findBucket(int hash) {
        return partitionLevelHash(hash) & this.numBucketsMask;
    }

    private void resize() throws IOException {
        MemorySegment[] oldBuckets = this.buckets;
        int oldNumBuckets = numBuckets;
        int newNumSegs = oldBuckets.length * 2;
        int newNumBuckets = MathUtils.roundDownToPowerOf2(newNumSegs * segmentSize / 16);

        // request new buckets.
        MemorySegment[] newBuckets = new MemorySegment[newNumSegs];
        for (int i = 0; i < newNumSegs; i++) {
            MemorySegment seg = longTable.getNextBuffer();
            if (seg == null) {
                final int spilledPart = longTable.spillPartition();
                if (spilledPart == partitionNum) {
                    // this bucket is no longer in-memory
                    // free new segments.
                    longTable.returnAll(Arrays.asList(newBuckets));
                    return;
                }
                seg = longTable.getNextBuffer();
                if (seg == null) {
                    throw new RuntimeException(
                            "Bug in HybridHashJoin: No memory became available after spilling a partition.");
                }
            }
            newBuckets[i] = seg;
        }

        setNewBuckets(newBuckets, newNumBuckets);
        reHash(oldBuckets, oldNumBuckets);
    }

    private void reHash(MemorySegment[] oldBuckets, int oldNumBuckets) throws IOException {
        long reHashStartTime = System.currentTimeMillis();
        int bucketOffset = 0;
        MemorySegment segment = oldBuckets[bucketOffset];
        int segOffset = 0;
        for (int i = 0; i < oldNumBuckets; i++) {
            long address = segment.getLong(segOffset + 8);
            if (address != INVALID_ADDRESS) {
                long key = segment.getLong(segOffset);
                // size/dataSegment/currentPositionInSegment should never be used.
                updateIndex(key, hashLong(key, recursionLevel), address, 0, null, 0);
            }

            // not last bucket, move to next.
            if (i != oldNumBuckets - 1) {
                if (segOffset + 16 < segmentSize) {
                    segOffset += 16;
                } else {
                    segment = oldBuckets[++bucketOffset];
                    segOffset = 0;
                }
            }
        }

        longTable.returnAll(Arrays.asList(oldBuckets));
        LOG.info(
                "The rehash take {} ms for {} segments",
                (System.currentTimeMillis() - reHashStartTime),
                numBuckets);
    }

    public MemorySegment[] getBuckets() {
        return buckets;
    }

    int getBuildSideBlockCount() {
        return this.partitionBuffers == null
                ? this.buildSideWriteBuffer.getBlockCount()
                : this.partitionBuffers.length;
    }

    int getProbeSideBlockCount() {
        return this.probeSideBuffer == null ? -1 : this.probeSideBuffer.getBlockCount();
    }

    BlockChannelWriter<MemorySegment> getBuildSideChannel() {
        return this.buildSideChannel;
    }

    int getPartitionNumber() {
        return this.partitionNum;
    }

    MemorySegment[] getPartitionBuffers() {
        return partitionBuffers;
    }

    int getRecursionLevel() {
        return this.recursionLevel;
    }

    int getNumOccupiedMemorySegments() {
        // either the number of memory segments, or one for spilling
        final int numPartitionBuffers =
                this.partitionBuffers != null
                        ? this.partitionBuffers.length
                        : this.buildSideWriteBuffer.getNumOccupiedMemorySegments();
        return numPartitionBuffers + buckets.length;
    }

    int spillPartition(
            IOManager ioAccess,
            FileIOChannel.ID targetChannel,
            LinkedBlockingQueue<MemorySegment> bufferReturnQueue)
            throws IOException {
        // sanity checks
        if (!isInMemory()) {
            throw new RuntimeException(
                    "Bug in Hybrid Hash Join: "
                            + "Request to spill a partition that has already been spilled.");
        }
        if (getNumOccupiedMemorySegments() < 2) {
            throw new RuntimeException(
                    "Bug in Hybrid Hash Join: "
                            + "Request to spill a partition with less than two buffers.");
        }

        // create the channel block writer and spill the current buffers
        // that keep the build side buffers current block, as it is most likely not full, yet
        // we return the number of blocks that become available
        this.buildSideChannel =
                FileChannelUtil.createBlockChannelWriter(
                        ioAccess,
                        targetChannel,
                        bufferReturnQueue,
                        longTable.compressionEnable(),
                        longTable.compressionCodecFactory(),
                        longTable.compressionBlockSize(),
                        segmentSize);
        return this.buildSideWriteBuffer.spill(this.buildSideChannel);
    }

    /**
     * After build phase.
     *
     * @return build spill return buffer, if have spilled, it returns the current write buffer,
     *     because it was used all the time in build phase, so it can only be returned at this time.
     */
    int finalizeBuildPhase(IOManager ioAccess, FileIOChannel.Enumerator probeChannelEnumerator)
            throws IOException {
        this.finalBufferLimit = this.buildSideWriteBuffer.getCurrentPositionInSegment();
        this.partitionBuffers = this.buildSideWriteBuffer.close();

        if (!isInMemory()) {
            // close the channel.
            this.buildSideChannel.close();

            this.probeSideBuffer =
                    FileChannelUtil.createOutputView(
                            ioAccess,
                            probeChannelEnumerator.next(),
                            longTable.compressionEnable(),
                            longTable.compressionCodecFactory(),
                            longTable.compressionBlockSize(),
                            segmentSize);
            return 1;
        } else {
            return 0;
        }
    }

    void finalizeProbePhase(List<LongHashPartition> spilledPartitions) throws IOException {
        if (isInMemory()) {
            releaseBuckets();
            longTable.returnAll(Arrays.asList(partitionBuffers));
            this.partitionBuffers = null;
        } else {
            if (this.probeSideRecordCounter == 0) {
                // delete the spill files
                this.probeSideBuffer.close();
                this.buildSideChannel.deleteChannel();
                this.probeSideBuffer.getChannel().deleteChannel();
            } else {
                // flush the last probe side buffer and register this partition as pending
                probeNumBytesInLastSeg = this.probeSideBuffer.close();
                spilledPartitions.add(this);
            }
        }
    }

    final PartitionIterator newPartitionIterator() {
        return new PartitionIterator();
    }

    final int getLastSegmentLimit() {
        return this.finalBufferLimit;
    }

    // ------------------ PagedInputView for read --------------------

    @Override
    public void setReadPosition(long pointer) {
        final int bufferNum = (int) (pointer >>> this.segmentSizeBits);
        final int offset = (int) (pointer & segmentSizeMask);

        this.currentBufferNum = bufferNum;

        seekInput(
                this.partitionBuffers[bufferNum],
                offset,
                bufferNum < partitionBuffers.length - 1 ? segmentSize : finalBufferLimit);
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
        this.currentBufferNum++;
        if (this.currentBufferNum < this.partitionBuffers.length) {
            return this.partitionBuffers[this.currentBufferNum];
        } else {
            throw new EOFException();
        }
    }

    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return segment == partitionBuffers[partitionBuffers.length - 1]
                ? finalBufferLimit
                : segmentSize;
    }

    boolean isInMemory() {
        return buildSideChannel == null;
    }

    final void insertIntoProbeBuffer(BinaryRowDataSerializer probeSer, BinaryRowData record)
            throws IOException {
        probeSer.serialize(record, this.probeSideBuffer);
        this.probeSideRecordCounter++;
    }

    long getBuildSideRecordCount() {
        return buildSideRecordCounter;
    }

    long getMinKey() {
        return minKey;
    }

    long getMaxKey() {
        return maxKey;
    }

    private void updateMinMax(long key) {
        if (key < minKey) {
            minKey = key;
        }
        if (key > maxKey) {
            maxKey = key;
        }
    }

    void insertIntoBucket(long key, int hashCode, int size, long address) throws IOException {
        this.buildSideRecordCounter++;
        updateMinMax(key);

        final int bufferNum = (int) (address >>> this.segmentSizeBits);
        final int offset = (int) (address & (this.segmentSize - 1));
        updateIndex(key, hashCode, address, size, partitionBuffers[bufferNum], offset);
    }

    void insertIntoTable(long key, int hashCode, BinaryRowData row) throws IOException {
        this.buildSideRecordCounter++;
        updateMinMax(key);
        int sizeInBytes = row.getSizeInBytes();
        if (sizeInBytes >= (1 << SIZE_BITS)) {
            throw new UnsupportedOperationException(
                    "Does not support row that is larger than 256M");
        }
        if (isInMemory()) {
            checkWriteAdvance();
            // after advance, we may run out memory and spill this partition, check still in memory
            // again
            if (isInMemory()) {
                updateIndex(
                        key,
                        hashCode,
                        buildSideWriteBuffer.getCurrentPointer(),
                        sizeInBytes,
                        buildSideWriteBuffer.getCurrentSegment(),
                        buildSideWriteBuffer.getCurrentPositionInSegment());
            } else {
                buildSideWriteBuffer
                        .getCurrentSegment()
                        .putLong(
                                buildSideWriteBuffer.getCurrentPositionInSegment(),
                                toAddrAndLen(INVALID_ADDRESS, sizeInBytes));
            }

            buildSideWriteBuffer.skipBytesToWrite(8);
            if (row.getSegments().length == 1) {
                buildSideWriteBuffer.write(row.getSegments()[0], row.getOffset(), sizeInBytes);
            } else {
                BinaryRowDataSerializer.serializeWithoutLengthSlow(row, buildSideWriteBuffer);
            }
        } else {
            serializeToPages(row);
        }
    }

    public void serializeToPages(BinaryRowData row) throws IOException {

        int sizeInBytes = row.getSizeInBytes();
        checkWriteAdvance();

        buildSideWriteBuffer
                .getCurrentSegment()
                .putLong(
                        buildSideWriteBuffer.getCurrentPositionInSegment(),
                        toAddrAndLen(INVALID_ADDRESS, row.getSizeInBytes()));
        buildSideWriteBuffer.skipBytesToWrite(8);

        if (row.getSegments().length == 1) {
            buildSideWriteBuffer.write(row.getSegments()[0], row.getOffset(), sizeInBytes);
        } else {
            BinaryRowDataSerializer.serializeWithoutLengthSlow(row, buildSideWriteBuffer);
        }
    }

    void releaseBuckets() {
        if (buckets != null) {
            longTable.returnAll(Arrays.asList(buckets));
            buckets = null;
        }
    }

    // ------------------ PagedInputView for read end --------------------

    /** Write Buffer. */
    private class BuildSideBuffer extends AbstractPagedOutputView {

        private final ArrayList<MemorySegment> targetList;
        private int currentBlockNumber;
        private BlockChannelWriter<MemorySegment> writer;

        private BuildSideBuffer(MemorySegment segment) {
            super(segment, segment.size(), 0);
            this.targetList = new ArrayList<>();
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
                throws IOException {
            final MemorySegment next;
            if (this.writer == null) {
                // Must first add current segment:
                // This may happen when you need to spill:
                // A partition called nextSegment, can not get memory, need to spill, the result
                // give itself to the spill, Since it is switching currentSeg, it is necessary
                // to give the previous currSeg to spill.
                this.targetList.add(current);
                next = longTable.nextSegment();
            } else {
                this.writer.writeBlock(current);
                try {
                    next = this.writer.getReturnQueue().take();
                } catch (InterruptedException iex) {
                    throw new IOException(
                            "Hash Join Partition was interrupted while "
                                    + "grabbing a new write-behind buffer.");
                }
            }

            this.currentBlockNumber++;
            return next;
        }

        long getCurrentPointer() {
            return (((long) this.currentBlockNumber) << segmentSizeBits)
                    + getCurrentPositionInSegment();
        }

        int getBlockCount() {
            return this.currentBlockNumber + 1;
        }

        int getNumOccupiedMemorySegments() {
            // return the current segment + all filled segments
            return this.targetList.size() + 1;
        }

        int spill(BlockChannelWriter<MemorySegment> writer) throws IOException {
            this.writer = writer;
            final int numSegments = this.targetList.size();
            for (MemorySegment segment : this.targetList) {
                this.writer.writeBlock(segment);
            }
            this.targetList.clear();
            return numSegments;
        }

        MemorySegment[] close() throws IOException {
            final MemorySegment current = getCurrentSegment();
            if (current == null) {
                throw new IllegalStateException(
                        "Illegal State in LongHashTable: "
                                + "No current buffer when finalizing build side.");
            }
            clear();

            if (this.writer == null) {
                this.targetList.add(current);
                MemorySegment[] buffers = this.targetList.toArray(new MemorySegment[0]);
                this.targetList.clear();
                return buffers;
            } else {
                writer.writeBlock(current);
                return null;
            }
        }
    }

    /** Iterator for probe match. */
    public class MatchIterator implements RowIterator<BinaryRowData> {
        private long address;

        public void set(long address) {
            this.address = address;
        }

        @Override
        public boolean advanceNext() {
            if (address != INVALID_ADDRESS) {
                setReadPosition(address);
                long addrAndLen = getCurrentSegment().getLong(getCurrentPositionInSegment());
                this.address = toAddress(addrAndLen);
                int size = toLength(addrAndLen);
                try {
                    skipBytesToRead(8);
                    buildSideSerializer.pointTo(size, buildReuseRow, LongHashPartition.this);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            return false;
        }

        @Override
        public BinaryRowData getRow() {
            return buildReuseRow;
        }
    }

    void clearAllMemory(LazyMemorySegmentPool pool) {
        // return current buffers from build side and probe side
        if (this.buildSideWriteBuffer != null) {
            if (this.buildSideWriteBuffer.getCurrentSegment() != null) {
                pool.returnPage(this.buildSideWriteBuffer.getCurrentSegment());
            }
            pool.returnAll(this.buildSideWriteBuffer.targetList);
            this.buildSideWriteBuffer.targetList.clear();
            this.buildSideWriteBuffer = null;
        }
        releaseBuckets();

        // return the partition buffers
        if (this.partitionBuffers != null) {
            pool.returnAll(Arrays.asList(this.partitionBuffers));
            this.partitionBuffers = null;
        }

        // clear the channels
        try {
            if (this.buildSideChannel != null) {
                this.buildSideChannel.close();
                this.buildSideChannel.deleteChannel();
            }
            if (this.probeSideBuffer != null) {
                this.probeSideBuffer.getChannel().closeAndDelete();
                this.probeSideBuffer = null;
            }
        } catch (IOException ioex) {
            throw new RuntimeException(
                    "Error deleting the partition files. "
                            + "Some temporary files might not be removed.",
                    ioex);
        }
    }

    /**
     * For spilled partition to rebuild index and hashcode when memory can store all the build side
     * data. (After bulk load to memory, see {@link BulkBlockChannelReader}).
     */
    final class PartitionIterator implements RowIterator<BinaryRowData> {

        private long currentPointer;

        private BinaryRowData reuse;

        private PartitionIterator() {
            this.reuse = buildSideSerializer.createInstance();
            setReadPosition(0);
        }

        @Override
        public boolean advanceNext() {
            try {
                checkReadAdvance();

                int pos = getCurrentPositionInSegment();
                this.currentPointer = (((long) currentBufferNum) << segmentSizeBits) + pos;

                long addrAndLen = getCurrentSegment().getLong(pos);
                skipBytesToRead(8);
                buildSideSerializer.pointTo(toLength(addrAndLen), reuse, LongHashPartition.this);
                return true;
            } catch (EOFException e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        final long getPointer() {
            return this.currentPointer;
        }

        @Override
        public BinaryRowData getRow() {
            return this.reuse;
        }
    }

    private void checkWriteAdvance() throws IOException {
        if (shouldAdvance(
                buildSideWriteBuffer.getSegmentSize()
                        - buildSideWriteBuffer.getCurrentPositionInSegment(),
                buildSideSerializer)) {
            buildSideWriteBuffer.advance();
        }
    }

    private void checkReadAdvance() throws IOException {
        if (shouldAdvance(
                getCurrentSegmentLimit() - getCurrentPositionInSegment(), buildSideSerializer)) {
            advance();
        }
    }

    private static boolean shouldAdvance(int available, BinaryRowDataSerializer serializer) {
        return available < 8 + serializer.getFixedLengthPartSize();
    }

    static void deserializeFromPages(
            BinaryRowData reuse,
            ChannelReaderInputView inView,
            BinaryRowDataSerializer buildSideSerializer)
            throws IOException {
        if (shouldAdvance(
                inView.getCurrentSegmentLimit() - inView.getCurrentPositionInSegment(),
                buildSideSerializer)) {
            inView.advance();
        }
        MemorySegment segment = (reuse.getSegments() != null) ? reuse.getSegments()[0] : null;

        int length =
                toLength(inView.getCurrentSegment().getLong(inView.getCurrentPositionInSegment()));
        inView.skipBytesToRead(8);

        if (segment == null || segment.size() < length) {
            segment = MemorySegmentFactory.wrap(new byte[length]);
        }
        inView.readFully(segment.getHeapMemory(), 0, length);
        reuse.pointTo(segment, 0, length);
    }

    void iteratorToDenseBucket(
            MemorySegment[] denseBuckets, long addressOffset, long globalMinKey) {
        int bucketOffset = 0;
        MemorySegment segment = buckets[bucketOffset];
        int segOffset = 0;
        for (int i = 0; i < numBuckets; i++) {
            long address = segment.getLong(segOffset + 8);
            if (address != INVALID_ADDRESS) {
                long key = segment.getLong(segOffset);
                long denseBucket = key - globalMinKey;
                long denseBucketOffset = denseBucket << 3;
                int denseSegIndex = (int) (denseBucketOffset >>> segmentSizeBits);
                int denseSegOffset = (int) (denseBucketOffset & segmentSizeMask);
                denseBuckets[denseSegIndex].putLong(denseSegOffset, address + addressOffset);
            }

            // not last bucket, move to next.
            if (i != numBuckets - 1) {
                if (segOffset + 16 < segmentSize) {
                    segOffset += 16;
                } else {
                    segment = buckets[++bucketOffset];
                    segOffset = 0;
                }
            }
        }
    }

    void updateDenseAddressOffset(long addressOffset) {
        if (addressOffset != 0) {
            setReadPosition(0);
            while (true) {
                try {
                    checkReadAdvance();
                    long addrAndLen = getCurrentSegment().getLong(getCurrentPositionInSegment());
                    long address = LongHashPartition.toAddress(addrAndLen);
                    int len = LongHashPartition.toLength(addrAndLen);
                    if (address != INVALID_ADDRESS) {
                        getCurrentSegment()
                                .putLong(
                                        getCurrentPositionInSegment(),
                                        LongHashPartition.toAddrAndLen(
                                                address + addressOffset, len));
                    }
                    skipBytesToRead(8 + len);
                } catch (EOFException e) {
                    break;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
