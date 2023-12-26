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
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.io.LongHashPartitionChannelReaderInputViewIterator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.MathUtils;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.runtime.hashtable.LongHashPartition.INVALID_ADDRESS;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special optimized hashTable with key long.
 *
 * <p>See {@link LongHashPartition}. TODO add min max long filter and bloomFilter to spilled
 * partition.
 */
public abstract class LongHybridHashTable extends BaseHybridHashTable {

    private final BinaryRowDataSerializer buildSideSerializer;
    private final BinaryRowDataSerializer probeSideSerializer;
    private final ArrayList<LongHashPartition> partitionsBeingBuilt;
    private final ArrayList<LongHashPartition> partitionsPending;
    /**
     * The partitions that have been spilled previously and are pending to be processed by sort
     * merge join operator.
     */
    private final List<LongHashPartition> partitionsPendingForSMJ;

    private ProbeIterator probeIterator;
    private LongHashPartition.MatchIterator matchIterator;

    private boolean denseMode = false;
    private long minKey;
    private long maxKey;
    private MemorySegment[] denseBuckets;
    private LongHashPartition densePartition;
    private LongHashPartition currentProbePartition;

    public LongHybridHashTable(
            Object owner,
            boolean compressionEnabled,
            int compressionBlockSize,
            BinaryRowDataSerializer buildSideSerializer,
            BinaryRowDataSerializer probeSideSerializer,
            MemoryManager memManager,
            long reservedMemorySize,
            IOManager ioManager,
            int avgRecordLen,
            long buildRowCount) {
        super(
                owner,
                compressionEnabled,
                compressionBlockSize,
                memManager,
                reservedMemorySize,
                ioManager,
                avgRecordLen,
                buildRowCount,
                false);
        this.buildSideSerializer = buildSideSerializer;
        this.probeSideSerializer = probeSideSerializer;

        this.partitionsBeingBuilt = new ArrayList<>();
        this.partitionsPending = new ArrayList<>();
        this.partitionsPendingForSMJ = new ArrayList<>();

        createPartitions(initPartitionFanOut, 0);
    }

    // ---------------------- interface to join operator ---------------------------------------

    public void putBuildRow(BinaryRowData row) throws IOException {
        long key = getBuildLongKey(row);
        final int hashCode = hashLong(key, 0);
        insertIntoTable(key, hashCode, row);
    }

    public void endBuild() throws IOException {
        int buildWriteBuffers = 0;
        for (LongHashPartition p : this.partitionsBeingBuilt) {
            buildWriteBuffers += p.finalizeBuildPhase(this.ioManager, this.currentEnumerator);
        }
        buildSpillRetBufferNumbers += buildWriteBuffers;

        // the first prober is the probe-side input, but the input is null at beginning
        this.probeIterator = new ProbeIterator(this.probeSideSerializer.createInstance());

        tryDenseMode();
    }

    /**
     * This method is only used for operator fusion codegen to get build row from hash table. If the
     * build partition has spilled to disk, return null directly which requires the join operator
     * also spill probe row to disk.
     */
    public final @Nullable RowIterator<BinaryRowData> get(long probeKey) throws IOException {
        if (denseMode) {
            if (probeKey >= minKey && probeKey <= maxKey) {
                long denseBucket = probeKey - minKey;
                long denseBucketOffset = denseBucket << 3;
                int denseSegIndex = (int) (denseBucketOffset >>> segmentSizeBits);
                int denseSegOffset = (int) (denseBucketOffset & segmentSizeMask);

                long address = denseBuckets[denseSegIndex].getLong(denseSegOffset);
                this.matchIterator = densePartition.valueIter(address);
            } else {
                this.matchIterator = densePartition.valueIter(INVALID_ADDRESS);
            }

            return matchIterator;
        } else {
            final int hash = hashLong(probeKey, this.currentRecursionDepth);
            currentProbePartition =
                    this.partitionsBeingBuilt.get(hash % partitionsBeingBuilt.size());
            if (currentProbePartition.isInMemory()) {
                this.matchIterator = currentProbePartition.get(probeKey, hash);
                return matchIterator;
            } else {
                // If the build partition has spilled to disk, return null directly which requires
                // the join operator also spill probe row to disk.
                return null;
            }
        }
    }

    /**
     * If the probe row corresponding partition has been spilled to disk, just call this method
     * spill probe row to disk.
     *
     * <p>Note: This must be called only after {@link LongHybridHashTable#get} method.
     */
    public final void insertIntoProbeBuffer(RowData probeRecord) throws IOException {
        checkNotNull(currentProbePartition);
        currentProbePartition.insertIntoProbeBuffer(
                probeSideSerializer, probeToBinary(probeRecord));
    }

    public boolean tryProbe(RowData record) throws IOException {
        long probeKey = getProbeLongKey(record);

        if (denseMode) {
            this.probeIterator.setInstance(record);

            if (probeKey >= minKey && probeKey <= maxKey) {
                long denseBucket = probeKey - minKey;
                long denseBucketOffset = denseBucket << 3;
                int denseSegIndex = (int) (denseBucketOffset >>> segmentSizeBits);
                int denseSegOffset = (int) (denseBucketOffset & segmentSizeMask);

                long address = denseBuckets[denseSegIndex].getLong(denseSegOffset);
                this.matchIterator = densePartition.valueIter(address);
            } else {
                this.matchIterator = densePartition.valueIter(INVALID_ADDRESS);
            }

            return true;
        } else {
            if (!this.probeIterator.hasSource()) {
                // set the current probe value when probeIterator is null at the begging.
                this.probeIterator.setInstance(record);
            }
            final int hash = hashLong(probeKey, this.currentRecursionDepth);

            LongHashPartition p = this.partitionsBeingBuilt.get(hash % partitionsBeingBuilt.size());

            // for an in-memory partition, process set the return iterators, else spill the probe
            // records
            if (p.isInMemory()) {
                this.matchIterator = p.get(probeKey, hash);
                return true;
            } else {
                p.insertIntoProbeBuffer(probeSideSerializer, probeToBinary(record));
                return false;
            }
        }
    }

    public boolean nextMatching() throws IOException {
        return !denseMode && (processProbeIter() || prepareNextPartition());
    }

    public RowData getCurrentProbeRow() {
        return this.probeIterator.current();
    }

    public LongHashPartition.MatchIterator getBuildSideIterator() {
        return matchIterator;
    }

    @Override
    public void close() {
        if (denseMode) {
            closed.compareAndSet(false, true);
        } else {
            super.close();
        }
    }

    @Override
    public void free() {
        if (denseMode) {
            returnAll(Arrays.asList(denseBuckets));
            returnAll(Arrays.asList(densePartition.getPartitionBuffers()));
        }
        super.free();
    }

    // ---------------------- interface to join operator end -----------------------------------

    /** After build end, try to use dense mode. */
    private void tryDenseMode() {
        // if some partitions have spilled to disk, always use hash mode
        if (numSpillFiles != 0) {
            return;
        }

        long minKey = Long.MAX_VALUE;
        long maxKey = Long.MIN_VALUE;
        long recordCount = 0;
        for (LongHashPartition p : this.partitionsBeingBuilt) {
            long partitionRecords = p.getBuildSideRecordCount();
            recordCount += partitionRecords;
            if (partitionRecords > 0) {
                if (p.getMinKey() < minKey) {
                    minKey = p.getMinKey();
                }
                if (p.getMaxKey() > maxKey) {
                    maxKey = p.getMaxKey();
                }
            }
        }

        if (buildSpillRetBufferNumbers != 0) {
            throw new RuntimeException(
                    "buildSpillRetBufferNumbers should be 0: " + buildSpillRetBufferNumbers);
        }

        long range = maxKey - minKey + 1;

        // 1.range is negative mean: range is too big to overflow
        // 2.range is zero, maybe the max is Long.Max, and the min is Long.Min,
        // so we should not use dense mode too.
        if (range > 0 && (range <= recordCount * 4 || range <= segmentSize / 8)) {

            // try to request memory.
            int buffers = (int) Math.ceil(((double) (range * 8)) / segmentSize);

            // TODO MemoryManager needs to support flexible larger segment, so that the index area
            // of the build side is placed on a segment to avoid the overhead of addressing.
            MemorySegment[] denseBuckets = new MemorySegment[buffers];
            for (int i = 0; i < buffers; i++) {
                MemorySegment seg = getNextBuffer();
                if (seg == null) {
                    returnAll(Arrays.asList(denseBuckets));
                    return;
                }
                denseBuckets[i] = seg;
                for (int j = 0; j < segmentSize; j += 8) {
                    seg.putLong(j, INVALID_ADDRESS);
                }
            }

            denseMode = true;
            LOG.info("LongHybridHashTable: Use dense mode!");
            this.minKey = minKey;
            this.maxKey = maxKey;
            List<MemorySegment> segments = new ArrayList<>();
            buildSpillReturnBuffers.drainTo(segments);
            returnAll(segments);

            ArrayList<MemorySegment> dataBuffers = new ArrayList<>();

            long addressOffset = 0;
            for (LongHashPartition p : this.partitionsBeingBuilt) {
                p.iteratorToDenseBucket(denseBuckets, addressOffset, minKey);
                p.updateDenseAddressOffset(addressOffset);

                dataBuffers.addAll(Arrays.asList(p.getPartitionBuffers()));
                addressOffset += (p.getPartitionBuffers().length << segmentSizeBits);
                returnAll(Arrays.asList(p.getBuckets()));
            }

            this.denseBuckets = denseBuckets;
            this.densePartition =
                    new LongHashPartition(
                            this, buildSideSerializer, dataBuffers.toArray(new MemorySegment[0]));
            freeCurrent();
        }
    }

    private void createPartitions(int numPartitions, int recursionLevel) {
        ensureNumBuffersReturned(numPartitions);

        this.currentEnumerator = this.ioManager.createChannelEnumerator();

        this.partitionsBeingBuilt.clear();
        double numRecordPerPartition = (double) buildRowCount / numPartitions;
        int maxBuffer = maxInitBufferOfBucketArea(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            LongHashPartition p =
                    new LongHashPartition(
                            this,
                            i,
                            buildSideSerializer,
                            numRecordPerPartition,
                            maxBuffer,
                            recursionLevel);
            this.partitionsBeingBuilt.add(p);
        }
    }

    /** For code gen get build side long key. */
    public abstract long getBuildLongKey(RowData row);

    /** For code gen get probe side long key. */
    public abstract long getProbeLongKey(RowData row);

    /** For code gen probe side to BinaryRowData. */
    public abstract BinaryRowData probeToBinary(RowData row);

    private void insertIntoTable(long key, int hashCode, BinaryRowData row) throws IOException {
        LongHashPartition p = partitionsBeingBuilt.get(hashCode % partitionsBeingBuilt.size());
        p.insertIntoTable(key, hashCode, row);
    }

    static int hashLong(long key, int level) {
        long h = key * 0x9E3779B9L;
        int hash = (int) (h ^ (h >> 32));
        return BaseHybridHashTable.hash(hash, level);
    }

    private boolean processProbeIter() throws IOException {

        // the prober's source is null at the begging.
        if (this.probeIterator.hasSource()) {
            final ProbeIterator probeIter = this.probeIterator;

            BinaryRowData next;
            while ((next = probeIter.next()) != null) {
                long probeKey = getProbeLongKey(next);
                final int hash = hashLong(probeKey, this.currentRecursionDepth);

                final LongHashPartition p =
                        this.partitionsBeingBuilt.get(hash % partitionsBeingBuilt.size());

                // for an in-memory partition, process set the return iterators, else spill the
                // probe records
                if (p.isInMemory()) {
                    this.matchIterator = p.get(probeKey, hash);
                    return true;
                } else {
                    p.insertIntoProbeBuffer(probeSideSerializer, next);
                }
            }

            return false;
        } else {
            return false;
        }
    }

    private boolean prepareNextPartition() throws IOException {
        // finalize and cleanup the partitions of the current table
        for (final LongHashPartition p : this.partitionsBeingBuilt) {
            p.finalizeProbePhase(this.partitionsPending);
        }

        this.partitionsBeingBuilt.clear();

        if (this.currentSpilledProbeSide != null) {
            this.currentSpilledProbeSide.getChannel().closeAndDelete();
            this.currentSpilledProbeSide = null;
        }

        if (this.partitionsPending.isEmpty()) {
            // no more data
            return false;
        }

        // there are pending partitions
        final LongHashPartition p = this.partitionsPending.get(0);
        LOG.info(String.format("Begin to process spilled partition [%d]", p.getPartitionNumber()));

        if (p.probeSideRecordCounter == 0) {
            this.partitionsPending.remove(0);
            return prepareNextPartition();
        }

        final int nextRecursionLevel = p.getRecursionLevel() + 1;
        if (nextRecursionLevel == 2) {
            LOG.info("Recursive hash join: partition number is " + p.getPartitionNumber());
        } else if (nextRecursionLevel > MAX_RECURSION_DEPTH) {
            LOG.info(
                    "Partition number [{}] recursive level more than {}, process the partition using SortMergeJoin later.",
                    p.getPartitionNumber(),
                    MAX_RECURSION_DEPTH);
            // if the partition has spilled to disk more than three times, process it by sort merge
            // join later
            this.partitionsPendingForSMJ.add(p);
            // also need to remove it from pending list
            this.partitionsPending.remove(0);
            // recursively get the next partition
            return prepareNextPartition();
        }

        // build the next table; memory must be allocated after this call
        buildTableFromSpilledPartition(p, nextRecursionLevel);

        // set the probe side
        setPartitionProbeReader(p);

        // unregister the pending partition
        this.partitionsPending.remove(0);
        this.currentRecursionDepth = p.getRecursionLevel() + 1;

        // recursively get the next
        return nextMatching();
    }

    private void setPartitionProbeReader(LongHashPartition p) throws IOException {
        ChannelWithMeta channelWithMeta =
                new ChannelWithMeta(
                        p.probeSideBuffer.getChannel().getChannelID(),
                        p.probeSideBuffer.getBlockCount(),
                        p.probeNumBytesInLastSeg);
        this.currentSpilledProbeSide =
                FileChannelUtil.createInputView(
                        ioManager,
                        channelWithMeta,
                        new ArrayList<>(),
                        compressionEnabled,
                        compressionCodecFactory,
                        compressionBlockSize,
                        segmentSize);

        ChannelReaderInputViewIterator<BinaryRowData> probeReader =
                new ChannelReaderInputViewIterator(
                        this.currentSpilledProbeSide, new ArrayList<>(), this.probeSideSerializer);
        this.probeIterator.set(probeReader);
        this.probeIterator.setReuse(probeSideSerializer.createInstance());
    }

    private void buildTableFromSpilledPartition(
            final LongHashPartition p, final int nextRecursionLevel) throws IOException {
        if (p.getBuildSideBlockCount() > p.getProbeSideBlockCount()) {
            LOG.info(
                    String.format(
                            "Hash join: Partition(%d) "
                                    + "build side block [%d] more than probe side block [%d]",
                            p.getPartitionNumber(),
                            p.getBuildSideBlockCount(),
                            p.getProbeSideBlockCount()));
        }

        // we distinguish two cases here:
        // 1) The partition fits entirely into main memory. That is the case if we have enough
        // buffers for
        //    all partition segments, plus enough buffers to hold the table structure.
        //    --> We read the partition in as it is and create a hashtable that references only
        //        that single partition.
        // 2) We can not guarantee that enough memory segments are available and read the partition
        //    in, distributing its data among newly created partitions.
        final int totalBuffersAvailable =
                this.internalPool.freePages() + this.buildSpillRetBufferNumbers;
        if (totalBuffersAvailable != this.totalNumBuffers) {
            throw new RuntimeException(
                    String.format(
                            "Hash Join bug in memory management: Memory buffers leaked."
                                    + " availableMemory(%s), buildSpillRetBufferNumbers(%s), reservedNumBuffers(%s)",
                            this.internalPool.freePages(),
                            buildSpillRetBufferNumbers,
                            totalNumBuffers));
        }

        int maxBucketAreaBuffers =
                MathUtils.roundUpToPowerOfTwo(
                        (int)
                                Math.max(
                                        1,
                                        Math.ceil(
                                                Math.ceil((p.getBuildSideRecordCount() / 0.5))
                                                        * 16
                                                        / segmentSize)));

        final long totalBuffersNeeded = maxBucketAreaBuffers + p.getBuildSideBlockCount() + 2;

        if (totalBuffersNeeded < totalBuffersAvailable) {
            LOG.info(
                    String.format(
                            "Build in memory hash table from spilled partition [%d]",
                            p.getPartitionNumber()));

            // first read the partition in
            final List<MemorySegment> partitionBuffers =
                    readAllBuffers(
                            p.getBuildSideChannel().getChannelID(), p.getBuildSideBlockCount());
            final LongHashPartition newPart =
                    new LongHashPartition(
                            this,
                            0,
                            this.buildSideSerializer,
                            maxBucketAreaBuffers,
                            nextRecursionLevel,
                            partitionBuffers,
                            p.getLastSegmentLimit());

            this.partitionsBeingBuilt.add(newPart);

            // now, index the partition through a hash table
            final LongHashPartition.PartitionIterator pIter = newPart.newPartitionIterator();

            while (pIter.advanceNext()) {
                long key = getBuildLongKey(pIter.getRow());
                final int hashCode = hashLong(key, nextRecursionLevel);
                final int pointer = (int) pIter.getPointer();
                newPart.insertIntoBucket(key, hashCode, pIter.getRow().getSizeInBytes(), pointer);
            }
        } else {
            // go over the complete input and insert every element into the hash table
            // compute in how many splits, we'd need to partition the result
            final int splits = (int) (totalBuffersNeeded / totalBuffersAvailable) + 1;
            final int partitionFanOut =
                    Math.min(Math.min(10 * splits, MAX_NUM_PARTITIONS), maxNumPartition());

            createPartitions(partitionFanOut, nextRecursionLevel);
            LOG.info(
                    String.format(
                            "Build hybrid hash table from spilled partition [%d] with recursion level [%d]",
                            p.getPartitionNumber(), nextRecursionLevel));

            ChannelReaderInputView inView =
                    createInputView(
                            p.getBuildSideChannel().getChannelID(),
                            p.getBuildSideBlockCount(),
                            p.getLastSegmentLimit());
            BinaryRowData rec = this.buildSideSerializer.createInstance();
            while (true) {
                try {
                    LongHashPartition.deserializeFromPages(rec, inView, buildSideSerializer);
                    long key = getBuildLongKey(rec);
                    insertIntoTable(key, hashLong(key, nextRecursionLevel), rec);
                } catch (EOFException e) {
                    break;
                }
            }
            inView.getChannel().closeAndDelete();

            // finalize the partitions
            int buildWriteBuffers = 0;
            for (LongHashPartition part : this.partitionsBeingBuilt) {
                buildWriteBuffers +=
                        part.finalizeBuildPhase(this.ioManager, this.currentEnumerator);
            }
            buildSpillRetBufferNumbers += buildWriteBuffers;
        }
    }

    @Override
    public int spillPartition() throws IOException {
        // find the largest partition
        int largestNumBlocks = 0;
        int largestPartNum = -1;

        for (int i = 0; i < partitionsBeingBuilt.size(); i++) {
            LongHashPartition p = partitionsBeingBuilt.get(i);
            if (p.isInMemory() && p.getNumOccupiedMemorySegments() > largestNumBlocks) {
                largestNumBlocks = p.getNumOccupiedMemorySegments();
                largestPartNum = i;
            }
        }
        final LongHashPartition p = partitionsBeingBuilt.get(largestPartNum);

        // spill the partition
        int numBuffersFreed =
                p.spillPartition(
                        this.ioManager,
                        this.currentEnumerator.next(),
                        this.buildSpillReturnBuffers);
        p.releaseBuckets();
        this.buildSpillRetBufferNumbers += numBuffersFreed;

        LOG.info(
                String.format(
                        "Grace hash join: Ran out memory, choosing partition "
                                + "[%d] to spill, %d memory segments being freed",
                        largestPartNum, numBuffersFreed));

        // grab as many buffers as are available directly
        MemorySegment currBuff;
        while (this.buildSpillRetBufferNumbers > 0
                && (currBuff = this.buildSpillReturnBuffers.poll()) != null) {
            returnPage(currBuff);
            this.buildSpillRetBufferNumbers--;
        }
        numSpillFiles++;
        spillInBytes += numBuffersFreed * segmentSize;
        return largestPartNum;
    }

    public List<LongHashPartition> getPartitionsPendingForSMJ() {
        return this.partitionsPendingForSMJ;
    }

    public RowIterator getSpilledPartitionBuildSideIter(LongHashPartition p) throws IOException {
        // close build side channel of last processed partition
        if (this.currentSpilledBuildSide != null) {
            try {
                this.currentSpilledBuildSide.getChannel().closeAndDelete();
            } catch (Throwable t) {
                LOG.warn(
                        "Could not close and delete the temp file for the current spilled partition build side.",
                        t);
            }
            this.currentSpilledBuildSide = null;
        }

        this.currentSpilledBuildSide =
                createInputView(
                        p.getBuildSideChannel().getChannelID(),
                        p.getBuildSideBlockCount(),
                        p.getLastSegmentLimit());
        return new WrappedRowIterator<>(
                new LongHashPartitionChannelReaderInputViewIterator(
                        this.currentSpilledBuildSide, this.buildSideSerializer),
                this.buildSideSerializer.createInstance());
    }

    public ProbeIterator getSpilledPartitionProbeSideIter(LongHashPartition p) throws IOException {
        // close probe side channel of last processed partition
        if (this.currentSpilledProbeSide != null) {
            try {
                this.currentSpilledProbeSide.getChannel().closeAndDelete();
            } catch (Throwable t) {
                LOG.warn(
                        "Could not close and delete the temp file for the current spilled partition probe side.",
                        t);
            }
            this.currentSpilledProbeSide = null;
        }

        // get the probe side iterator
        this.probeIterator = new ProbeIterator(this.probeSideSerializer.createInstance());
        setPartitionProbeReader(p);
        return this.probeIterator;
    }

    @Override
    protected void clearPartitions() {
        this.probeIterator = null;

        for (int i = this.partitionsBeingBuilt.size() - 1; i >= 0; --i) {
            final LongHashPartition p = this.partitionsBeingBuilt.get(i);
            try {
                p.clearAllMemory(this.internalPool);
            } catch (Exception e) {
                LOG.error("Error during partition cleanup.", e);
            }
        }
        this.partitionsBeingBuilt.clear();

        // clear the partitions that are still to be done (that have files on disk)
        for (final LongHashPartition p : this.partitionsPending) {
            p.clearAllMemory(this.internalPool);
        }

        // clear the partitions that processed by sort merge join operator
        for (final LongHashPartition p : this.partitionsPendingForSMJ) {
            try {
                p.clearAllMemory(this.internalPool);
            } catch (Exception e) {
                LOG.error("Error during partition cleanup.", e);
            }
        }
        this.partitionsPendingForSMJ.clear();
    }

    public boolean compressionEnabled() {
        return compressionEnabled;
    }

    public BlockCompressionFactory compressionCodecFactory() {
        return compressionCodecFactory;
    }

    public int compressionBlockSize() {
        return compressionBlockSize;
    }
}
