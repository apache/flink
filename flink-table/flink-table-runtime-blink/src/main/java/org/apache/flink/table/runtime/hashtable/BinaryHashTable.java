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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.util.BitSet;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.io.BinaryRowChannelInputViewIterator;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.operators.join.NullAwareJoinHelper;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.MathUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An implementation of a Hybrid Hash Join. The join starts operating in memory and gradually
 * starts spilling contents to disk, when the memory is not sufficient. It does not need to know a
 * priority how large the input will be.
 *
 * <p>The design of this class follows in many parts the design presented in
 * "Hash joins and hash teams in Microsoft SQL Server", by Goetz Graefe et al. In its current state,
 * the implementation lacks features like dynamic role reversal, partition tuning, or histogram
 * guided partitioning.</p>
 */
public class BinaryHashTable extends BaseHybridHashTable {

	/**
	 * The utilities to serialize the build side data types.
	 */
	final BinaryRowDataSerializer binaryBuildSideSerializer;

	private final AbstractRowDataSerializer originBuildSideSerializer;

	/**
	 * The utilities to serialize the probe side data types.
	 */
	private final BinaryRowDataSerializer binaryProbeSideSerializer;

	private final AbstractRowDataSerializer originProbeSideSerializer;

	/**
	 * The utilities to hash and compare the build side data types.
	 */
	private final Projection<RowData, BinaryRowData> buildSideProjection;

	/**
	 * The utilities to hash and compare the probe side data types.
	 */
	private final Projection<RowData, BinaryRowData> probeSideProjection;

	final int bucketsPerSegment;

	/**
	 * The number of hash table buckets in a single memory segment - 1.
	 * Because memory segments can be comparatively large, we fit multiple buckets into one memory
	 * segment.
	 * This variable is a mask that is 1 in the lower bits that define the number of a bucket
	 * in a segment.
	 */
	final int bucketsPerSegmentMask;

	/**
	 * The number of bits that describe the position of a bucket in a memory segment. Computed as
	 * log2(bucketsPerSegment).
	 */
	final int bucketsPerSegmentBits;

	/** Flag to enable/disable bloom filters for spilled partitions. */
	final boolean useBloomFilters;

	/**
	 * The partitions that are built by processing the current partition.
	 */
	final ArrayList<BinaryHashPartition> partitionsBeingBuilt;

	/**
	 * BitSet which used to mark whether the element(int build side) has successfully matched during
	 * probe phase. As there are 9 elements in each bucket, we assign 2 bytes to BitSet.
	 */
	final BitSet probedSet = new BitSet(2);

	/**
	 * The partitions that have been spilled previously and are pending to be processed.
	 */
	private final ArrayList<BinaryHashPartition> partitionsPending;

	private final JoinCondition condFunc;

	private final boolean reverseJoin;

	/**
	 * Should filter null keys.
	 */
	private final int[] nullFilterKeys;

	/**
	 * No keys need to filter null.
	 */
	private final boolean nullSafe;

	/**
	 * Filter null to all keys.
	 */
	private final boolean filterAllNulls;

	/**
	 * Iterator over the elements in the hash table.
	 */
	LookupBucketIterator bucketIterator;

	/**
	 * Iterator over the elements from the probe side.
	 */
	private ProbeIterator probeIterator;

	final HashJoinType type;

	private RowIterator<BinaryRowData> buildIterator;

	private boolean probeMatchedPhase = true;

	private boolean buildIterVisited = false;

	private BinaryRowData probeKey;
	private RowData probeRow;

	BinaryRowData reuseBuildRow;

	public BinaryHashTable(
			Configuration conf,
			Object owner,
			AbstractRowDataSerializer buildSideSerializer,
			AbstractRowDataSerializer probeSideSerializer,
			Projection<RowData, BinaryRowData> buildSideProjection,
			Projection<RowData, BinaryRowData> probeSideProjection,
			MemoryManager memManager,
			long reservedMemorySize,
			IOManager ioManager,
			int avgRecordLen,
			long buildRowCount,
			boolean useBloomFilters,
			HashJoinType type,
			JoinCondition condFunc,
			boolean reverseJoin,
			boolean[] filterNulls,
			boolean tryDistinctBuildRow) {
		super(conf, owner, memManager, reservedMemorySize,
				ioManager, avgRecordLen, buildRowCount, !type.buildLeftSemiOrAnti() && tryDistinctBuildRow);
		// assign the members
		this.originBuildSideSerializer = buildSideSerializer;
		this.binaryBuildSideSerializer = new BinaryRowDataSerializer(buildSideSerializer.getArity());
		this.reuseBuildRow = binaryBuildSideSerializer.createInstance();
		this.originProbeSideSerializer = probeSideSerializer;
		this.binaryProbeSideSerializer = new BinaryRowDataSerializer(originProbeSideSerializer.getArity());

		this.buildSideProjection = buildSideProjection;
		this.probeSideProjection = probeSideProjection;
		this.useBloomFilters = useBloomFilters;
		this.type = type;
		this.condFunc = condFunc;
		this.reverseJoin = reverseJoin;
		this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNulls);
		this.nullSafe = nullFilterKeys.length == 0;
		this.filterAllNulls = nullFilterKeys.length == filterNulls.length;

		this.bucketsPerSegment = this.segmentSize >> BinaryHashBucketArea.BUCKET_SIZE_BITS;
		checkArgument(bucketsPerSegment != 0,
				"Hash Table requires buffers of at least " + BinaryHashBucketArea.BUCKET_SIZE + " bytes.");
		this.bucketsPerSegmentMask = bucketsPerSegment - 1;
		this.bucketsPerSegmentBits = MathUtils.log2strict(bucketsPerSegment);

		this.partitionsBeingBuilt = new ArrayList<>();
		this.partitionsPending = new ArrayList<>();

		createPartitions(initPartitionFanOut, 0);
	}

	// ========================== build phase public method ======================================

	/**
	 * Put a build side row to hash table.
	 */
	public void putBuildRow(RowData row) throws IOException {
		final int hashCode = hash(this.buildSideProjection.apply(row).hashCode(), 0);
		// TODO: combine key projection and build side conversion to code gen.
		insertIntoTable(originBuildSideSerializer.toBinaryRow(row), hashCode);
	}

	/**
	 * End build phase.
	 */
	public void endBuild() throws IOException {
		// finalize the partitions
		int buildWriteBuffers = 0;
		for (BinaryHashPartition p : this.partitionsBeingBuilt) {
			buildWriteBuffers += p.finalizeBuildPhase(this.ioManager, this.currentEnumerator);
		}
		buildSpillRetBufferNumbers += buildWriteBuffers;

		// the first prober is the probe-side input, but the input is null at beginning
		this.probeIterator = new ProbeIterator(this.binaryProbeSideSerializer.createInstance());

		// the bucket iterator can remain constant over the time
		this.bucketIterator = new LookupBucketIterator(this);
	}

	// ========================== probe phase public method ======================================

	/**
	 * Find matched build side rows for a probe row.
	 * @return return false if the target partition has spilled, we will spill this probe row too.
	 *         The row will be re-match in rebuild phase.
	 */
	public boolean tryProbe(RowData record) throws IOException {
		if (!this.probeIterator.hasSource()) {
			// set the current probe value when probeIterator is null at the begging.
			this.probeIterator.setInstance(record);
		}
		// calculate the hash
		BinaryRowData probeKey = probeSideProjection.apply(record);
		final int hash = hash(probeKey.hashCode(), this.currentRecursionDepth);

		BinaryHashPartition p = this.partitionsBeingBuilt.get(hash % partitionsBeingBuilt.size());

		// for an in-memory partition, process set the return iterators, else spill the probe records
		if (p.isInMemory()) {
			this.probeKey = probeKey;
			this.probeRow = record;
			p.bucketArea.startLookup(hash);
			return true;
		} else {
			if (p.testHashBloomFilter(hash)) {
				BinaryRowData row = originProbeSideSerializer.toBinaryRow(record);
				p.insertIntoProbeBuffer(row);
			}
			return false;
		}
	}

	// ========================== rebuild phase public method ======================================

	/**
	 * Next record from rebuilt spilled partition or build side outer partition.
	 */
	public boolean nextMatching() throws IOException {
		if (type.needSetProbed()) {
			return processProbeIter() || processBuildIter() || prepareNextPartition();
		} else {
			return processProbeIter() || prepareNextPartition();
		}
	}

	public RowData getCurrentProbeRow() {
		if (this.probeMatchedPhase) {
			return this.probeIterator.current();
		} else {
			return null;
		}
	}

	public RowIterator<BinaryRowData> getBuildSideIterator() {
		return probeMatchedPhase ? bucketIterator : buildIterator;
	}

	// ================================ internal method ===========================================

	/**
	 * Determines the number of buffers to be used for asynchronous write behind. It is currently
	 * computed as the logarithm of the number of buffers to the base 4, rounded up, minus 2.
	 * The upper limit for the number of write behind buffers is however set to six.
	 *
	 * @param numBuffers The number of available buffers.
	 * @return The number
	 */
	@VisibleForTesting
	static int getNumWriteBehindBuffers(int numBuffers) {
		int numIOBufs = (int) (Math.log(numBuffers) / Math.log(4) - 1.5);
		return numIOBufs > 6 ? 6 : numIOBufs;
	}

	private boolean processProbeIter() throws IOException {

		// the prober's source is null at the begging.
		if (this.probeIterator.hasSource()) {
			final ProbeIterator probeIter = this.probeIterator;

			if (!this.probeMatchedPhase) {
				return false;
			}

			BinaryRowData next;
			while ((next = probeIter.next()) != null) {
				BinaryRowData probeKey = probeSideProjection.apply(next);
				final int hash = hash(probeKey.hashCode(), this.currentRecursionDepth);

				final BinaryHashPartition p = this.partitionsBeingBuilt.get(hash % partitionsBeingBuilt.size());

				// for an in-memory partition, process set the return iterators, else spill the probe records
				if (p.isInMemory()) {
					this.probeKey = probeKey;
					this.probeRow = next;
					p.bucketArea.startLookup(hash);
					return true;
				} else {
					p.insertIntoProbeBuffer(next);
				}
			}
			// -------------- partition done ---------------

			return false;
		} else {
			return false;
		}
	}

	private boolean processBuildIter() throws IOException {
		if (this.buildIterVisited) {
			return false;
		}

		this.probeMatchedPhase = false;
		this.buildIterator = new BuildSideIterator(
				this.binaryBuildSideSerializer, reuseBuildRow,
				this.partitionsBeingBuilt, probedSet, type.equals(HashJoinType.BUILD_LEFT_SEMI));
		this.buildIterVisited = true;
		return true;
	}

	private boolean prepareNextPartition() throws IOException {
		// finalize and cleanup the partitions of the current table
		for (final BinaryHashPartition p : this.partitionsBeingBuilt) {
			p.finalizeProbePhase(this.internalPool, this.partitionsPending, type.needSetProbed());
		}

		this.partitionsBeingBuilt.clear();

		if (this.currentSpilledBuildSide != null) {
			this.currentSpilledBuildSide.getChannel().closeAndDelete();
			this.currentSpilledBuildSide = null;
		}

		if (this.currentSpilledProbeSide != null) {
			this.currentSpilledProbeSide.getChannel().closeAndDelete();
			this.currentSpilledProbeSide = null;
		}

		if (this.partitionsPending.isEmpty()) {
			// no more data
			return false;
		}

		// there are pending partitions
		final BinaryHashPartition p = this.partitionsPending.get(0);
		LOG.info(String.format("Begin to process spilled partition [%d]", p.getPartitionNumber()));

		if (p.probeSideRecordCounter == 0) {
			// unprobed spilled partitions are only re-processed for a build-side outer join;
			// there is no need to create a hash table since there are no probe-side records
			this.currentSpilledBuildSide = createInputView(p.getBuildSideChannel().getChannelID(),
					p.getBuildSideBlockCount(), p.getLastSegmentLimit());
			this.buildIterator = new WrappedRowIterator<>(
					new BinaryRowChannelInputViewIterator(currentSpilledBuildSide, this.binaryBuildSideSerializer),
					binaryBuildSideSerializer.createInstance());

			this.partitionsPending.remove(0);

			return true;
		}

		this.probeMatchedPhase = true;
		this.buildIterVisited = false;

		// build the next table; memory must be allocated after this call
		buildTableFromSpilledPartition(p);

		// set the probe side
		ChannelWithMeta channelWithMeta = new ChannelWithMeta(
				p.probeSideBuffer.getChannel().getChannelID(),
				p.probeSideBuffer.getBlockCount(),
				p.probeNumBytesInLastSeg);
		this.currentSpilledProbeSide = FileChannelUtil.createInputView(
				ioManager, channelWithMeta, new ArrayList<>(),
				compressionEnable, compressionCodecFactory, compressionBlockSize, segmentSize);

		ChannelReaderInputViewIterator<BinaryRowData> probeReader =
				new ChannelReaderInputViewIterator(
						this.currentSpilledProbeSide, new ArrayList<>(), this.binaryProbeSideSerializer);
		this.probeIterator.set(probeReader);
		this.probeIterator.setReuse(binaryProbeSideSerializer.createInstance());

		// unregister the pending partition
		this.partitionsPending.remove(0);
		this.currentRecursionDepth = p.getRecursionLevel() + 1;

		// recursively get the next
		return nextMatching();
	}

	private void buildTableFromSpilledPartition(
			final BinaryHashPartition p) throws IOException {

		final int nextRecursionLevel = p.getRecursionLevel() + 1;
		if (nextRecursionLevel == 2) {
			LOG.info("Recursive hash join: partition number is " + p.getPartitionNumber());
		} else if (nextRecursionLevel > MAX_RECURSION_DEPTH) {
			throw new RuntimeException("Hash join exceeded maximum number of recursions, without reducing "
					+ "partitions enough to be memory resident. Probably cause: Too many duplicate keys.");
		}

		if (p.getBuildSideBlockCount() > p.getProbeSideBlockCount()) {
			LOG.info(String.format(
					"Hash join: Partition(%d) " +
							"build side block [%d] more than probe side block [%d]",
					p.getPartitionNumber(),
					p.getBuildSideBlockCount(),
					p.getProbeSideBlockCount()));
		}

		// we distinguish two cases here:
		// 1) The partition fits entirely into main memory. That is the case if we have enough buffers for
		//    all partition segments, plus enough buffers to hold the table structure.
		//    --> We read the partition in as it is and create a hashtable that references only
		//        that single partition.
		// 2) We can not guarantee that enough memory segments are available and read the partition
		//    in, distributing its data among newly created partitions.
		final int totalBuffersAvailable = this.internalPool.freePages() + this.buildSpillRetBufferNumbers;
		if (totalBuffersAvailable != this.totalNumBuffers) {
			throw new RuntimeException(String.format("Hash Join bug in memory management: Memory buffers leaked." +
					" availableMemory(%s), buildSpillRetBufferNumbers(%s), reservedNumBuffers(%s)",
					internalPool.freePages(), buildSpillRetBufferNumbers, totalNumBuffers));
		}

		long numBuckets = p.getBuildSideRecordCount() / BinaryHashBucketArea.NUM_ENTRIES_PER_BUCKET + 1;

		// we need to consider the worst case where everything hashes to one bucket which needs to overflow by the same
		// number of total buckets again. Also, one buffer needs to remain for the probing
		int maxBucketAreaBuffers = Math.max((int) (2 * (numBuckets / (this.bucketsPerSegmentMask + 1))), 1);
		final long totalBuffersNeeded = maxBucketAreaBuffers + p.getBuildSideBlockCount() + 2;

		if (totalBuffersNeeded < totalBuffersAvailable) {
			LOG.info(String.format("Build in memory hash table from spilled partition [%d]", p.getPartitionNumber()));

			// first read the partition in
			final List<MemorySegment> partitionBuffers = readAllBuffers(p.getBuildSideChannel().getChannelID(), p.getBuildSideBlockCount());
			BinaryHashBucketArea area = new BinaryHashBucketArea(this, (int) p.getBuildSideRecordCount(), maxBucketAreaBuffers, false);
			final BinaryHashPartition newPart = new BinaryHashPartition(area, this.binaryBuildSideSerializer, this.binaryProbeSideSerializer,
					0, nextRecursionLevel, partitionBuffers, p.getBuildSideRecordCount(), this.segmentSize, p.getLastSegmentLimit());
			area.setPartition(newPart);

			this.partitionsBeingBuilt.add(newPart);

			// now, index the partition through a hash table
			final BinaryHashPartition.PartitionIterator pIter = newPart.newPartitionIterator();

			while (pIter.advanceNext()) {
				final int hashCode = hash(buildSideProjection.apply(pIter.getRow()).hashCode(), nextRecursionLevel);
				final int pointer = (int) pIter.getPointer();
				area.insertToBucket(hashCode, pointer, true);
			}
		} else {
			// go over the complete input and insert every element into the hash table
			// compute in how many splits, we'd need to partition the result
			final int splits = (int) (totalBuffersNeeded / totalBuffersAvailable) + 1;
			final int partitionFanOut = Math.min(Math.min(10 * splits, MAX_NUM_PARTITIONS), maxNumPartition());

			createPartitions(partitionFanOut, nextRecursionLevel);
			LOG.info(String.format("Build hybrid hash table from spilled partition [%d] with recursion level [%d]",
					p.getPartitionNumber(), nextRecursionLevel));

			ChannelReaderInputView inView = createInputView(p.getBuildSideChannel().getChannelID(), p.getBuildSideBlockCount(), p.getLastSegmentLimit());
			final BinaryRowChannelInputViewIterator inIter =
					new BinaryRowChannelInputViewIterator(inView, this.binaryBuildSideSerializer);
			BinaryRowData rec = this.binaryBuildSideSerializer.createInstance();
			while ((rec = inIter.next(rec)) != null) {
				final int hashCode = hash(this.buildSideProjection.apply(rec).hashCode(), nextRecursionLevel);
				insertIntoTable(rec, hashCode);
			}

			inView.getChannel().closeAndDelete();

			// finalize the partitions
			int buildWriteBuffers = 0;
			for (BinaryHashPartition part : this.partitionsBeingBuilt) {
				buildWriteBuffers += part.finalizeBuildPhase(this.ioManager, this.currentEnumerator);
			}
			buildSpillRetBufferNumbers += buildWriteBuffers;
		}
	}

	private void insertIntoTable(final BinaryRowData record, final int hashCode) throws IOException {
		BinaryHashPartition p = partitionsBeingBuilt.get(hashCode % partitionsBeingBuilt.size());
		if (p.isInMemory()) {
			if (!p.bucketArea.appendRecordAndInsert(record, hashCode)) {
				p.addHashBloomFilter(hashCode);
			}
		} else {
			p.insertIntoBuildBuffer(record);
			p.addHashBloomFilter(hashCode);
		}
	}

	private void createPartitions(int numPartitions, int recursionLevel) {
		// sanity check
		ensureNumBuffersReturned(numPartitions);

		this.currentEnumerator = this.ioManager.createChannelEnumerator();

		this.partitionsBeingBuilt.clear();
		double numRecordPerPartition = (double) buildRowCount / numPartitions;
		int maxBuffer = maxInitBufferOfBucketArea(numPartitions);
		for (int i = 0; i < numPartitions; i++) {
			BinaryHashBucketArea area = new BinaryHashBucketArea(this, numRecordPerPartition, maxBuffer);
			BinaryHashPartition p = new BinaryHashPartition(area, this.binaryBuildSideSerializer,
					this.binaryProbeSideSerializer, i, recursionLevel, getNotNullNextBuffer(), this, this.segmentSize,
					compressionEnable, compressionCodecFactory, compressionBlockSize);
			area.setPartition(p);
			this.partitionsBeingBuilt.add(p);
		}
	}

	/**
	 * This method clears all partitions currently residing (partially) in memory. It releases all
	 * memory
	 * and deletes all spilled partitions.
	 *
	 * <p>This method is intended for a hard cleanup in the case that the join is aborted.
	 */
	@Override
	public void clearPartitions() {
		// clear the iterators, so the next call to next() will notice
		this.bucketIterator = null;
		this.probeIterator = null;

		for (int i = this.partitionsBeingBuilt.size() - 1; i >= 0; --i) {
			final BinaryHashPartition p = this.partitionsBeingBuilt.get(i);
			try {
				p.clearAllMemory(this.internalPool);
			} catch (Exception e) {
				LOG.error("Error during partition cleanup.", e);
			}
		}
		this.partitionsBeingBuilt.clear();

		// clear the partitions that are still to be done (that have files on disk)
		for (final BinaryHashPartition p : this.partitionsPending) {
			p.clearAllMemory(this.internalPool);
		}
	}

	/**
	 * Selects a partition and spills it. The number of the spilled partition is returned.
	 *
	 * @return The number of the spilled partition.
	 */
	@Override
	protected int spillPartition() throws IOException {
		// find the largest partition
		int largestNumBlocks = 0;
		int largestPartNum = -1;

		for (int i = 0; i < partitionsBeingBuilt.size(); i++) {
			BinaryHashPartition p = partitionsBeingBuilt.get(i);
			if (p.isInMemory() && p.getNumOccupiedMemorySegments() > largestNumBlocks) {
				largestNumBlocks = p.getNumOccupiedMemorySegments();
				largestPartNum = i;
			}
		}
		final BinaryHashPartition p = partitionsBeingBuilt.get(largestPartNum);

		// spill the partition
		int numBuffersFreed = p.spillPartition(this.ioManager,
				this.currentEnumerator.next(), this.buildSpillReturnBuffers);
		this.buildSpillRetBufferNumbers += numBuffersFreed;

		LOG.info(String.format("Grace hash join: Ran out memory, choosing partition " +
						"[%d] to spill, %d memory segments being freed",
				largestPartNum, numBuffersFreed));

		// grab as many buffers as are available directly
		MemorySegment currBuff;
		while (this.buildSpillRetBufferNumbers > 0 && (currBuff = this.buildSpillReturnBuffers.poll()) != null) {
			returnPage(currBuff);
			this.buildSpillRetBufferNumbers--;
		}
		numSpillFiles++;
		spillInBytes += numBuffersFreed * segmentSize;
		// The bloomFilter is built after the data is spilled, so that we can use enough memory.
		p.buildBloomFilterAndFreeBucket();
		return largestPartNum;
	}

	boolean applyCondition(BinaryRowData candidate) {
		BinaryRowData buildKey = buildSideProjection.apply(candidate);
		// They come from Projection, so we can make sure it is in byte[].
		boolean equal = buildKey.getSizeInBytes() == probeKey.getSizeInBytes()
				&& BinaryRowDataUtil.byteArrayEquals(
				buildKey.getSegments()[0].getHeapMemory(),
				probeKey.getSegments()[0].getHeapMemory(),
				buildKey.getSizeInBytes());
		// TODO do null filter in advance?
		if (!nullSafe) {
			equal = equal && !(filterAllNulls ? buildKey.anyNull() : buildKey.anyNull(nullFilterKeys));
		}
		return condFunc == null ? equal : equal && (reverseJoin ? condFunc.apply(probeRow, candidate)
				: condFunc.apply(candidate, probeRow));
	}
}
