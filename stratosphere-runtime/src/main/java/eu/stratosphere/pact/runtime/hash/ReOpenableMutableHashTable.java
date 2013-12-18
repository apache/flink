/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.typeutils.TypeComparator;
import eu.stratosphere.api.typeutils.TypePairComparator;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.util.MutableObjectIterator;

public class ReOpenableMutableHashTable<BT, PT> extends MutableHashTable<BT, PT> {

	/**
	 * Channel for the spilled partitions
	 */
	private final Channel.Enumerator spilledInMemoryPartitions;
	
	/**
	 * Stores the initial partitions and a list of the files that contain the spilled contents
	 */
	private List<HashPartition<BT, PT>> initialPartitions;
	

	/**
	 * The values of these variables are stored here after the initial open()
	 * Required to restore the initial state before each additional probe phase.
	 */
	private int initialBucketCount;
	private byte initialPartitionFanOut;
	
	private boolean spilled = false;
	
	public ReOpenableMutableHashTable(TypeSerializer<BT> buildSideSerializer,
			TypeSerializer<PT> probeSideSerializer,
			TypeComparator<BT> buildSideComparator,
			TypeComparator<PT> probeSideComparator,
			TypePairComparator<PT, BT> comparator,
			List<MemorySegment> memorySegments, IOManager ioManager) {
		super(buildSideSerializer, probeSideSerializer, buildSideComparator,
				probeSideComparator, comparator, memorySegments, ioManager);
		keepBuildSidePartitions = true;
		spilledInMemoryPartitions = ioManager.createChannelEnumerator();
	}
	
	@Override
	public void open(MutableObjectIterator<BT> buildSide,
			MutableObjectIterator<PT> probeSide) throws IOException {
		super.open(buildSide, probeSide);
		initialPartitions = new ArrayList<HashPartition<BT, PT>>( partitionsBeingBuilt );
		initialPartitionFanOut = (byte) partitionsBeingBuilt.size();
		initialBucketCount = this.numBuckets;
	}

	public void reopenProbe(MutableObjectIterator<PT> probeInput) throws IOException {
		if (this.closed.get()) {
			throw new IllegalStateException("Cannot open probe input because hash join has already been closed");
		}
		partitionsBeingBuilt.clear();
		probeIterator = new ProbeIterator<PT>(probeInput, probeSideSerializer.createInstance());
		// We restore the same "partitionsBeingBuild" state as after the initial open call.
		partitionsBeingBuilt.addAll(initialPartitions);
		
		if (spilled) {
			this.currentRecursionDepth = 0;
			initTable(initialBucketCount, initialPartitionFanOut);
			
			//setup partitions for insertion:
			for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
				ReOpenableHashPartition<BT, PT> part = (ReOpenableHashPartition<BT, PT>) this.partitionsBeingBuilt.get(i);
				if (part.isInMemory()) {
					ensureNumBuffersReturned(part.initialPartitionBuffersCount);
					part.restorePartitionBuffers(ioManager, availableMemory);
					// now, index the partition through a hash table
					final HashPartition<BT, PT>.PartitionIterator pIter = part.getPartitionIterator(this.buildSideComparator);
					final BT record = this.buildSideSerializer.createInstance();
					
					while (pIter.next(record)) {
						final int hashCode = hash(pIter.getCurrentHashCode(), 0);
						final int posHashCode = hashCode % initialBucketCount;
						final long pointer = pIter.getPointer();
						// get the bucket for the given hash code
						final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
						final int bucketInSegmentPos = (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
						final MemorySegment bucket = this.buckets[bucketArrayPos];
						insertBucketEntry(part, bucket, bucketInSegmentPos, hashCode, pointer);
					}
				} else {
					this.writeBehindBuffersAvailable--; // we are not in-memory, thus the probe side buffer will grab one wbb.
					if (this.writeBehindBuffers.size() == 0) { // prepareProbePhase always requires one buffer in the writeBehindBuffers-Queue.
						this.writeBehindBuffers.add(getNextBuffer());
						this.writeBehindBuffersAvailable++;
					}
					part.prepareProbePhase(ioManager,currentEnumerator,writeBehindBuffers);
				}
			}
			// spilled partitions are automatically added as pending partitions after in-memory has been handled
		} else {
			// the build input completely fits into memory, hence everything is still in memory.
			for (int partIdx = 0; partIdx < partitionsBeingBuilt.size(); partIdx++) {
				final HashPartition<BT, PT> p = partitionsBeingBuilt.get(partIdx);
				p.prepareProbePhase(ioManager,currentEnumerator,writeBehindBuffers);
			}
		}
	}
	

	/**
	 * This method stores the initial hash table's contents on disk if hash join needs the memory
	 * for further partition processing.
	 * The initial hash table is rebuild before a new secondary input is opened.
	 * 
	 * For the sake of simplicity we iterate over all in-memory elements and store them in one file.
	 * The file is hashed into memory upon opening a new probe input.
	 * @throws IOException 
	 */
	void storeInitialHashTable() throws IOException {
		if (spilled) {
			return; // we create the initialHashTable only once. Later calls are caused by deeper recursion lvls
		}
		spilled = true;
		
		for (int partIdx = 0; partIdx < initialPartitions.size(); partIdx++) {
			final ReOpenableHashPartition<BT, PT> p = (ReOpenableHashPartition<BT, PT>) initialPartitions.get(partIdx);
			if (p.isInMemory()) { // write memory resident partitions to disk
				this.writeBehindBuffersAvailable += p.spillInMemoryPartition(spilledInMemoryPartitions.next(), ioManager, writeBehindBuffers);
			}
		}
	}
	
	@Override
	protected boolean prepareNextPartition() throws IOException {
		// check if there will be further partition processing.
		this.furtherPartitioning = false;
		for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
			final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(i);
			if (!p.isInMemory() && p.getProbeSideRecordCount() != 0) {
				furtherPartitioning = true;
				break;
			}
		}
		if (furtherPartitioning) {
			((ReOpenableMutableHashTable<BT, PT>) this).storeInitialHashTable();
		}
		return super.prepareNextPartition();
	}
	
	
	@Override
	protected void releaseTable() {
		if(furtherPartitioning | this.currentRecursionDepth > 0) {
			super.releaseTable();
		}
	}
	
	@Override
	protected HashPartition<BT, PT> getNewInMemoryPartition(int number, int recursionLevel) {
		return new ReOpenableHashPartition<BT, PT>(this.buildSideSerializer, this.probeSideSerializer,
				number, recursionLevel, this.availableMemory.remove(this.availableMemory.size() - 1),
				this, this.segmentSize);
	}
	@Override
	public void close() {
		if (partitionsBeingBuilt.size() == 0) { // partitions are cleared after the build phase. But we need to drop
			// memory with them.
			this.partitionsBeingBuilt.addAll(initialPartitions);
		}
		this.furtherPartitioning = true; // fake, to release table properly (close() will call releaseTable())
		super.close();
	}
}
