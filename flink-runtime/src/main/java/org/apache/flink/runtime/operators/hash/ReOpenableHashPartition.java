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


package org.apache.flink.runtime.operators.hash;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.BulkBlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

public class ReOpenableHashPartition<BT, PT> extends HashPartition<BT, PT> {

	protected int initialPartitionBuffersCount = -1; 						// stores the number of buffers used for an in-memory partition after the build phase has finished.

	private FileIOChannel.ID initialBuildSideChannel = null;			// path to initial build side contents (only for in-memory partitions)
	
	private BlockChannelWriter<MemorySegment> initialBuildSideWriter = null;

	private boolean isRestored = false;							// marks a restored partition
	
	
	
	int getInitialPartitionBuffersCount() {
		if (initialPartitionBuffersCount == -1) {
			throw new RuntimeException("Hash Join: Bug: This partition is most likely a spilled partition that is not restorable");
		}
		return initialPartitionBuffersCount;
	}
	
	
	ReOpenableHashPartition(TypeSerializer<BT> buildSideAccessors,
			TypeSerializer<PT> probeSideAccessors, int partitionNumber,
			int recursionLevel, MemorySegment initialBuffer,
			MemorySegmentSource memSource, int segmentSize) {
		super(buildSideAccessors, probeSideAccessors, partitionNumber, recursionLevel,
				initialBuffer, memSource, segmentSize);
	}

	@Override
	public int finalizeProbePhase(List<MemorySegment> freeMemory,
			List<HashPartition<BT, PT>> spilledPartitions,
			boolean keepUnprobedSpilledPartitions) throws IOException {
		if ( furtherPartitioning || recursionLevel != 0 || isRestored) {
			if (isInMemory() && initialBuildSideChannel != null && !isRestored) {
				// return the overflow segments
				for (int k = 0; k < this.numOverflowSegments; k++) {
					freeMemory.add(this.overflowSegments[k]);
				}
				this.overflowSegments = null;
				this.numOverflowSegments = 0;
				this.nextOverflowBucket = 0;
				// we already returned the partitionBuffers via the returnQueue.
				return 0; 
			}
			return super.finalizeProbePhase(freeMemory, spilledPartitions, keepUnprobedSpilledPartitions);
		}
		if (isInMemory()) {
			return 0;
		} else if (this.probeSideRecordCounter == 0 && !keepUnprobedSpilledPartitions) {
			freeMemory.add(this.probeSideBuffer.getCurrentSegment());
			// delete the spill files
			this.probeSideChannel.close();
			this.probeSideChannel.deleteChannel();
			return 0;
		} else {
			this.probeSideBuffer.close();
			this.probeSideChannel.close(); // finish pending write requests.
			spilledPartitions.add(this);
			return 1;
		}
	}
	
	/**
	 * Spills this partition to disk. This method is invoked once after the initial open() method
	 * 
	 * @return Number of memorySegments in the writeBehindBuffers!
	 */
	int spillInMemoryPartition(FileIOChannel.ID targetChannel, IOManager ioManager, LinkedBlockingQueue<MemorySegment> writeBehindBuffers) throws IOException {
		this.initialPartitionBuffersCount = partitionBuffers.length; // for ReOpenableHashMap
		this.initialBuildSideChannel = targetChannel;
		
		initialBuildSideWriter = ioManager.createBlockChannelWriter(targetChannel, writeBehindBuffers);
		
		final int numSegments = this.partitionBuffers.length;
		for (int i = 0; i < numSegments; i++) {
			initialBuildSideWriter.writeBlock(partitionBuffers[i]);
		}
		this.partitionBuffers = null;
		initialBuildSideWriter.close();
		// num partitions are now in the writeBehindBuffers. We propagate this information back
		return numSegments;
		
	}
	
	/**
	 * This method is called every time a multi-match hash map is opened again for a new probe input.
	 * @param ioManager 
	 * @param availableMemory 
	 * @throws IOException 
	 */
	void restorePartitionBuffers(IOManager ioManager, List<MemorySegment> availableMemory) throws IOException {
		final BulkBlockChannelReader reader = ioManager.createBulkBlockChannelReader(this.initialBuildSideChannel, 
			availableMemory, this.initialPartitionBuffersCount);
		reader.close();
		final List<MemorySegment> partitionBuffersFromDisk = reader.getFullSegments();
		this.partitionBuffers = (MemorySegment[]) partitionBuffersFromDisk.toArray(new MemorySegment[partitionBuffersFromDisk.size()]);
		
		this.overflowSegments = new MemorySegment[2];
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		this.isRestored = true;
	}
	
	
	@Override
	public void clearAllMemory(List<MemorySegment> target) {
		if (initialBuildSideChannel != null) {
			try {
				this.initialBuildSideWriter.closeAndDelete();
			} catch (IOException ioex) {
				throw new RuntimeException("Error deleting the partition files. Some temporary files might not be removed.");
			}
		}
		super.clearAllMemory(target);
	}

}
