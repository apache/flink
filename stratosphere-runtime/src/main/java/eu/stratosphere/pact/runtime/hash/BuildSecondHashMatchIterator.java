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
import java.util.List;

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.task.util.JoinTaskIterator;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.JoinTaskIterator} that uses a hybrid-hash-join
 * internally to match the records with equal key. The build side of the hash is the second input of the match.  
 */
public final class BuildSecondHashMatchIterator<V1, V2, O> implements JoinTaskIterator<V1, V2, O> {
	
	private final MutableHashTable<V2, V1> hashJoin;
	
	private final V2 nextBuildSideObject;
	
	private final V2 tempBuildSideRecord;
	
	private final V1 probeCopy;
	
	private final TypeSerializer<V1> probeSideSerializer;
	
	private final MemoryManager memManager;
	
	private final MutableObjectIterator<V1> firstInput;
	
	private final MutableObjectIterator<V2> secondInput;
	
	private volatile boolean running = true;
	
	// --------------------------------------------------------------------------------------------
	
	public BuildSecondHashMatchIterator(MutableObjectIterator<V1> firstInput, MutableObjectIterator<V2> secondInput,
			TypeSerializer<V1> serializer1, TypeComparator<V1> comparator1,
			TypeSerializer<V2> serializer2, TypeComparator<V2> comparator2,
			TypePairComparator<V1, V2> pairComparator,
			MemoryManager memManager, IOManager ioManager, AbstractInvokable ownerTask, long totalMemory)
	throws MemoryAllocationException
	{		
		this.memManager = memManager;
		this.firstInput = firstInput;
		this.secondInput = secondInput;
		this.probeSideSerializer = serializer1;
		
		this.nextBuildSideObject = serializer2.createInstance();
		this.tempBuildSideRecord = serializer2.createInstance();
		this.probeCopy = serializer1.createInstance();
		
		this.hashJoin = getHashJoin(serializer2, comparator2, serializer1, comparator1, pairComparator,
			memManager, ioManager, ownerTask, totalMemory);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException {
		this.hashJoin.open(this.secondInput, this.firstInput);
	}

	@Override
	public void close() {
		// close the join
		this.hashJoin.close();
		
		// free the memory
		final List<MemorySegment> segments = this.hashJoin.getFreedMemory();
		this.memManager.release(segments);
	}

	@Override
	public boolean callWithNextKey(GenericJoiner<V1, V2, O> matchFunction, Collector<O> collector)
	throws Exception
	{
		if (this.hashJoin.nextRecord())
		{
			// we have a next record, get the iterators to the probe and build side values
			final MutableHashTable.HashBucketIterator<V2, V1> buildSideIterator = this.hashJoin.getBuildSideIterator();
			V2 nextBuildSideRecord = this.nextBuildSideObject;
			
			// get the first build side value
			if ((nextBuildSideRecord = buildSideIterator.next(nextBuildSideRecord)) != null) {
				V2 tmpRec = this.tempBuildSideRecord;
				final V1 probeRecord = this.hashJoin.getCurrentProbeRecord();
				
				// check if there is another build-side value
				if ((tmpRec = buildSideIterator.next(tmpRec)) != null) {
					// more than one build-side value --> copy the probe side
					V1 probeCopy = this.probeCopy;
					probeCopy = this.probeSideSerializer.copy(probeRecord, probeCopy);
					
					// call match on the first pair
					matchFunction.join(probeCopy, nextBuildSideRecord, collector);
					
					// call match on the second pair
					probeCopy = this.probeSideSerializer.copy(probeRecord, probeCopy);
					matchFunction.join(probeCopy, tmpRec, collector);
					
					while (this.running && ((nextBuildSideRecord = buildSideIterator.next(nextBuildSideRecord)) != null)) {
						// call match on the next pair
						// make sure we restore the value of the probe side record
						probeCopy = this.probeSideSerializer.copy(probeRecord, probeCopy);
						matchFunction.join(probeCopy, nextBuildSideRecord, collector);
					}
				}
				else {
					// only single pair matches
					matchFunction.join(probeRecord, nextBuildSideRecord, collector);
				}
			}
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public void abort() {
		this.running = false;
		this.hashJoin.abort();
	}
	
	public <BT, PT> MutableHashTable<BT, PT> getHashJoin(TypeSerializer<BT> buildSideSerializer, TypeComparator<BT> buildSideComparator,
			TypeSerializer<PT> probeSideSerializer, TypeComparator<PT> probeSideComparator,
			TypePairComparator<PT, BT> pairComparator,
			MemoryManager memManager, IOManager ioManager, AbstractInvokable ownerTask, long totalMemory)
	throws MemoryAllocationException
	{
		final int numPages = memManager.computeNumberOfPages(totalMemory);
		final List<MemorySegment> memorySegments = memManager.allocatePages(ownerTask, numPages);
		return new MutableHashTable<BT, PT>(buildSideSerializer, probeSideSerializer, buildSideComparator, probeSideComparator, pairComparator, memorySegments, ioManager);
	}
	
}
