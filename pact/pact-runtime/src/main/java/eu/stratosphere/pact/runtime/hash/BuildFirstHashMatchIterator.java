/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.GenericMatcher;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypePairComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.MatchTaskIterator} that uses a hybrid-hash-join
 * internally to match the records with equal key. The build side of the hash is the first input of the match.  
 *
 * @author Stephan Ewen
 */
public final class BuildFirstHashMatchIterator<V1, V2, O> implements MatchTaskIterator<V1, V2, O>
{	
	private final MutableHashTable<V1, V2> hashJoin;
	
	private final V1 nextBuildSideObject;
	
	private final V1 tempBuildSideRecord;
	
	private final V2 probeCopy;
	
	private final TypeSerializer<V2> probeSideSerializer;
	
	private final MemoryManager memManager;
	
	private final MutableObjectIterator<V1> firstInput;
	
	private final MutableObjectIterator<V2> secondInput;
	
	private volatile boolean running = true;
	
	// --------------------------------------------------------------------------------------------
	
	public BuildFirstHashMatchIterator(MutableObjectIterator<V1> firstInput, MutableObjectIterator<V2> secondInput,
			TypeSerializer<V1> serializer1, TypeComparator<V1> comparator1,
			TypeSerializer<V2> serializer2, TypeComparator<V2> comparator2,
			TypePairComparator<V2, V1> pairComparator,
			MemoryManager memManager, IOManager ioManager, AbstractInvokable ownerTask, long totalMemory)
	throws MemoryAllocationException
	{		
		this.memManager = memManager;
		this.firstInput = firstInput;
		this.secondInput = secondInput;
		this.probeSideSerializer = serializer2;
		
		this.nextBuildSideObject = serializer1.createInstance();
		this.tempBuildSideRecord = serializer1.createInstance();
		this.probeCopy = serializer2.createInstance();
		
		this.hashJoin = getHashJoin(serializer1, comparator1, serializer2, comparator2, pairComparator,
			memManager, ioManager, ownerTask, totalMemory);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#open()
	 */
	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException
	{
		this.hashJoin.open(this.firstInput, this.secondInput);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#close()
	 */
	@Override
	public void close()
	{
		// close the join
		this.hashJoin.close();
		
		// free the memory
		final List<MemorySegment> segments = this.hashJoin.getFreedMemory();
		this.memManager.release(segments);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#callWithNextKey(eu.stratosphere.pact.common.stub.MatchStub, eu.stratosphere.pact.common.stub.Collector)
	 */
	@Override
	public boolean callWithNextKey(GenericMatcher<V1, V2, O> matchFunction, Collector<O> collector)
	throws Exception
	{
		if (this.hashJoin.nextRecord())
		{
			// we have a next record, get the iterators to the probe and build side values
			final MutableHashTable.HashBucketIterator<V1, V2> buildSideIterator = this.hashJoin.getBuildSideIterator();
			final V1 nextBuildSideRecord = this.nextBuildSideObject;
			
			// get the first build side value
			if (buildSideIterator.next(nextBuildSideRecord)) {
				final V1 tmpRec = this.tempBuildSideRecord;
				final V2 probeRecord = this.hashJoin.getCurrentProbeRecord();
				
				// check if there is another build-side value
				if (buildSideIterator.next(tmpRec)) {
					// more than one build-side value --> copy the probe side
					final V2 probeCopy = this.probeCopy;
					this.probeSideSerializer.copyTo(probeRecord, probeCopy);
					
					// call match on the first pair
					matchFunction.match(nextBuildSideRecord, probeCopy, collector);
					
					// call match on the second pair
					this.probeSideSerializer.copyTo(probeRecord, probeCopy);
					matchFunction.match(tmpRec, probeCopy, collector);
					
					while (this.running && buildSideIterator.next(nextBuildSideRecord)) {
						// call match on the next pair
						// make sure we restore the value of the probe side record
						this.probeSideSerializer.copyTo(probeRecord, probeCopy);
						matchFunction.match(nextBuildSideRecord, probeCopy, collector);
					}
				}
				else {
					// only single pair matches
					matchFunction.match(nextBuildSideRecord, probeRecord, collector);
				}
			}
			return true;
		}
		else {
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#abort()
	 */
	@Override
	public void abort()
	{
		// close the join
		this.running = false;
		this.hashJoin.close();
		
		// free the memory
		final List<MemorySegment> segments = this.hashJoin.getFreedMemory();
		this.memManager.release(segments);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static <BT, PT> MutableHashTable<BT, PT> getHashJoin(TypeSerializer<BT> buildSideSerializer, TypeComparator<BT> buildSideComparator,
			TypeSerializer<PT> probeSideSerializer, TypeComparator<PT> probeSideComparator,
			TypePairComparator<PT, BT> pairComparator,
			MemoryManager memManager, IOManager ioManager, AbstractInvokable ownerTask, long totalMemory)
	throws MemoryAllocationException
	{
		totalMemory = memManager.roundDownToPageSizeMultiple(totalMemory);
		final int numPages = (int) (totalMemory / memManager.getPageSize());
		final List<MemorySegment> memorySegments = memManager.allocatePages(ownerTask, numPages);
		return new MutableHashTable<BT, PT>(buildSideSerializer, probeSideSerializer, buildSideComparator, probeSideComparator, pairComparator, memorySegments, ioManager);
	}
}
