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
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.MatchTaskIterator} that uses a hybrid-hash-join
 * internally to match the records with equal key. The build side of the hash is the first input of the match.  
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class BuildFirstHashMatchIterator implements MatchTaskIterator
{
	/**
	 * Constant describing the size of the pages, used by the internal hash join, in bytes.
	 */
	public static final int HASH_JOIN_PAGE_SIZE = 0x1 << 15;
	
	// --------------------------------------------------------------------------------------------
	
	private final MemoryManager memManager;
	
	private final HashJoin hashJoin;
	
	private PactRecord nextBuildSideObject; 
	
	private PactRecord probeCopy = new PactRecord();
	
	private volatile boolean running = true;
	
	// --------------------------------------------------------------------------------------------
	
	public BuildFirstHashMatchIterator(MutableObjectIterator<PactRecord> firstInput, MutableObjectIterator<PactRecord> secondInput,
			int[] buildSideKeyFields, int[]probeSideKeyFields, Class<? extends Key>[] keyClasses, MemoryManager memManager, IOManager ioManager,
			AbstractInvokable ownerTask, long totalMemory)
	throws MemoryAllocationException
	{		
		this.memManager = memManager;
		this.nextBuildSideObject = new PactRecord();
		
		this.hashJoin = getHashJoin(firstInput, secondInput, buildSideKeyFields, probeSideKeyFields, keyClasses, memManager, ioManager,
			ownerTask, totalMemory);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#open()
	 */
	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException
	{
		this.hashJoin.open();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#close()
	 */
	@Override
	public void close()
	{
		List<MemorySegment> segments = this.hashJoin.close();
		this.memManager.release(segments);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#callWithNextKey(eu.stratosphere.pact.common.stub.MatchStub, eu.stratosphere.pact.common.stub.Collector)
	 */
	@Override
	public boolean callWithNextKey(MatchStub matchFunction, Collector collector)
	throws Exception
	{
		if (this.hashJoin.nextRecord())
		{
			// we have a next record, get the iterators to the probe and build side values
			final HashJoin.HashBucketIterator buildSideIterator = this.hashJoin.getBuildSideIterator();
			PactRecord probeRecord = this.hashJoin.getCurrentProbeRecord();
			PactRecord nextBuildSidePair = this.nextBuildSideObject;
			
			// get the first build side value
			if (buildSideIterator.next(nextBuildSidePair)) {
				PactRecord tmpPair = new PactRecord();
				
				// check if there is another build-side value
				if (buildSideIterator.next(tmpPair)) {
					// more than one build-side value --> copy the probe side
					probeRecord.copyTo(this.probeCopy);
					
					// call match on the first pair
					matchFunction.match(nextBuildSidePair, probeRecord, collector);
					
					// call match on the second pair
					probeRecord = new PactRecord();
					this.probeCopy.copyTo(probeRecord);
					matchFunction.match(tmpPair, probeRecord, collector);
					
					tmpPair = new PactRecord();
					while (this.running && buildSideIterator.next(tmpPair)) {
						// call match on the next pair
						probeRecord = new PactRecord();
						this.probeCopy.copyTo(probeRecord);
						matchFunction.match(tmpPair, probeRecord, collector);
						tmpPair = new PactRecord();
					}
					this.nextBuildSideObject = tmpPair;
				}
				else {
					// only single pair matches
					this.nextBuildSideObject = tmpPair;
					matchFunction.match(nextBuildSidePair, probeRecord, collector);
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
		this.running = false;
		List<MemorySegment> segments = this.hashJoin.close();
		this.memManager.release(segments);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static HashJoin getHashJoin(MutableObjectIterator<PactRecord> buildSideInput, MutableObjectIterator<PactRecord> probeSideInput,
			int[] buildSideKeyFields, int[] probeSideKeyFields, Class<? extends Key>[] keyClasses,
			MemoryManager memManager, IOManager ioManager, AbstractInvokable ownerTask, long totalMemory)
	throws MemoryAllocationException
	{
		// adjust the memory for full page sizes
		totalMemory &= ~(((long) HASH_JOIN_PAGE_SIZE) - 1);
		// NOTE: This calculation is erroneous if the total memory is above 63 TiBytes. 
		final int numPages = (int) (totalMemory / HASH_JOIN_PAGE_SIZE);
		
		final List<MemorySegment> memorySegments = memManager.allocateStrict(ownerTask, numPages, HASH_JOIN_PAGE_SIZE);
		
		return new HashJoin(buildSideInput, probeSideInput, buildSideKeyFields, probeSideKeyFields, keyClasses, memorySegments, ioManager);
	}
}
