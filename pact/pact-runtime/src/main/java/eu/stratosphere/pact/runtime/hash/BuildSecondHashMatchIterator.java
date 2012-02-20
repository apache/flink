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

import eu.stratosphere.nephele.execution.Environment;
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
import eu.stratosphere.pact.runtime.task.util.OutputCollector;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.MatchTaskIterator} that uses a hybrid-hash-join
 * internally to match the records with equal key. The build side of the hash is the first input of the match.  
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class BuildSecondHashMatchIterator implements MatchTaskIterator
{
private final MemoryManager memManager;
	
	private final HashJoin hashJoin;
	
	private PactRecord nextBuildSideObject; 
	
	private PactRecord probeCopy = new PactRecord();
	
	// DW: Start of temporary code
	private final Environment environment;
	// DW: End of temporary code
	
	private volatile boolean running = true;
	
	// --------------------------------------------------------------------------------------------
	
	public BuildSecondHashMatchIterator(MutableObjectIterator<PactRecord> firstInput, MutableObjectIterator<PactRecord> secondInput,
			int[] buildSideKeyFields, int[] probeSideKeyFields, Class<? extends Key>[] keyClasses, MemoryManager memManager, IOManager ioManager,
			AbstractInvokable ownerTask, long totalMemory)
	throws MemoryAllocationException
	{		
		this.memManager = memManager;
		this.nextBuildSideObject = new PactRecord();
		
		this.hashJoin = BuildFirstHashMatchIterator.getHashJoin(secondInput, firstInput, buildSideKeyFields, probeSideKeyFields, keyClasses, 
			memManager, ioManager, ownerTask, totalMemory);
		
		// DW: Start of temporary code
		this.environment = ownerTask.getEnvironment();
		// DW: End of temporary code
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
		// DW: Start of temporary code
		final Environment env = this.environment;
		final OutputCollector oc = (OutputCollector) collector;
		// DW: End of temporary code
		
		
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
					
					// DW : Start of temporary code
					long r1 = probeRecord.getBinaryLength();
					long r2 = nextBuildSidePair.getBinaryLength();
					// DW: End of temporary code
					
					// call match on the first pair
					matchFunction.match(probeRecord, nextBuildSidePair, collector);
					// DW: Start of temporary code
					env.reportPACTDataStatistics(r1 + r2,  
						oc.getCollectedPactRecordsInBytes());
					// DW: End of temporary code
					
					// call match on the second pair
					probeRecord = new PactRecord();
					this.probeCopy.copyTo(probeRecord);
					
					// DW : Start of temporary code
					r1 = probeRecord.getBinaryLength();
					r2 = tmpPair.getBinaryLength();
					// DW: End of temporary code
					
					matchFunction.match(probeRecord, tmpPair, collector);
					// DW: Start of temporary code
					env.reportPACTDataStatistics(r1 + r2,  
						oc.getCollectedPactRecordsInBytes());
					// DW: End of temporary code
					
					tmpPair = new PactRecord();
					while (this.running && buildSideIterator.next(tmpPair)) {
						// call match on the next pair
						probeRecord = new PactRecord();
						this.probeCopy.copyTo(probeRecord);
						
						// DW : Start of temporary code
						r1 = probeRecord.getBinaryLength();
						r2 = tmpPair.getBinaryLength();
						// DW: End of temporary code
						
						matchFunction.match(probeRecord, tmpPair, collector);
						// DW: Start of temporary code
						env.reportPACTDataStatistics(r1 + r2,  
							oc.getCollectedPactRecordsInBytes());
						// DW: End of temporary code
						tmpPair = new PactRecord();
					}
					this.nextBuildSideObject = tmpPair;
				}
				else {
					// only single pair matches
					this.nextBuildSideObject = tmpPair;
					
					// DW : Start of temporary code
					final long r1 = probeRecord.getBinaryLength();
					final long r2 = nextBuildSidePair.getBinaryLength();
					// DW: End of temporary code
					
					matchFunction.match(probeRecord, nextBuildSidePair, collector);
					// DW: Start of temporary code
					env.reportPACTDataStatistics(r1 + r2,  
						oc.getCollectedPactRecordsInBytes());
					// DW: End of temporary code
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
}
