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
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.SerializationCopier;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.MatchTaskIterator} that uses a hybrid-hash-join
 * internally to match the records with equal key. The build side of the hash is the first input of the match.  
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class BuildSecondHashMatchIterator<K extends Key, V1 extends Value, V2 extends Value> implements MatchTaskIterator<K, V1, V2>
{
	private final MemoryManager memManager;
	
	private final HashJoin<K, V2, V1> hashJoin;
	
	private final SerializationCopier<V1> probeSideCopier;
	
	private final Class<K> keyClass;
	
	private final Class<V2> buildValueClass;
	
	private final Class<V1> probeValueClass;
	
	private KeyValuePair<K, V2> nextBuildSideObject; 
	
	private volatile boolean running = true;
	
	// --------------------------------------------------------------------------------------------
	
	
	public BuildSecondHashMatchIterator(Iterator<KeyValuePair<K, V1>> firstInput, Iterator<KeyValuePair<K, V2>> secondInput,
			Class<K> keyClass, Class<V1> firstValueClass, Class<V2> secondValueClass,
			MemoryManager memManager, IOManager ioManager, AbstractInvokable ownerTask, 
			long totalMemory)
	throws MemoryAllocationException
	{		
		this.memManager = memManager;
		this.keyClass = keyClass;
		this.buildValueClass = secondValueClass;
		this.probeValueClass =  firstValueClass;
		this.probeSideCopier = new SerializationCopier<V1>();
		
		this.nextBuildSideObject = newBuildSidePair();
		
		this.hashJoin = BuildFirstHashMatchIterator.getHashJoin(secondInput, firstInput, keyClass, secondValueClass, 
			firstValueClass, memManager, ioManager, ownerTask, totalMemory);
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
	public <OK extends Key, OV extends Value> boolean callWithNextKey(MatchStub<K, V1, V2, OK, OV> matchFunction,
			Collector<OK, OV> collector)
	throws IOException
	{
		if (this.hashJoin.nextKey())
		{
			// we have a next key, get the iterators to the probe and build side values
			final HashJoin.HashBucketIterator<K, V2> buildSideIterator = this.hashJoin.getBuildSideIterator();
			final HashJoin.ProbeSideIterator<K, V1> probeIterator = this.hashJoin.getProbeSideIterator();
			boolean notFirst = false;
			
			while (probeIterator.hasNext()) {
				V1 probeValue = probeIterator.nextValue();
				
				if (notFirst) {
					buildSideIterator.reset();
				}
				notFirst = true;
				
				KeyValuePair<K, V2> nextBuildSidePair = this.nextBuildSideObject;
				
				// get the first build side value
				if (buildSideIterator.next(nextBuildSidePair)) {
					KeyValuePair<K, V2> tmpPair = newBuildSidePair();
					
					// check if there is another build-side value
					if (buildSideIterator.next(tmpPair)) {
						// more than one build-side value --> copy the probe side
						this.probeSideCopier.setCopy(probeValue);
						
						// call match on the first pair
						matchFunction.match(nextBuildSidePair.getKey(), probeValue, nextBuildSidePair.getValue(), collector);
						
						// call match on the second pair
						probeValue = newProbeValue();
						this.probeSideCopier.getCopy(probeValue);
						matchFunction.match(tmpPair.getKey(), probeValue, tmpPair.getValue(), collector);
						
						tmpPair = newBuildSidePair();
						while (this.running && buildSideIterator.next(tmpPair)) {
							// call match on the next pair
							probeValue = newProbeValue();
							this.probeSideCopier.getCopy(probeValue);
							matchFunction.match(tmpPair.getKey(), probeValue, tmpPair.getValue(), collector);
							tmpPair = newBuildSidePair();
						}
						this.nextBuildSideObject = tmpPair;
					}
					else {
						// only single pair matches
						this.nextBuildSideObject = tmpPair;
						matchFunction.match(nextBuildSidePair.getKey(), probeValue, nextBuildSidePair.getValue(), collector);
					}
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
	
	/**
	 * Utility function to instantiate a new empty key/value pair, typed for the build side.
	 * 
	 * @return A new empty key/value pair, typed for the build side.
	 */
	private final KeyValuePair<K, V2> newBuildSidePair()
	{
		try {
			return new KeyValuePair<K, V2>(
					this.keyClass.newInstance(), this.buildValueClass.newInstance());
		}
		catch (IllegalAccessException ex) {
			throw new RuntimeException("Cannot create instance of data type. Class or nullary constructor not public.");
		}
		catch (InstantiationException iex) {
			throw new RuntimeException("Cannot create instance of data type. No public nullary constructor.");
		}
	}
	
	/**
	 * Utility function to instantiate a new empty value, typed for the probe side.
	 * 
	 * @return A new empty value, typed for the probe side.
	 */
	private final V1 newProbeValue() {
		try {
			return this.probeValueClass.newInstance();
		}
		catch (IllegalAccessException ex) {
			throw new RuntimeException("Cannot create instance of data type. Class or nullary constructor not public.");
		}
		catch (InstantiationException iex) {
			throw new RuntimeException("Cannot create instance of data type. No public nullary constructor.");
		}
	}
}
