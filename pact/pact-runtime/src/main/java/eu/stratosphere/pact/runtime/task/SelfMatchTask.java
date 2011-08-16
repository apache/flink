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

package eu.stratosphere.pact.runtime.task;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.sort.SortMergeMatchIterator;
import eu.stratosphere.pact.runtime.sort.SortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.SerializationCopier;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * SelfMatch task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a MatchStub
 * implementation.
 * <p>
 * The SelfMatchTask creates a iterator over all key-value pairs of its input. 
 * The iterator returns all k-v pairs grouped by their key. A Cartesian product is build 
 * over pairs that share the same key. Each element of these Cartesian products is handed 
 * to the <code>match()</code> method of the MatchStub.
 * 
 * @see eu.stratosphere.pact.common.stub.MatchStub
 * @author Fabian Hueske
 * @auther Matthias Ringwald
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SelfMatchTask extends AbstractPactTask<MatchStub> {

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;
	
	// share ratio for resettable iterator
	private static final double MEMORY_SHARE_RATIO = 0.10;
	
	// size of value buffer in elements
	private static final int VALUE_BUFFER_SIZE = 10;
	
	private long availableMemory;
	
	// output collector
	private OutputCollector output;

	// copier for key and values
	private final SerializationCopier<Key> keyCopier = new SerializationCopier<Key>();
	private final SerializationCopier<Value> innerValCopier = new SerializationCopier<Value>();
	
	private int[] keyPositions;
	private Class<? extends Key>[] keyClasses;
	
	private CloseableInputProvider<PactRecord> closeableInput;

	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 2; // TODO only one?
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<MatchStub> getStubType() {
		return MatchStub.class;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		// set up memory and I/O parameters
		long availableMemory = this.config.getMemorySize();
		final int maxFileHandles = this.config.getNumFilehandles();
		final float spillThreshold = this.config.getSortSpillingTreshold();
		
		
		// test minimum memory requirements
		final LocalStrategy ls = this.config.getLocalStrategy();
		long strategyMinMem = 0;
		
		switch (ls) {
			case SORT_SELF_NESTEDLOOP:
				strategyMinMem = MIN_REQUIRED_MEMORY*2;
				break;
			case SELF_NESTEDLOOP: 
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
		}
		
		if (availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The SelfMatch task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}
		
		// obtain the TaskManager's MemoryManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the TaskManager's IOManager
		final IOManager ioManager = getEnvironment().getIOManager();
		
		keyPositions = this.config.getLocalStrategyKeyPositions(0);
		keyClasses = this.config.getLocalStrategyKeyClasses(this.userCodeClassLoader);
		
		if (keyPositions == null || keyClasses == null) {
			throw new Exception("The key positions and types are not specified for the SelfMatchTask.");
		}
		if (keyPositions.length != keyClasses.length) {
			throw new Exception("The number of key positions and types does not match in the configuration");
		}
		
		// create the comparators
		@SuppressWarnings("unchecked")
		final Comparator<Key>[] comparators = new Comparator[keyPositions.length];
		final KeyComparator kk = new KeyComparator();
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = kk;
		}
		
		switch (ls) {
			// local strategy is NONE
			// input is already grouped, an iterator that wraps the reader is
			// created and returned
			case SELF_NESTEDLOOP:
				// iterator wraps input reader
				closeableInput = new SimpleCloseableInputProvider<PactRecord>(inputs[0]);
				break;
				
				// local strategy is SORT
				// The input is grouped using a sort-merge strategy.
				// An iterator on the sorted pairs is created and returned.
			case SORT_SELF_NESTEDLOOP:
					// instantiate a sort-merger
					closeableInput = new UnilateralSortMerger(memoryManager, ioManager,
						(long)(availableMemory * (1.0 - MEMORY_SHARE_RATIO)), maxFileHandles, 
						comparators, keyPositions, keyClasses, inputs[0], this, spillThreshold);
					// obtain and return a grouped iterator from the sort-merger
					break;
			default:
				throw new RuntimeException("Invalid local strategy provided for SelfMatchTask: " +
					config.getLocalStrategy());
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		final KeyGroupedIterator it = 
				new KeyGroupedIterator(closeableInput.getIterator(), keyPositions, keyClasses);
		
		while(this.running && it.nextKey()) {
			// cross all value of a certain key
			crossValues(it.getKeys(), it.getValues(), output);
		}
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception
	{
		if (this.closeableInput != null) {
			this.closeableInput.close();
			this.closeableInput = null;
		}
	}
	
	
	
	
	
	
	
	

	// ------------------------------------------------------------------------
	

	
	/**
	 * Crosses the values of all pairs that have the same key.
	 * The {@link MatchStub#match(Key, Iterator, Collector)} method is called for each element of the 
	 * Cartesian product. 
	 * 
	 * @param key 
	 *        The key of all values in the iterator.
	 * @param vals 
	 *        An iterator over values that share the same key.
	 * @param out
	 *        The collector to write the results to.
	 */
	private final void crossValues(Key[] key, final Iterator<PactRecord> values, final OutputCollector out)
	{
		// allocate buffer
		final PactRecord[] valBuffer = new PactRecord[VALUE_BUFFER_SIZE];
		
		// fill value buffer for the first time
		int bufferValCnt;
		for(bufferValCnt = 0; bufferValCnt < VALUE_BUFFER_SIZE; bufferValCnt++) {
			if(values.hasNext()) {
				// read value into buffer
				valBuffer[bufferValCnt] = values.next();
			} else {
				break;
			}
		}
		
		// cross values in buffer
		for (int i = 0;i < bufferValCnt; i++) {
			// check if task was canceled
			if (!this.running) return;
			
			for (int j = 0; j < bufferValCnt; j++) {
				// check if task was canceled
				if (!this.running) return;
				
				// match
				stub.match(valBuffer[i].createCopy(), valBuffer[j].createCopy(), out);
			}
			
		}
		
		if(this.running && values.hasNext()) {
			// there are still value in the reader

			// wrap value iterator in a reader
			Iterator<Value> valReader = new Iterator<Value>() {

				@Override
				public boolean hasNext() {
					
					if (!running) 
						return false;
					else
						return values.hasNext();
				}

				@Override
				public PactRecord next() {
						
					// get next value
					PactRecord nextVal = values.next();

					for(int i=0;i<VALUE_BUFFER_SIZE;i++) {
						stub.match(valBuffer[i].createCopy(),nextVal.createCopy(),out);
					}
					
					// return value
					return nextVal;
				}
				
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
			
			SpillingResettableIterator<Value> outerValResettableIterator = null;
			SpillingResettableIterator<Value> innerValResettableIterator = null;
			
			try {
				//ValueDeserializer<Value> v1Deserializer = new ValueDeserializer<Value>(stub.getFirstInValueType());
				
				// read values into outer resettable iterator
				outerValResettableIterator = 
						new SpillingResettableIterator<Value>(getEnvironment().getMemoryManager(), getEnvironment().getIOManager(), 
								valReader, (long) (this.availableMemory * (MEMORY_SHARE_RATIO/2)), v1Deserializer, this);
				outerValResettableIterator.open();

				// iterator returns first buffer than outer resettable iterator (all values of the incoming iterator)
				BufferIncludingIterator bii = new BufferIncludingIterator(valBuffer, outerValResettableIterator);
				
				// read remaining values into inner resettable iterator
				if(!this.taskCanceled && outerValResettableIterator.hasNext()) {
					innerValResettableIterator =
						new SpillingResettableIterator<Value>(getEnvironment().getMemoryManager(), getEnvironment().getIOManager(),
								bii, (long) (this.availableMemory * (MEMORY_SHARE_RATIO/2)), v1Deserializer, this);
					innerValResettableIterator.open();
					
					// reset outer iterator
					outerValResettableIterator.reset();
				
					// cross remaining values
					while(!this.taskCanceled && outerValResettableIterator.hasNext()) {
						
						// fill buffer with next elements from outer resettable iterator
						bufferValCnt = 0;
						while(!this.taskCanceled && outerValResettableIterator.hasNext() && bufferValCnt < VALUE_BUFFER_SIZE) {
							valBuffer[bufferValCnt++].setCopy(outerValResettableIterator.next());
						}
						if(bufferValCnt == 0) break;
						
						// cross buffer with inner iterator
						while(!this.taskCanceled && innerValResettableIterator.hasNext()) {
							
							// read inner value
							innerVal = innerValResettableIterator.next();
							
							for(int i=0;i<bufferValCnt;i++) {
								
								// get copies
								copyKey = keySerialization.newInstance();
								keyCopier.getCopy(copyKey);
								outerVal = valSerialization.newInstance();
								valBuffer[i].getCopy(outerVal);
								
								stub.match(copyKey, outerVal, innerVal, out);
								
								if(i < bufferValCnt - 1)
									innerVal = innerValResettableIterator.repeatLast();
							}
						}
						innerValResettableIterator.reset();
					}
				}
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if(innerValResettableIterator != null) {
					innerValResettableIterator.close();
				}
				if(outerValResettableIterator != null) {
					outerValResettableIterator.close();
				}
			}
			
		}
		
	}
	
	private final class BufferIncludingIterator implements Iterator<Value> {

		int bufferIdx = 0;
		
		private SerializationCopier<Value>[] valBuffer;
		private Iterator<Value> valIterator;
		
		public BufferIncludingIterator(SerializationCopier<Value>[] valBuffer, Iterator<Value> valIterator) {
			this.valBuffer = valBuffer;
			this.valIterator = valIterator;
		}
		
		@Override
		public boolean hasNext() {
			if(taskCanceled) 
				return false;
			
			if(bufferIdx < VALUE_BUFFER_SIZE) 
				return true;
			
			return valIterator.hasNext();
		}

		@Override
		public Value next() {
			if(bufferIdx < VALUE_BUFFER_SIZE) {
				Value outVal = valSerialization.newInstance();
				valBuffer[bufferIdx++].getCopy(outVal);
				return outVal;
			} else {
				return valIterator.next();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	};
}
