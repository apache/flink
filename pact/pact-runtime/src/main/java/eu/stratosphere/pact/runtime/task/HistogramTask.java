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

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.util.KeyComparator;

/**
 * COMMENTS: TODO
 */
@SuppressWarnings({"unchecked"})
public class HistogramTask extends AbstractPactTask<Stub> {


	public static final String NUMBER_OF_BUCKETS = "histogram.buckets.count";
	
	public static final String HISTOGRAM_MEMORY = "histogram.memory.amount";
	
	private int[] keyPositions;
	
	private Class<? extends Key>[] keyClasses;
	
	private CloseableInputProvider<PactRecord> input = null;
	// input reader
	private CountingMutableObjectIterator countingReader; 
	//private RecordReader reader;


	// the memory dedicated to the sorter
	private long availableMemory;
	
	// maximum number of file handles
	private int maxFileHandles;
	
	// the fill fraction of the buffers that triggers the spilling
	private float spillThreshold;
	
	private int numBuckets;

	// ------------------------------------------------------------------------
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<Stub> getStubType() {
		return Stub.class;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception {

		// obtain the TaskManager's MemoryManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the TaskManager's IOManager
		final IOManager ioManager = getEnvironment().getIOManager();
		
		// get the key positions and types
		this.keyPositions = this.config.getLocalStrategyKeyPositions(0);
		this.keyClasses = this.config.getLocalStrategyKeyClasses(this.userCodeClassLoader);
		if (this.keyPositions == null || this.keyClasses == null) {
			throw new Exception("The key positions and types are not specified for the ReduceTask.");
		}
		
		// create the comparators
		final Comparator<Key>[] comparators = new Comparator[keyPositions.length];
		final KeyComparator kk = new KeyComparator();
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = kk;
		}
		countingReader = new CountingMutableObjectIterator(inputs[0]);
		
		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy()) {

			// local strategy is NONE
			// input is already grouped, an iterator that wraps the reader is
			// created and returned
			case NONE: 
				//throw new UnsupportedOperationException("Histogram input has to be sorted");
				// iterator wraps input reader
				 this.input = new SimpleCloseableInputProvider<PactRecord>(countingReader);
				 break;
				// local strategy is SORT
				// The input is grouped using a sort-merge strategy.
				// An iterator on the sorted pairs is created and returned.
			case SORT: 
				this.input = new UnilateralSortMerger(memoryManager, ioManager, availableMemory, maxFileHandles, comparators, 
						keyPositions, keyClasses, countingReader, this, spillThreshold);
				break;
			default:
				throw new RuntimeException("Invalid local strategy provided for ReduceTask.");
			}
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		// cache references on the stack
		final MutableObjectIterator<PactRecord> input = this.inputs[0];
		final OutputCollector output = this.output;
		
		final PactRecord record = new PactRecord();
		
		while (this.running && input.next(record)) {
			int recordCount = countingReader.getCount();
			int bucketSize = recordCount / numBuckets;
			for (int i = 0; i < recordCount; i++) {
				if(i%bucketSize == 0 && i/bucketSize != 0 && i/bucketSize != numBuckets) {
					output.collect(record);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		if (this.input != null) {
			this.input.close();
			this.input = null;
		}
	}

	private static class CountingMutableObjectIterator implements MutableObjectIterator<PactRecord> {
		private int count = 0;
		private MutableObjectIterator<PactRecord> delegatingIterator;
		public CountingMutableObjectIterator(MutableObjectIterator<PactRecord> delegatingIterator) {
			this.delegatingIterator = delegatingIterator;
		}
		
		public int getCount() {
			return count;
		}

		@Override
		public boolean next(PactRecord target) throws IOException {
			if (delegatingIterator.next(target) == true)
			{
				count++;
				return true;
			}
			
			return false;
		}
		
		
		
		
	}
}
