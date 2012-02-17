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

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

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
 * @author Matthias Ringwald
 */
@SuppressWarnings({"unchecked"})
public class SelfMatchTask extends AbstractPactTask<MatchStub> {

	public static final String SELFMATCH_CROSS_MODE_KEY = "selfMatch.crossMode";
	
	public static enum CrossMode {
		FULL_CROSS,
		TRIANGLE_CROSS_INCL_DIAG,
		TRIANGLE_CROSS_EXCL_DIAG
	}
	
	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;
	
	// share ratio for resettable iterator
	private static final double MEMORY_SHARE_RATIO = 0.10;
	
	// size of value buffer in elements
	private static final int VALUE_BUFFER_SIZE = 32;
	
	private long availableMemory;
	
	// obtain the TaskManager's MemoryManager
	private MemoryManager memoryManager;
	// obtain the TaskManager's IOManager
	private IOManager ioManager;
	
	// used for tracking of exceptions for matching values in valReader
	private Exception exceptionInMatchForValReader = null;

	private int[] keyPositions;
	private Class<? extends Key>[] keyClasses;
	
	private CloseableInputProvider<PactRecord> closeableInput;
	private SpillingResettableMutableObjectIterator outerValResettableIterator = null;
	private SpillingResettableMutableObjectIterator innerValResettableIterator = null;
	
	private CrossMode crossMode;
	
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
		availableMemory = this.config.getMemorySize();
		final int maxFileHandles = this.config.getNumFilehandles();
		final float spillThreshold = this.config.getSortSpillingTreshold();
		
		
		// test minimum memory requirements
		final LocalStrategy ls = this.config.getLocalStrategy();
		long strategyMinMem = 0;
		
		String crossModeS = this.config.getStubParameter(SELFMATCH_CROSS_MODE_KEY, null);
		if(crossModeS == null) {
			this.crossMode = CrossMode.FULL_CROSS;
		} else if(crossModeS.equals(CrossMode.FULL_CROSS.toString())) {
			this.crossMode = CrossMode.FULL_CROSS;
		} else if(crossModeS.equals(CrossMode.TRIANGLE_CROSS_INCL_DIAG.toString())) {
			this.crossMode = CrossMode.TRIANGLE_CROSS_INCL_DIAG;
		} else if(crossModeS.equals(CrossMode.TRIANGLE_CROSS_EXCL_DIAG.toString())) {
			this.crossMode = CrossMode.TRIANGLE_CROSS_EXCL_DIAG;
		} else {
			throw new IllegalArgumentException("Invalid Cross Mode: "+crossModeS);
		}
		
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
		memoryManager = getEnvironment().getMemoryManager();
		// obtain the TaskManager's IOManager
		ioManager = getEnvironment().getIOManager();
		
		keyPositions = this.config.getLocalStrategyKeyPositions(0);
		keyClasses = this.config.getLocalStrategyKeyClasses(this.userCodeClassLoader);
		
		if (keyPositions == null || keyClasses == null) {
			throw new Exception("The key positions and types are not specified for the SelfMatchTask.");
		}
		if (keyPositions.length != keyClasses.length) {
			throw new Exception("The number of key positions and types does not match in the configuration");
		}
		
		// create the comparators
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
			switch(this.crossMode) {
			case FULL_CROSS:
				fullCross(it.getValues(), output);
				break;
			case TRIANGLE_CROSS_INCL_DIAG:
				diagInclTriangleCross(it.getValues(), output);
				break;
			case TRIANGLE_CROSS_EXCL_DIAG:
				diagExclTriangleCross(it.getValues(), output);
				break;
			default:
				throw new IllegalArgumentException("Invalid Cross Mode");
			}
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		super.cancel();
		if (this.innerValResettableIterator != null) {
			innerValResettableIterator.abort();
		}
		if (this.outerValResettableIterator != null) {
			outerValResettableIterator.abort();
		}
	}

	// ------------------------------------------------------------------------
	
	/**
	 * Crosses the values of all pairs that have the same key.
	 * The {@link MatchStub#match(Key, Iterator, Collector)} method is called for each element of the 
	 * Cartesian product. 
	 * 
	 * @param values 
	 *        An iterator over values that share the same key.
	 * @param out
	 *        The collector to write the results to.
	 * @throws Exception 
	 */
	private final void fullCross(final Iterator<PactRecord> values, final Collector out) throws Exception
	{
		// allocate buffer
		final PactRecord[] valBuffer = new PactRecord[VALUE_BUFFER_SIZE];
		
		// DW: Start of temporary code
		final Environment env = getEnvironment();
		final OutputCollector oc = (OutputCollector) out;
		// DW: End of temporary code
		
		// fill value buffer for the first time
		int bufferValCnt;
		for(bufferValCnt = 0; bufferValCnt < VALUE_BUFFER_SIZE; bufferValCnt++) {
			if(values.hasNext()) {
				// read value into buffer
				valBuffer[bufferValCnt] = values.next().createCopy();
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
				
				// DW : Start of temporary code
				final PactRecord copy1 = valBuffer[i].createCopy();
				final PactRecord copy2 =  valBuffer[j].createCopy();
				final long r1 = copy1.getBinaryLength();
				final long r2 = copy2.getBinaryLength();
				// DW: End of temporary code
				
				stub.match(copy1, copy2, out);
				
				// DW: Start of temporary code
				env.reportPACTDataStatistics(r1 + r2,  
					oc.getCollectedPactRecordsInBytes());
				// DW: End of temporary code
			}
			
		}
		
		if(this.running && values.hasNext()) {
			// there are still value in the reader

			// wrap value iterator in a reader
			MutableObjectIterator<PactRecord> valReader = new MutableObjectIterator<PactRecord>() {
				@Override
				public boolean next(PactRecord target) throws IOException {
					if (!running || !values.hasNext()) {
						return false;
					}
					values.next().copyTo(target);
					
					for(int i=0;i<VALUE_BUFFER_SIZE;i++) {
						try {
							
							// DW : Start of temporary code
							final PactRecord copy1 = valBuffer[i].createCopy();
							final PactRecord copy2 =  target.createCopy();
							final long r1 = copy1.getBinaryLength();
							final long r2 = copy2.getBinaryLength();
							// DW: End of temporary code
							
							stub.match(copy1, copy2 ,out);
							
							// DW: Start of temporary code
							env.reportPACTDataStatistics(r1 + r2,  
								oc.getCollectedPactRecordsInBytes());
							// DW: End of temporary code
							
						} catch (Exception e) {
							exceptionInMatchForValReader = e;
							return false;
						}
					}
					
					return true; 
				}
			};
			
			outerValResettableIterator = null;
			innerValResettableIterator = null;
			
			try {
				// read values into outer resettable iterator
				outerValResettableIterator =
						new SpillingResettableMutableObjectIterator(memoryManager, ioManager, valReader,  (long) (availableMemory * (MEMORY_SHARE_RATIO/2)), this);
				outerValResettableIterator.open();
				if (exceptionInMatchForValReader != null) {
					throw exceptionInMatchForValReader;
				}

				// iterator returns first buffer then outer resettable iterator (all values of the incoming iterator)
				BufferIncludingIterator bii = new BufferIncludingIterator(valBuffer, outerValResettableIterator);
				
				PactRecord outerRecord = new PactRecord();
				PactRecord innerRecord = new PactRecord();
				// read remaining values into inner resettable iterator
				if(this.running) {
					innerValResettableIterator =
						new SpillingResettableMutableObjectIterator(memoryManager, ioManager, bii, (long) (availableMemory * (MEMORY_SHARE_RATIO/2)), this);							
					innerValResettableIterator.open();
					
					// reset outer iterator
					outerValResettableIterator.reset();
				
					// cross remaining values
					while(this.running && outerValResettableIterator.next(outerRecord)) {
						
						// fill buffer with next elements from outer resettable iterator
						bufferValCnt = 0;
						do {
							outerRecord.copyTo(valBuffer[bufferValCnt++]);
						} while(this.running && bufferValCnt < VALUE_BUFFER_SIZE && outerValResettableIterator.next(outerRecord));
						if(bufferValCnt == 0) break;
						
						// cross buffer with inner iterator
						while(this.running && innerValResettableIterator.next(innerRecord)) {
							
							for(int i=0;i<bufferValCnt;i++) {
								
								// DW : Start of temporary code
								final PactRecord copy1 = valBuffer[i].createCopy();
								final long r1 = copy1.getBinaryLength();
								final long r2 = innerRecord.getBinaryLength();
								// DW: End of temporary code
								
								stub.match(copy1, innerRecord, out);
								
								// DW: Start of temporary code
								env.reportPACTDataStatistics(r1 + r2,  
									oc.getCollectedPactRecordsInBytes());
								// DW: End of temporary code
								
								if(i < bufferValCnt - 1)
									innerValResettableIterator.repeatLast(innerRecord);
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
	
	/**
	 * Enumerates an triangle of the Carthesian product including its diagonal.
	 * The {@link MatchStub#match(Key, Iterator, Collector)} method is called for each element of the 
	 * triangle. 
	 * 
	 * @param values 
	 *        An iterator over values that share the same key.
	 * @param out
	 *        The collector to write the results to.
	 * @throws Exception 
	 */
	private final void diagInclTriangleCross(final Iterator<PactRecord> values, final Collector out) throws Exception
	{
		
		// allocate buffer
		final PactRecord[] valBuffer = new PactRecord[VALUE_BUFFER_SIZE];
		
		// DW: Start of temporary code
		final Environment env = getEnvironment();
		final OutputCollector oc = (OutputCollector) out;
		// DW: End of temporary code
		
		// fill value buffer for the first time
		int bufferValCnt;
		for(bufferValCnt = 0; bufferValCnt < VALUE_BUFFER_SIZE; bufferValCnt++) {
			if(values.hasNext()) {
				// read value into buffer
				valBuffer[bufferValCnt] = values.next().createCopy();
			} else {
				break;
			}
		}
		
		// enumerate triangle of values in buffer
		for (int i = 0;i < bufferValCnt; i++) {
			
			// check if task was canceled
			if (!this.running) return;
			
			for (int j = i; j < bufferValCnt; j++) {
				// check if task was canceled
				if (!this.running) return;
				
				// match
				
				// DW : Start of temporary code
				final PactRecord copy1 = valBuffer[i].createCopy();
				final PactRecord copy2 =  valBuffer[j].createCopy();
				final long r1 = copy1.getBinaryLength();
				final long r2 = copy2.getBinaryLength();
				// DW: End of temporary code
				
				stub.match(copy1, copy2, out);
				
				// DW: Start of temporary code
				env.reportPACTDataStatistics(r1 + r2,  
					oc.getCollectedPactRecordsInBytes());
				// DW: End of temporary code
			}
			
		}
		
		if(this.running && values.hasNext()) {
			// there are still value in the reader

			// wrap value iterator in a reader
			MutableObjectIterator<PactRecord> valReader = new MutableObjectIterator<PactRecord>() {
				@Override
				public boolean next(PactRecord target) throws IOException {
					if (!running || !values.hasNext()) {
						return false;
					}
					values.next().copyTo(target);
					
					for(int i=0;i<VALUE_BUFFER_SIZE;i++) {
						try {
							
							// DW : Start of temporary code
							final PactRecord copy1 = valBuffer[i].createCopy();
							final PactRecord copy2 = target.createCopy();
							final long r1 = copy1.getBinaryLength();
							final long r2 = copy2.getBinaryLength();
							// DW: End of temporary code
							
							stub.match(copy1,copy2,out);
							
							// DW: Start of temporary code
							env.reportPACTDataStatistics(r1 + r2,  
								oc.getCollectedPactRecordsInBytes());
							// DW: End of temporary code
							
						} catch (Exception e) {
							exceptionInMatchForValReader = e;
							return false;
						}
					}
					
					return true; 
				}
			};
			
			outerValResettableIterator = null;
			innerValResettableIterator = null;
			
			try {
				// read values into outer resettable iterator
				outerValResettableIterator =
						new SpillingResettableMutableObjectIterator(memoryManager, ioManager, valReader,  (long) (availableMemory * (MEMORY_SHARE_RATIO/2)), this);
				outerValResettableIterator.open();
				if (exceptionInMatchForValReader != null) {
					throw exceptionInMatchForValReader;
				}

				PactRecord outerRecord = new PactRecord();
				PactRecord innerRecord = new PactRecord();
				// read remaining values into inner resettable iterator
				if(this.running) {
					innerValResettableIterator =
						new SpillingResettableMutableObjectIterator(memoryManager, ioManager, outerValResettableIterator, (long) (availableMemory * (MEMORY_SHARE_RATIO/2)), this);							
					innerValResettableIterator.open();
					
					// reset outer iterator
					outerValResettableIterator.reset();
					
					int outerConsumedRecordCnt = 0;
					bufferValCnt = 0;
					// enumerate triangle of remaining values
					while(this.running && outerValResettableIterator.next(outerRecord)) {
						
						// fill buffer with next elements from outer resettable iterator
						outerConsumedRecordCnt += bufferValCnt;
						bufferValCnt = 0;
						do {
							outerRecord.copyTo(valBuffer[bufferValCnt++]);
						} while(this.running && bufferValCnt < VALUE_BUFFER_SIZE && outerValResettableIterator.next(outerRecord));
						if(bufferValCnt == 0) break;
						
						int innerConsumedRecordCnt = 0;
						
						// enumerate triangle of buffer and inner iterator
						while(this.running && innerValResettableIterator.next(innerRecord)) {
							
							for(int i=0;i<bufferValCnt;i++) {
								
								if(outerConsumedRecordCnt+i <= innerConsumedRecordCnt) {
									
									// DW : Start of temporary code
									final PactRecord copy1 = valBuffer[i].createCopy();
									final long r1 = copy1.getBinaryLength();
									final long r2 = innerRecord.getBinaryLength();
									// DW: End of temporary code
									
									stub.match(copy1, innerRecord, out);
									// DW: Start of temporary code
									env.reportPACTDataStatistics(r1 + r2,  
										oc.getCollectedPactRecordsInBytes());
									// DW: End of temporary code
									
									if(i < bufferValCnt - 1)
										innerValResettableIterator.repeatLast(innerRecord);
								}
							}
							
							innerConsumedRecordCnt++;
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
	
	/**
	 * Enumerates an triangle of the Carthesian product excluding its diagonal.
	 * The {@link MatchStub#match(Key, Iterator, Collector)} method is called for each element of the 
	 * triangle. 
	 * 
	 * @param values 
	 *        An iterator over values that share the same key.
	 * @param out
	 *        The collector to write the results to.
	 * @throws Exception 
	 */
	private final void diagExclTriangleCross(final Iterator<PactRecord> values, final Collector out) throws Exception
	{
		
		// allocate buffer
		final PactRecord[] valBuffer = new PactRecord[VALUE_BUFFER_SIZE];
		
		// DW: Start of temporary code
		final Environment env = getEnvironment();
		final OutputCollector oc = (OutputCollector) out;
		// DW: End of temporary code
		
		// fill value buffer for the first time
		int bufferValCnt;
		for(bufferValCnt = 0; bufferValCnt < VALUE_BUFFER_SIZE; bufferValCnt++) {
			if(values.hasNext()) {
				// read value into buffer
				valBuffer[bufferValCnt] = values.next().createCopy();
			} else {
				break;
			}
		}
		
		// enumerate the triangle of the values in buffer
		for (int i = 0;i < bufferValCnt; i++) {
			
			// check if task was canceled
			if (!this.running) return;
			
			for (int j = i+1; j < bufferValCnt; j++) {
				// check if task was canceled
				if (!this.running) return;
				
				// match
				
				// DW : Start of temporary code
				final PactRecord copy1 = valBuffer[i].createCopy();
				final PactRecord copy2 =  valBuffer[j].createCopy();
				final long r1 = copy1.getBinaryLength();
				final long r2 = copy2.getBinaryLength();
				// DW: End of temporary code
				
				stub.match(copy1, copy2, out);
				
				// DW: Start of temporary code
				env.reportPACTDataStatistics(r1 + r2,  
					oc.getCollectedPactRecordsInBytes());
				// DW: End of temporary code
				
				
			}
			
		}
		
		if(this.running && values.hasNext()) {
			// there are still value in the reader

			// wrap value iterator in a reader
			MutableObjectIterator<PactRecord> valReader = new MutableObjectIterator<PactRecord>() {
				@Override
				public boolean next(PactRecord target) throws IOException {
					if (!running || !values.hasNext()) {
						return false;
					}
					values.next().copyTo(target);
					
					for(int i=0;i<VALUE_BUFFER_SIZE;i++) {
						try {
							
							// DW : Start of temporary code
							final PactRecord copy1 = valBuffer[i].createCopy();
							final PactRecord copy2 =  target.createCopy();
							final long r1 = copy1.getBinaryLength();
							final long r2 = copy2.getBinaryLength();
							// DW: End of temporary code
							
							stub.match(copy1,copy2,out);
							
							// DW: Start of temporary code
							env.reportPACTDataStatistics(r1 + r2,  
								oc.getCollectedPactRecordsInBytes());
							// DW: End of temporary code
							
						} catch (Exception e) {
							exceptionInMatchForValReader = e;
							return false;
						}
					}
					
					return true; 
				}
			};
			
			outerValResettableIterator = null;
			innerValResettableIterator = null;
			
			try {
				// read values into outer resettable iterator
				outerValResettableIterator =
						new SpillingResettableMutableObjectIterator(memoryManager, ioManager, valReader,  (long) (availableMemory * (MEMORY_SHARE_RATIO/2)), this);
				outerValResettableIterator.open();
				if (exceptionInMatchForValReader != null) {
					throw exceptionInMatchForValReader;
				}

				PactRecord outerRecord = new PactRecord();
				PactRecord innerRecord = new PactRecord();
				// read remaining values into inner resettable iterator
				if(this.running) {
					innerValResettableIterator =
						new SpillingResettableMutableObjectIterator(memoryManager, ioManager, outerValResettableIterator, (long) (availableMemory * (MEMORY_SHARE_RATIO/2)), this);							
					innerValResettableIterator.open();
					
					// reset outer iterator
					outerValResettableIterator.reset();
					
					int outerConsumedRecordCnt = 0;
					bufferValCnt = 0;
					// build triangle over remaining values
					while(this.running && outerValResettableIterator.next(outerRecord)) {
						
						// fill buffer with next elements from outer resettable iterator
						outerConsumedRecordCnt += bufferValCnt;
						bufferValCnt = 0;
						do {
							outerRecord.copyTo(valBuffer[bufferValCnt++]);
						} while(this.running && bufferValCnt < VALUE_BUFFER_SIZE && outerValResettableIterator.next(outerRecord));
						if(bufferValCnt == 0) break;
						
						int innerConsumedRecordCnt = 0;
						
						// enumerate triangle of buffer and inner iterator
						while(this.running && innerValResettableIterator.next(innerRecord)) {
							
							for(int i=0;i<bufferValCnt;i++) {
								
								if(outerConsumedRecordCnt+i < innerConsumedRecordCnt) {
									
									// DW : Start of temporary code
									final PactRecord copy1 = valBuffer[i].createCopy();
									final long r1 = copy1.getBinaryLength();
									final long r2 = innerRecord.getBinaryLength();
									// DW: End of temporary code
									
									stub.match(copy1, innerRecord, out);
									
									// DW: Start of temporary code
									env.reportPACTDataStatistics(r1 + r2,  
										oc.getCollectedPactRecordsInBytes());
									// DW: End of temporary code
									
									if(i < bufferValCnt - 1)
										innerValResettableIterator.repeatLast(innerRecord);
								}
							}
							
							innerConsumedRecordCnt++;
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
	
	private final class BufferIncludingIterator implements MutableObjectIterator<PactRecord> {

		int bufferIdx = 0;
		
		private PactRecord[] valBuffer;
		private MutableObjectIterator<PactRecord> valIterator;
		
		public BufferIncludingIterator(PactRecord[] valBuffer, MutableObjectIterator<PactRecord> valIterator) {
			this.valBuffer = valBuffer;
			this.valIterator = valIterator;
		}
		
		@Override
		public boolean next(PactRecord target) throws IOException {
			if (!running) {
				return false;
			}
			if(bufferIdx < VALUE_BUFFER_SIZE) {
				valBuffer[bufferIdx++].copyTo(target);
				return true;
			}
			return valIterator.next(target);
		}
		
	};
}
