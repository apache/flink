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
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.resettable.BlockResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.sort.SortMergeMatchIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.SerializationCopier;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.LastRepeatableIterator;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * Cross task which is executed by a Nephele task manager. The task has two
 * inputs and one or multiple outputs. It is provided with a CrossStub
 * implementation.
 * <p>
 * The CrossTask builds the Cartesian product of the pairs of its two inputs. Each element (pair of pairs) is handed to
 * the <code>cross()</code> method of the CrossStub.
 * 
 * @see eu.stratosphere.pact.common.stub.CrossStub
 * @author Fabian Hueske
 * @author Matthias Ringwald
 */
public class CrossTask extends AbstractPactTask<CrossStub>
{
	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 1 * 1024 * 1024;

	// the iterator that does the actual matching
	//private CrossT matchIterator;
	private boolean runBlocked = true;
	
	private long availableMemory;
	private int maxFileHandles;
	private float spillThreshold;

	
	// ------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<CrossStub> getStubType() {
		return CrossStub.class;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		// set up memory and I/O parameters
		availableMemory = this.config.getMemorySize();
		maxFileHandles = this.config.getNumFilehandles();
		spillThreshold = this.config.getSortSpillingTreshold();
		
		// test minimum memory requirements
		final LocalStrategy ls = this.config.getLocalStrategy();
		long strategyMinMem = 0;
		
		switch (ls) {
			case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			case NESTEDLOOP_BLOCKED_OUTER_SECOND: 
			case NESTEDLOOP_STREAMED_OUTER_FIRST: 
			case NESTEDLOOP_STREAMED_OUTER_SECOND: 
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
		}
	
		if (availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The Cross task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}
		
		// assign inner and outer readers according to local strategy decision
		if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST
			|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
			MutableObjectIterator<PactRecord> tempInput = inputs[0];
			inputs[0] = inputs[1];
			inputs[1] = tempInput;
		} else if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND
				|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
			
		}
		else {
			throw new RuntimeException("Invalid local strategy for CROSS: " + config.getLocalStrategy());
		}

		// create and return MatchTaskIterator according to provided local strategy.
		switch (ls)
		{
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			//run Blocked
			runBlocked = true;
		default:
			//run Streamed
			runBlocked = false;
		}
		
		// open MatchTaskIterator - this triggers the sorting or hash-table building
		// and blocks until the iterator is ready
		//this.matchIterator.open();
		
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Match task iterator ready."));
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		/*final MatchStub matchStub = this.stub;
		final OutputCollector collector = this.output;
		final MatchTaskIterator matchIterator = this.matchIterator;
		
		while (this.running && matchIterator.callWithNextKey(matchStub, collector));*/
		if (runBlocked == true) {
			runBlocked();
		}
		else {
			runStreamed();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception
	{
	/*	if (this.matchIterator != null) {
			this.matchIterator.close();
			this.matchIterator = null;
		}*/
	}
	
	
	
	
	
	
	
	
	
	
	


	/**
	 * Runs a blocked nested loop strategy to build the Cartesian product and
	 * call the <code>cross()</code> method of the CrossStub implementation. The
	 * outer side is read using a BlockResettableIterator. The inner side is
	 * read using a SpillingResettableIterator.
	 * 
	 * @see eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator
	 * @see eu.stratosphere.pact.runtime.resettable.BlockResettableMutableObjectIterator
	 * @param memoryManager
	 *        The task manager's memory manager.
	 * @param ioManager
	 *        The task manager's IO manager
	 * @param innerReader
	 *        The inner reader of the nested loops.
	 * @param outerReader
	 *        The outer reader of the nested loops.
	 * @throws RuntimeException
	 *         Throws a RuntimeException if something fails during
	 *         execution.
	 */
	private void runBlocked()
			throws Exception {

		
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();  
		final IOManager ioManager = getEnvironment().getIOManager();
		final MutableObjectIterator<PactRecord> innerReader = inputs[0]; 
		final MutableObjectIterator<PactRecord> outerReader = inputs[1];
		
		// spilling iterator for inner side
		SpillingResettableIterator<PactRecord> innerInput = null;
		// blocked iterator for outer side
		BlockResettableMutableObjectIterator outerInput = null;

		try {
			final boolean firstInputIsOuter;
		
			// obtain iterators according to local strategy decision
			if (this.config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
				// obtain spilling iterator (inner side) for first input
				try {
					innerInput = new SpillingResettableIterator<PactRecord>(memoryManager, ioManager,
						innerReader, availableMemory / 2, this);
					this.spillingResetIt = innerInput;
				}
				catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettableIterator for first input", mae);
				}
				// obtain blocked iterator (outer side) for second input
				try {
					outerInput = new BlockResettableMutableObjectIterator<KeyValuePair<Key, Value>>(memoryManager,
							outerReader,
							this.availableMemory / 2, 1, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(), 
								stub.getSecondInValueType()), this);
					this.blockResetIt = outerInput;
				}
				catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain BlockResettableIterator for second input", mae);
				}
				firstInputIsOuter = false;
			}
			else if (this.config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST) {
				// obtain spilling iterator (inner side) for second input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, this.availableMemory / 2, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(),
							stub.getSecondInValueType()), this);
					this.spillingResetIt = innerInput;
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettableIterator for second input", mae);
				}
				// obtain blocked iterator (outer side) for second input
				try {
					outerInput = new BlockResettableMutableObjectIterator<KeyValuePair<Key, Value>>(memoryManager, outerReader,
							this.availableMemory / 2, 1, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
							.getFirstInValueType()), this);
					this.blockResetIt = outerInput;
				}
				catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain BlockResettableIterator for first input", mae);
				}
				firstInputIsOuter = true;
			}
			else {
				throw new RuntimeException("Invalid local strategy for CrossTask: " + config.getLocalStrategy());
			}
	
			// open spilling resettable iterator
			try {
				innerInput.open();
			}
			catch (ServiceException se) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", se);
			}
			catch (IOException ioe) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", ioe);
			}
			catch (InterruptedException ie) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", ie);
			}
			
			// check if task was canceled while data was read
			if (this.taskCanceled)
				return;
			
			// open blocked resettable iterator
			outerInput.open();
	
			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("SpillingResettable iterator obtained"));
				LOG.debug(getLogString("BlockResettable iterator obtained"));
			}
	
			// open stub implementation
			this.stub.open();
	
			boolean moreOuterBlocks = false;

			if (innerInput.hasNext()) { // avoid painful work when one input is empty
				do {
					// loop over the spilled resettable iterator
					while (!this.taskCanceled && innerInput.hasNext()) {
						// get inner pair
						KeyValuePair<Key, Value> innerPair = innerInput.next();
						// loop over the pairs in the current memory block
						while (!this.taskCanceled && outerInput.hasNext()) {
							// get outer pair
							KeyValuePair<Key, Value> outerPair = outerInput.next();
		
							// call cross() method of CrossStub depending on local strategy
							if(firstInputIsOuter) {
								stub.cross(outerPair.getKey(), outerPair.getValue(), innerPair.getKey(), innerPair.getValue(), output);
							} else {
								stub.cross(innerPair.getKey(), innerPair.getValue(), outerPair.getKey(), outerPair.getValue(), output);
							}
	
							innerPair = innerInput.repeatLast();
						}
						// reset the memory block iterator to the beginning of the
						// current memory block (outer side)
						outerInput.reset();
					}
					// reset the spilling resettable iterator (inner side)
					moreOuterBlocks = outerInput.nextBlock();
					if(moreOuterBlocks) {
						innerInput.reset();
					}
				} while (!this.taskCanceled && moreOuterBlocks);
			}
				
			// close stub implementation
			this.stub.close();
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				LOG.error(getLogString("Unexpected ERROR in PACT code"));
				throw ex;
			}
		}
		finally {
			Throwable t1 = null, t2 = null;
			try {
				if(innerInput != null) {
					innerInput.close();
				}
			}
			catch (Throwable t) {
				LOG.warn(t);
				t1 = t;
			}
			
			try {
				if(outerInput != null) {
					outerInput.close();
				}
			}
			catch (Throwable t) {
				LOG.warn(t);
				t2 = t;
			}
			
			if(t1 != null) throw new RuntimeException("Error closing SpillingResettableIterator.", t1);
			if(t2 != null) throw new RuntimeException("Error closung BlockResettableIterator.", t2);
		}
	}

	/**
	 * Runs a streamed nested loop strategy to build the Cartesian product and
	 * call the <code>cross()</code> method of the CrossStub implementation.
	 * The outer side is read directly from the input reader. The inner side is
	 * read and reseted using a SpillingResettableIterator.
	 * 
	 * @see eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator
	 * @param memoryManager
	 *        The task manager's memory manager.
	 * @param ioManager
	 *        The task manager's IO manager
	 * @param innerReader
	 *        The inner reader of the nested loops.
	 * @param outerReader
	 *        The outer reader of the nested loops.
	 * @throws RuntimeException
	 *         Throws a RuntimeException if something fails during
	 *         execution.
	 */
	private void runStreamed()
			throws Exception {

		final MemoryManager memoryManager = getEnvironment().getMemoryManager();  
		final IOManager ioManager = getEnvironment().getIOManager();
		final MutableObjectIterator<PactRecord> innerReader = inputs[0]; 
		final MutableObjectIterator<PactRecord> outerReader = inputs[1];
		
		// obtain streaming iterator for outer side
		// streaming is achieved by simply wrapping the input reader of the outer side
		
		// obtain SpillingResettableIterator for inner side
		SpillingResettableIterator<KeyValuePair<Key, Value>> innerInput = null;
		RepeatableIterator outerInput = null;
		
		try {
		
			final boolean firstInputIsOuter;
			
			if(config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
				// obtain spilling resettable iterator for first input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, this.availableMemory, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(), stub
							.getSecondInValueType()), this);
					spillingResetIt = innerInput;
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettable iterator for inner side.", mae);
				}
				// obtain repeatable iterator for second input
				outerInput = new RepeatableIterator(outerReader, stub.getFirstInKeyType(), stub.getFirstInValueType());
				
				firstInputIsOuter = true;
			} else if(config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
				// obtain spilling resettable iterator for second input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, this.availableMemory, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
							.getFirstInValueType()), this);
					spillingResetIt = innerInput;
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettable iterator for inner side.", mae);
				}
				// obtain repeatable iterator for first input
				outerInput = new RepeatableIterator(outerReader, stub.getSecondInKeyType(), stub.getSecondInValueType());
				
				firstInputIsOuter = false;
			} else {
				throw new RuntimeException("Invalid local strategy for CrossTask: " + config.getLocalStrategy());
			}
			
			// open spilling resettable iterator
			try {
				innerInput.open();
			} catch (ServiceException se) {
				throw new RuntimeException("Unable to open SpillingResettable iterator for inner side.", se);
			} catch (IOException ioe) {
				throw new RuntimeException("Unable to open SpillingResettable iterator for inner side.", ioe);
			} catch (InterruptedException ie) {
				throw new RuntimeException("Unable to open SpillingResettable iterator for inner side.", ie);
			}
			
			
			if (this.taskCanceled)
				return;
	
			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Resetable iterator obtained"));
	
			// open stub implementation
			stub.open();
			
			// if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
			
			// read streamed iterator of outer side
			while (!this.taskCanceled && outerInput.hasNext()) {
				// get outer pair
				KeyValuePair outerPair = outerInput.next();
	
				// read spilled iterator of inner side
				while (!this.taskCanceled && innerInput.hasNext()) {
					// get inner pair
					KeyValuePair innerPair = innerInput.next();
	
					// call cross() method of CrossStub depending on local strategy
					if(firstInputIsOuter) {
						stub.cross(outerPair.getKey(), outerPair.getValue(), innerPair.getKey(), innerPair.getValue(), output);
					} else {
						stub.cross(innerPair.getKey(), innerPair.getValue(), outerPair.getKey(), outerPair.getValue(), output);
					}
					
					outerPair = outerInput.repeatLast();
				}
				// reset spilling resettable iterator of inner side
				if(!this.taskCanceled && outerInput.hasNext()) {
					innerInput.reset();
				}
			}
			
			// close stub implementation
			stub.close();
		
		}
		catch (Exception ex) {
			
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				LOG.error(getLogString("Unexpected ERROR in PACT code"));
				throw ex;
			}
		}
		finally {
			// close spilling resettable iterator
			if(innerInput != null) {
				innerInput.close();
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Utility class that turns a standard {@link java.util.Iterator} for key/value pairs into a
	 * {@link LastRepeatableIterator}. 
	 */
	private static final class RepeatableIterator implements LastRepeatableIterator<KeyValuePair<Key, Value>>
	{
		private final SerializationCopier<KeyValuePair<Key, Value>> copier = new SerializationCopier<KeyValuePair<Key,Value>>();
		
		private final KeyValuePairDeserializer<Key, Value> deserializer;
		
		private final Iterator<KeyValuePair<Key,Value>> input;
		
		public RepeatableIterator(Iterator<KeyValuePair<Key,Value>> input, Class<Key> keyClass, Class<Value> valClass) {
			this.input = input;
			this.deserializer = new KeyValuePairDeserializer<Key, Value>(keyClass, valClass);
		}
		
		@Override
		public boolean hasNext() {
			return this.input.hasNext();
		}

		@Override
		public KeyValuePair<Key, Value> next() {
			KeyValuePair<Key,Value> pair = this.input.next();
			// serialize pair
			this.copier.setCopy(pair);	
			return pair;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public KeyValuePair<Key, Value> repeatLast() {
			KeyValuePair<Key,Value> pair = this.deserializer.getInstance();
			this.copier.getCopy(pair);	
			return pair;
		}
	}
	
}
