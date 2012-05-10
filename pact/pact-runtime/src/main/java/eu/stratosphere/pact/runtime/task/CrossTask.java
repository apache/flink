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

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.generic.GenericCrosser;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.BlockResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.LastRepeatableIterator;
import eu.stratosphere.pact.runtime.util.LastRepeatableMutableObjectIterator;

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
public class CrossTask<IT1, IT2, OT> extends AbstractPactTask<GenericCrosser<IT1, IT2, OT>, OT>
{
	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 1 * 1024 * 1024;

	private boolean runBlocked = true;
	
	private long availableMemory;
	
	// spilling iterator for inner side
	private SpillingResettableMutableObjectIterator innerInput;
	// blocked iterator for outer side
	private BlockResettableMutableObjectIterator outerInput;

	
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

		switch (ls)
		{
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			//run Blocked
			runBlocked = true;
			break;
		default:
			//run Streamed
			runBlocked = false;
		}
		
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Match task iterator ready."));
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
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
		if (this.innerInput != null) {
			innerInput.close();
			this.innerInput = null;
		}
		if (this.outerInput != null) {
			outerInput.close();
			this.outerInput = null;
		}
		
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		super.cancel();
		if (this.innerInput != null) {
			innerInput.abort();
		}
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
		innerInput = null;
		// blocked iterator for outer side
		outerInput = null;

		final boolean firstInputIsOuter;
	
		// obtain iterators according to local strategy decision
		if (this.config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
			// obtain spilling iterator (inner side) for first input
			innerInput = new SpillingResettableMutableObjectIterator(memoryManager, 
				ioManager, innerReader, availableMemory / 2, this);
			// obtain blocked iterator (outer side) for second input
			outerInput = new BlockResettableMutableObjectIterator(memoryManager, outerReader, availableMemory / 2, 1, this);
			firstInputIsOuter = false;
		}
		else if (this.config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST) {
			// obtain spilling iterator (inner side) for second input
			innerInput = new SpillingResettableMutableObjectIterator(memoryManager, ioManager, 
					innerReader, availableMemory / 2, this);
			// obtain blocked iterator (outer side) for second input
			outerInput = new BlockResettableMutableObjectIterator(memoryManager, outerReader, availableMemory / 2, 1, this);
			firstInputIsOuter = true;
		}
		else {
			throw new RuntimeException("Invalid local strategy for CrossTask: " + config.getLocalStrategy());
		}

		// open spilling resettable iterator
		innerInput.open();
		
		// check if task was canceled while data was read
		if (!this.running)
			return;
		
		// open blocked resettable iterator
		outerInput.open();

		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("SpillingResettable iterator obtained"));
			LOG.debug(getLogString("BlockResettable iterator obtained"));
		}

		boolean moreOuterBlocks = false;

		PactRecord innerRecord = new PactRecord();
		PactRecord outerRecord = new PactRecord();
		if (innerInput.next(innerRecord)) { // avoid painful work when one input is empty
			innerInput.reset();
			do {
				// loop over the spilled resettable iterator
				while (this.running && innerInput.next(innerRecord)) {
					// loop over the pairs in the current memory block
					while (this.running && outerInput.next(outerRecord)) {
	
						// call cross() method of CrossStub depending on local strategy
						if(firstInputIsOuter) {
							stub.cross(outerRecord, innerRecord, output);
						} else {
							stub.cross(innerRecord, outerRecord, output);
						}

						innerInput.repeatLast(innerRecord);
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
			} while (this.running && moreOuterBlocks);
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
	private void runStreamed() throws Exception {

		final MemoryManager memoryManager = getEnvironment().getMemoryManager();  
		final IOManager ioManager = getEnvironment().getIOManager();
		final MutableObjectIterator<PactRecord> innerReader = inputs[0]; 
		final MutableObjectIterator<PactRecord> outerReader = inputs[1];
		
		// obtain streaming iterator for outer side
		// streaming is achieved by simply wrapping the input reader of the outer side
		
		// obtain SpillingResettableIterator for inner side
		LastRepeatableMutableObjectIterator<PactRecord> repeatableInput = null;
		
		final boolean firstInputIsOuter;
		
		if(config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
			// obtain spilling resettable iterator for first input
			innerInput = new SpillingResettableMutableObjectIterator(memoryManager, 
					ioManager, innerReader, availableMemory, this);
			// obtain repeatable iterator for second input
			repeatableInput = new RepeatableMutableObjectIterator(outerReader);
			
			firstInputIsOuter = true;
		} else if(config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
			// obtain spilling resettable iterator for second input
			innerInput = new SpillingResettableMutableObjectIterator(memoryManager, 
					ioManager, innerReader, availableMemory, this);
			// obtain repeatable iterator for first input
			repeatableInput = new RepeatableMutableObjectIterator(outerReader);
			
			firstInputIsOuter = false;
		} else {
			throw new RuntimeException("Invalid local strategy for CrossTask: " + config.getLocalStrategy());
		}
		
		// open spilling resettable iterator
		innerInput.open();
		
		if (!this.running)
			return;

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Resetable iterator obtained"));

		
		PactRecord innerRecord = new PactRecord();
		PactRecord outerRecord = new PactRecord();
		boolean outerValid = repeatableInput.next(outerRecord);
		// read streamed iterator of outer side
		while (this.running && outerValid) {

			// read spilled iterator of inner side
			while (this.running && innerInput.next(innerRecord)) {
				// call cross() method of CrossStub depending on local strategy
				if(firstInputIsOuter) {
					stub.cross(outerRecord, innerRecord, output);
				} else {
					stub.cross(innerRecord, outerRecord, output);
				}
				
				repeatableInput.repeatLast(outerRecord);
			}
			// reset spilling resettable iterator of inner side
			if(this.running && (outerValid = repeatableInput.next(outerRecord))) {
				innerInput.reset();
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Utility class that turns a standard {@link java.util.Iterator} for key/value pairs into a
	 * {@link LastRepeatableIterator}. 
	 */
	private static final class RepeatableMutableObjectIterator implements LastRepeatableMutableObjectIterator<PactRecord>
	{
		private final MutableObjectIterator<PactRecord> input;
		
		public RepeatableMutableObjectIterator(MutableObjectIterator<PactRecord> input) {
			this.input = input;
		}

		private PactRecord lastRecord = new PactRecord();
		private boolean lastRecordValid = false;

		@Override
		public boolean next(PactRecord target) throws IOException {
			if ((lastRecordValid = input.next(lastRecord))) {
				lastRecord.copyTo(target);
				return true;
			}
			return false;
		}

		@Override
		public boolean repeatLast(PactRecord target) {
			if (lastRecordValid)
			{
				lastRecord.copyTo(target);
				return true;
			}
			return false;
		}
	}
}
