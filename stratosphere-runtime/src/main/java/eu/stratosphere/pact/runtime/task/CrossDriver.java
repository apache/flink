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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.functions.GenericCrosser;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.runtime.resettable.BlockResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Cross task which is executed by a Nephele task manager. The task has two
 * inputs and one or multiple outputs. It is provided with a CrossFunction
 * implementation.
 * <p>
 * The CrossTask builds the Cartesian product of the pairs of its two inputs. Each element (pair of pairs) is handed to
 * the <code>cross()</code> method of the CrossFunction.
 * 
 * @see eu.stratosphere.api.record.functions.CrossFunction
 * 
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class CrossDriver<T1, T2, OT> implements PactDriver<GenericCrosser<T1, T2, OT>, OT>
{
	private static final Log LOG = LogFactory.getLog(CrossDriver.class);
	
	
	private PactTaskContext<GenericCrosser<T1, T2, OT>, OT> taskContext;
	
	private MemoryManager memManager;
	
	private SpillingResettableMutableObjectIterator<?> spillIter;
	
	private BlockResettableMutableObjectIterator<?> blockIter;
	
	private long memForBlockSide;
	
	private long memForSpillingSide;

	private boolean blocked;
	
	private boolean firstIsOuter;
	
	private volatile boolean running;
	
	// ------------------------------------------------------------------------


	@Override
	public void setup(PactTaskContext<GenericCrosser<T1, T2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}


	@Override
	public int getNumberOfInputs() {
		return 2;
	}


	@Override
	public Class<GenericCrosser<T1, T2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericCrosser<T1, T2, OT>> clazz = (Class<GenericCrosser<T1, T2, OT>>) (Class<?>) GenericCrosser.class;
		return clazz;
	}
	

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}


	@Override
	public void prepare() throws Exception
	{
		final TaskConfig config = this.taskContext.getTaskConfig();
		final DriverStrategy ls = config.getDriverStrategy();
		
		switch (ls)
		{
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			this.blocked = true;
			this.firstIsOuter = true;
			break;
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			this.blocked = true;
			this.firstIsOuter = false;
			break;
		case NESTEDLOOP_STREAMED_OUTER_FIRST:
			this.blocked = false;
			this.firstIsOuter = true;
			break;
		case NESTEDLOOP_STREAMED_OUTER_SECOND:
			this.blocked = false;
			this.firstIsOuter = false;
			break;
		default:
			throw new RuntimeException("Invalid local strategy for CROSS: " + ls);
		}
		
		this.memManager = this.taskContext.getMemoryManager();
		final long totalAvailableMemory = config.getMemoryDriver();
		final int numPages = this.memManager.computeNumberOfPages(totalAvailableMemory);
		
		if (numPages < 2) {
			throw new RuntimeException(	"The Cross task was initialized with too little memory. " +
					"Cross requires at least 2 memory pages.");
		}
		
		// divide memory between spilling and blocking side
		if (ls == DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST || ls == DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
			this.memForSpillingSide = totalAvailableMemory;
			this.memForBlockSide = 0;
		} else {
			if (numPages > 32) {
				this.memForSpillingSide = 2 * this.memManager.getPageSize();
			} else {
				this.memForSpillingSide =  this.memManager.getPageSize();
			}
			this.memForBlockSide = totalAvailableMemory - this.memForSpillingSide;
		}
	}


	@Override
	public void run() throws Exception
	{
		if (this.blocked) {
			if (this.firstIsOuter) {
				runBlockedOuterFirst();
			} else {
				runBlockedOuterSecond();
			}
		} else {
			if (this.firstIsOuter) {
				runStreamedOuterFirst();
			} else {
				runStreamedOuterSecond();
			}
		}
	}


	@Override
	public void cleanup() throws Exception
	{
		if (this.spillIter != null) {
			this.spillIter.close();
			this.spillIter = null;
		}
		if (this.blockIter != null) {
			this.blockIter.close();
			this.blockIter = null;
		}
	}
	

	@Override
	public void cancel() {
		this.running = false;
	}

	private void runBlockedOuterFirst() throws Exception
	{
		if (LOG.isDebugEnabled())  {
			LOG.debug(this.taskContext.formatLogString("Running Cross with Block-Nested-Loops: " +
					"First input is outer (blocking) side, second input is inner (spilling) side."));
		}
			
		final MutableObjectIterator<T1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<T2> in2 = this.taskContext.getInput(1);
		
		final TypeSerializer<T1> serializer1 = this.taskContext.getInputSerializer(0);
		final TypeSerializer<T2> serializer2 = this.taskContext.getInputSerializer(1);
		
		final BlockResettableMutableObjectIterator<T1> blockVals = 
				new BlockResettableMutableObjectIterator<T1>(this.memManager, in1, serializer1, this.memForBlockSide,
							this.taskContext.getOwningNepheleTask());
		this.blockIter = blockVals;
		
		final SpillingResettableMutableObjectIterator<T2> spillVals = new SpillingResettableMutableObjectIterator<T2>(
				in2, serializer2, this.memManager, this.taskContext.getIOManager(), this.memForSpillingSide,
				this.taskContext.getOwningNepheleTask());
		this.spillIter = spillVals;
		
		final T1 val1 = serializer1.createInstance();
		final T2 val2 = serializer2.createInstance();
		final T2 val2Copy = serializer2.createInstance();
		
		final GenericCrosser<T1, T2, OT> crosser = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		
		// for all blocks
		do {
			// for all values from the spilling side
			while (this.running && spillVals.next(val2)) {
				// for all values in the block
				while (blockVals.next(val1)) {
					serializer2.copyTo(val2, val2Copy);
					crosser.cross(val1, val2Copy, collector);
				}
				blockVals.reset();
			}
			spillVals.reset();
		}
		while (this.running && blockVals.nextBlock());
	}
	
	private void runBlockedOuterSecond() throws Exception
	{
		if (LOG.isDebugEnabled())  {
			LOG.debug(this.taskContext.formatLogString("Running Cross with Block-Nested-Loops: " +
					"First input is inner (spilling) side, second input is outer (blocking) side."));
		}
		
		final MutableObjectIterator<T1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<T2> in2 = this.taskContext.getInput(1);
		
		final TypeSerializer<T1> serializer1 = this.taskContext.getInputSerializer(0);
		final TypeSerializer<T2> serializer2 = this.taskContext.getInputSerializer(1);
		
		final SpillingResettableMutableObjectIterator<T1> spillVals = new SpillingResettableMutableObjectIterator<T1>(
				in1, serializer1, this.memManager, this.taskContext.getIOManager(), this.memForSpillingSide,
				this.taskContext.getOwningNepheleTask());
		this.spillIter = spillVals;
		
		final BlockResettableMutableObjectIterator<T2> blockVals = 
				new BlockResettableMutableObjectIterator<T2>(this.memManager, in2, serializer2, this.memForBlockSide,
						this.taskContext.getOwningNepheleTask());
		this.blockIter = blockVals;
		
		final T1 val1 = serializer1.createInstance();
		final T1 val1Copy = serializer1.createInstance();
		final T2 val2 = serializer2.createInstance();
		
		final GenericCrosser<T1, T2, OT> crosser = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		
		// for all blocks
		do {
			// for all values from the spilling side
			while (this.running && spillVals.next(val1)) {
				// for all values in the block
				while (this.running && blockVals.next(val2)) {
					serializer1.copyTo(val1, val1Copy);
					crosser.cross(val1Copy, val2, collector);
				}
				blockVals.reset();
			}
			spillVals.reset();
		}
		while (this.running && blockVals.nextBlock());
	}
	
	private void runStreamedOuterFirst() throws Exception
	{
		if (LOG.isDebugEnabled())  {
			LOG.debug(this.taskContext.formatLogString("Running Cross with Nested-Loops: " +
					"First input is outer side, second input is inner (spilling) side."));
		}
		
		final MutableObjectIterator<T1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<T2> in2 = this.taskContext.getInput(1);
		
		final TypeSerializer<T1> serializer1 = this.taskContext.getInputSerializer(0);
		final TypeSerializer<T2> serializer2 = this.taskContext.getInputSerializer(1);
		
		final SpillingResettableMutableObjectIterator<T2> spillVals = new SpillingResettableMutableObjectIterator<T2>(
				in2, serializer2, this.memManager, this.taskContext.getIOManager(), this.memForSpillingSide,
				this.taskContext.getOwningNepheleTask());
		this.spillIter = spillVals;
		
		final T1 val1 = serializer1.createInstance();
		final T1 val1Copy = serializer1.createInstance();
		final T2 val2 = serializer2.createInstance();
		
		final GenericCrosser<T1, T2, OT> crosser = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		
		// for all blocks
		while (this.running && in1.next(val1)) {
			// for all values from the spilling side
			while (this.running && spillVals.next(val2)) {
				serializer1.copyTo(val1, val1Copy);
				crosser.cross(val1Copy, val2, collector);
			}
			spillVals.reset();
		}
	}
	
	private void runStreamedOuterSecond() throws Exception
	{
		if (LOG.isDebugEnabled())  {
			LOG.debug(this.taskContext.formatLogString("Running Cross with Nested-Loops: " +
					"First input is inner (spilling) side, second input is outer side."));
		}
		final MutableObjectIterator<T1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<T2> in2 = this.taskContext.getInput(1);
		
		final TypeSerializer<T1> serializer1 = this.taskContext.getInputSerializer(0);
		final TypeSerializer<T2> serializer2 = this.taskContext.getInputSerializer(1);
		
		final SpillingResettableMutableObjectIterator<T1> spillVals = new SpillingResettableMutableObjectIterator<T1>(
				in1, serializer1, this.memManager, this.taskContext.getIOManager(), this.memForSpillingSide,
				this.taskContext.getOwningNepheleTask());
		this.spillIter = spillVals;
		
		final T1 val1 = serializer1.createInstance();
		final T2 val2 = serializer2.createInstance();
		final T2 val2Copy = serializer2.createInstance();
		
		final GenericCrosser<T1, T2, OT> crosser = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		
		// for all blocks
		while (this.running && in2.next(val2)) {
			// for all values from the spilling side
			while (this.running && spillVals.next(val1)) {
				serializer2.copyTo(val2, val2Copy);
				crosser.cross(val1, val2Copy, collector);
			}
			spillVals.reset();
		}
	}
}
