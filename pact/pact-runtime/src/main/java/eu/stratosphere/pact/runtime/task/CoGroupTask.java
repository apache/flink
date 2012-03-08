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

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.sort.SortMergeCoGroupIterator;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * CoGroup task which is executed by a Nephele task manager. The task has two
 * inputs and one or multiple outputs. It is provided with a CoGroupStub
 * implementation.
 * <p>
 * The CoGroupTask group all pairs that share the same key from both inputs. Each for each key, the sets of values that
 * were pair with that key of both inputs are handed to the <code>coGroup()</code> method of the CoGroupStub.
 * 
 * @see eu.stratosphere.pact.common.stub.CoGroupStub
 * @author Fabian Hueske
 * @author Matthias Ringwald
 */
public class CoGroupTask extends AbstractPactTask<CoGroupStub>
{
	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;
	
	// the iterator that does the actual cogroup
	private CoGroupTaskIterator coGroupIterator;

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
	public Class<CoGroupStub> getStubType() {
		return CoGroupStub.class;
	}
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		// set up memory and I/O parameters
		final long availableMemory = this.config.getMemorySize();
		final int maxFileHandles = this.config.getNumFilehandles();
		final float spillThreshold = this.config.getSortSpillingTreshold();
		
		// test minimum memory requirements
		final LocalStrategy ls = this.config.getLocalStrategy();
		long strategyMinMem = 0;
		
		switch (ls) {
			case SORT_BOTH_MERGE:
				strategyMinMem = MIN_REQUIRED_MEMORY*2;
				break;
			case SORT_FIRST_MERGE: 
			case SORT_SECOND_MERGE: 
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case MERGE: 
				strategyMinMem = 0;
				break;
		}
		
		if (availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The CoGroup task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}
		
		// get the key positions and types
		final int[] keyPositions1 = this.config.getLocalStrategyKeyPositions(0);
		final int[] keyPositions2 = this.config.getLocalStrategyKeyPositions(1);
		final Class<? extends Key>[] keyClasses = this.config.getLocalStrategyKeyClasses(this.userCodeClassLoader);
		
		if (keyPositions1 == null || keyPositions2 == null || keyClasses == null) {
			throw new Exception("The key positions and types are not specified for the CoGroupTask.");
		}
		if (keyPositions1.length != keyPositions2.length || keyPositions2.length != keyClasses.length) {
			throw new Exception("The number of key positions and types does not match in the configuration");
		}
		
		// obtain task manager's memory manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain task manager's I/O manager
		final IOManager ioManager = getEnvironment().getIOManager();

		// create CoGropuTaskIterator according to provided local strategy.
		switch (ls)
		{
		case SORT_BOTH_MERGE:
		case SORT_FIRST_MERGE:
		case SORT_SECOND_MERGE:
		case MERGE:
			this.coGroupIterator = new SortMergeCoGroupIterator(memoryManager, ioManager, 
					this.inputs[0], this.inputs[1], keyPositions1, keyPositions2, keyClasses, 
					availableMemory, maxFileHandles, spillThreshold, ls, this);
			break;
			default:
				throw new Exception("Unsupported local strategy for CoGropuTask: " + ls.name());
		}
		
		
		// open CoGroupTaskIterator - this triggers the sorting and blocks until the iterator is ready
		this.coGroupIterator.open();
		
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("CoGroup task iterator ready."));
	}
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		final CoGroupStub coGroupStub = this.stub;
		final Collector collector = this.output;
		final CoGroupTaskIterator coGroupIterator = this.coGroupIterator;
		if(this.stub.getClass().isAnnotationPresent(ForceCheckpoint.class)){
			getEnvironment().isForced(this.stub.getClass().getAnnotation(ForceCheckpoint.class).checkpoint());
		}
		while (this.running && coGroupIterator.next()) {
			coGroupStub.coGroup(coGroupIterator.getValues1(), coGroupIterator.getValues2(), 
					collector);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception
	{
		if (this.coGroupIterator != null) {
			this.coGroupIterator.close();
			this.coGroupIterator = null;
		}
	}
}
