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

import java.util.Comparator;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

/**
 * Reduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceStub
 * implementation.
 * <p>
 * The ReduceTask creates a iterator over all records from its input. The iterator returns all records grouped
 * by their key. The iterator is handed to the <code>reduce()</code> method of the ReduceStub.
 * 
 * @see ReduceStub
 * 
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class ReduceTask extends AbstractPactTask<ReduceStub>
{

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;
	
	
	private CloseableInputProvider<PactRecord> input;
	
	private int[] keyPositions;
	
	private Class<? extends Key>[] keyClasses;

	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Preprocessing done, iterator obtained."));

		final KeyGroupedIterator iter = new KeyGroupedIterator(this.input.getIterator(), this.keyPositions, this.keyClasses);
		
		// cache references on the stack
		final ReduceStub stub = this.stub;
		final Collector output = this.output;
		
		// DW: Start of temporary code
		final OutputCollector oc = (OutputCollector) output;
		final Environment env = getEnvironment();
		// DW: End of temporary code
		
		// run stub implementation
		while (this.running && iter.nextKey())
		{
			stub.reduce(iter.getValues(), output);
			// DW: Start of temporary code
			env.reportPACTDataStatistics(iter.getConsumedPactRecordsInBytes(), 
				oc.getCollectedPactRecordsInBytes());
			// DW: End of temporary code
		}
	}

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
	public Class<ReduceStub> getStubType() {
		return ReduceStub.class;
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
		LocalStrategy ls = this.config.getLocalStrategy();
		if ((ls == LocalStrategy.SORT || ls == LocalStrategy.COMBININGSORT) && availableMemory < MIN_REQUIRED_MEMORY)
		{
			throw new Exception("The Reduce task was initialized with too little memory for local strategy " +
					this.config.getLocalStrategy() + " : " + availableMemory + " bytes." +
				    "Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}
		
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
		@SuppressWarnings("unchecked")
		final Comparator<Key>[] comparators = new Comparator[keyPositions.length];
		final KeyComparator kk = new KeyComparator();
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = kk;
		}

		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy())
		{
		case NONE:
			// local strategy is NONE
			// input is already grouped, an iterator that wraps the reader is created and returned
			this.input = new SimpleCloseableInputProvider<PactRecord>(this.inputs[0]);
			break;

			// local strategy is SORT
			// The input is grouped using a sort-merge strategy. An iterator on the sorted pairs is created and returned.
		case SORT:			
			// instantiate a sort-merger
			this.input = new UnilateralSortMerger(memoryManager, ioManager, availableMemory, maxFileHandles, comparators, 
				keyPositions, keyClasses, this.inputs[0], this, spillThreshold);
			break;
			
		case COMBININGSORT:
			// instantiate a combining sort-merger
			this.input = new CombiningUnilateralSortMerger(this.stub, memoryManager,
					ioManager, availableMemory, maxFileHandles, comparators,
					keyPositions, keyClasses, this.inputs[0], this, spillThreshold, false);
			break;
		default:
			throw new Exception("Invalid local strategy provided for ReduceTask: " + ls.name());
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
}
