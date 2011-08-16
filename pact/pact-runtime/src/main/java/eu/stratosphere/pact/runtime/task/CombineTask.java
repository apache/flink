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

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.sort.AsynchronousPartialSorter;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

/**
 * Combine task which is executed by a Nephele task manager.
 * <p>
 * The task is inserted into a PACT program before a ReduceTask. The combine task has a single input and one output. It
 * is provided with a ReduceStub that implemented the <code>combine()</code> method.
 * <p>
 * The CombineTask uses a combining iterator over all key-value pairs of its input. The output of the iterator is
 * emitted.
 * 
 * @see eu.stratosphere.pact.common.stub.ReduceStub
 * @author Fabian Hueske
 * @author Matthias Ringwald
 */
public class CombineTask extends AbstractPactTask<ReduceStub> {

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 1 * 1024 * 1024;
	
	private CloseableInputProvider<PactRecord> input;
	
	private int[] keyPositions;
	
	private Class<? extends Key>[] keyClasses;

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
		
		// test minimum memory requirements
		LocalStrategy ls = config.getLocalStrategy();
		
		long strategyMinMem = 0;
		
		switch (ls) {
			case COMBININGSORT:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
		}
	
		if (availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The Combine task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}
		
		// obtain the TaskManager's MemoryManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the TaskManager's IOManager
		final IOManager ioManager = getEnvironment().getIOManager();

		// get the key positions and types
		this.keyPositions = this.config.getLocalStrategyKeyPositions(0);
		this.keyClasses = this.config.getLocalStrategyKeyClasses(this.userCodeClassLoader);
		if (this.keyPositions == null || this.keyClasses == null) {
			throw new Exception("The key positions and types are not specified for the CombineTask.");
		}
		
		// create the comparators
		@SuppressWarnings("unchecked")
		final Comparator<Key>[] comparators = new Comparator[keyPositions.length];
		final KeyComparator kk = new KeyComparator();
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = kk;
		}

		switch (ls) {

			// local strategy is COMBININGSORT
			// The Input is combined using a sort-merge strategy. Before spilling on disk, the data volume is reduced using
			// the combine() method of the ReduceStub.
			// An iterator on the sorted, grouped, and combined pairs is created and returned
			case COMBININGSORT:
				input = new AsynchronousPartialSorter(memoryManager,
						ioManager, availableMemory, comparators, keyPositions, keyClasses, inputs[0], this);
				break;
					// obtain and return a grouped iterator from the combining sort-merger
			default:
				throw new RuntimeException("Invalid local strategy provided for CombineTask.");
		}
	}


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
		final OutputCollector output = this.output;
		
		// run stub implementation
		while (this.running && iter.nextKey())
		{
			stub.combine(iter.getValues(), output);
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
