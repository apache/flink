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

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.generic.GenericReducer;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
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
public class ReduceTask<IT, OT> extends AbstractPactTask<GenericReducer<IT, OT>, OT>
{

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;
	
	
	private CloseableInputProvider<IT> input;
	
	private TypeSerializer<IT> serializer;
	
	private TypeComparator<IT> comparator;
	

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
	public Class<GenericReducer<IT, OT>> getStubType()
	{
		@SuppressWarnings("unchecked")
		final Class<GenericReducer<IT, OT>> clazz = (Class<GenericReducer<IT, OT>>) (Class<?>) GenericReducer.class; 
		return clazz;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
	 */
	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}
	
	// --------------------------------------------------------------------------------------------

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
		if ((ls == LocalStrategy.SORT || ls == LocalStrategy.COMBININGSORT) && availableMemory < MIN_REQUIRED_MEMORY)
		{
			throw new Exception("The Reduce task was initialized with too little memory for local strategy " +
					this.config.getLocalStrategy() + " : " + availableMemory + " bytes." +
				    "Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}
		
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		final IOManager ioManager = getEnvironment().getIOManager();
		
		final MutableObjectIterator<IT> in = getInput(0);
		this.serializer = getInputSerializer(0);
		this.comparator = getInputComparator(0);
		
		TypeComparator<IT> secondarySortComparator = getSecondarySortComparator(0);

		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy())
		{
		case NONE:
			// local strategy is NONE
			// input is already grouped, an iterator that wraps the reader is created and returned
			this.input = new SimpleCloseableInputProvider<IT>(in);
			break;

			// local strategy is SORT
			// The input is grouped using a sort-merge strategy. An iterator on the sorted pairs is created and returned.
		case SORT:			
			// instantiate a sort-merger
			this.input = new UnilateralSortMerger<IT>(memoryManager, ioManager, in,
					this, this.serializer, secondarySortComparator, availableMemory, maxFileHandles, spillThreshold);
			break;
			
		case COMBININGSORT:
			// instantiate a combining sort-merger
			this.input = new CombiningUnilateralSortMerger<IT>(this.stub, memoryManager,
					ioManager, in, this, this.serializer, secondarySortComparator,
					availableMemory, maxFileHandles, spillThreshold, false);
			break;
		default:
			throw new Exception("Invalid local strategy provided for ReduceTask: " + ls.name());
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Reducer preprocessing done. Running Reducer code."));
		}

		final KeyGroupedIterator<IT> iter = new KeyGroupedIterator<IT>(
						this.input.getIterator(), this.serializer, this.comparator);
		
		// cache references on the stack
		final GenericReducer<IT, OT> stub = this.stub;
		final Collector<OT> output = this.output;
		
		// run stub implementation
		while (this.running && iter.nextKey())
		{
			stub.reduce(iter.getValues(), output);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception
	{
		if (this.input != null) {
			this.input.close();
			this.input = null;
		}
	}
}
