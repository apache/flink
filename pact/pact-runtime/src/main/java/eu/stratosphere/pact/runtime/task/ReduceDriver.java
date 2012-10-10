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

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

/**
 * Reduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceStub
 * implementation.
 * <p>
 * The ReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the ReduceStub.
 * 
 * @see ReduceStub
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class ReduceDriver<IT, OT> implements PactDriver<GenericReducer<IT, OT>, OT>
{
	private static final Log LOG = LogFactory.getLog(ReduceDriver.class);
	
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;		// minimal memory for the task to operate

	private PactTaskContext<GenericReducer<IT, OT>, OT> taskContext;
	
	private CloseableInputProvider<IT> input;

	private TypeSerializer<IT> serializer;

	private TypeComparator<IT> comparator;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#setup(eu.stratosphere.pact.runtime.task.PactTaskContext)
	 */
	@Override
	public void setup(PactTaskContext<GenericReducer<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<GenericReducer<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericReducer<IT, OT>> clazz = (Class<GenericReducer<IT, OT>>) (Class<?>) GenericReducer.class;
		return clazz;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
	 */
	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}

	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		final TaskConfig config = this.taskContext.getTaskConfig();
		
		// set up memory and I/O parameters
		final long availableMemory = config.getMemorySize();
		final int maxFileHandles = config.getNumFilehandles();
		final float spillThreshold = config.getSortSpillingTreshold();

		// test minimum memory requirements
		final LocalStrategy ls = config.getLocalStrategy();
		if ((ls == LocalStrategy.SORT || ls == LocalStrategy.COMBININGSORT) && availableMemory < MIN_REQUIRED_MEMORY) {
			throw new Exception("The Reduce task was initialized with too little memory for local strategy " +
					config.getLocalStrategy() + " : " + availableMemory + " bytes." +
					"Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}

		final MemoryManager memoryManager = this.taskContext.getMemoryManager();
		final IOManager ioManager = this.taskContext.getIOManager();

		final MutableObjectIterator<IT> in = this.taskContext.getInput(0);
		this.serializer = this.taskContext.getInputSerializer(0);
		this.comparator = this.taskContext.getInputComparator(0);
		
		TypeComparator<IT> sortComparator = this.taskContext.getSecondarySortComparator(0);
		if (sortComparator == null) {
			sortComparator = this.comparator.duplicate();
		}

		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy()) {
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
						this.taskContext.getOwningNepheleTask(), this.serializer, sortComparator,
						availableMemory, maxFileHandles,
				spillThreshold);
			break;

		case COMBININGSORT:
			// instantiate a combining sort-merger
			this.input = new CombiningUnilateralSortMerger<IT>(this.taskContext.getStub(), memoryManager,
						ioManager, in, this.taskContext.getOwningNepheleTask(), this.serializer,
						sortComparator, availableMemory, maxFileHandles, spillThreshold, false);
			break;
		default:
			throw new Exception("Invalid local strategy provided for ReduceTask: " + ls.name());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("Reducer preprocessing done. Running Reducer code."));
		}

		final KeyGroupedIterator<IT> iter = new KeyGroupedIterator<IT>(
				this.input.getIterator(), this.serializer, this.comparator);

		// cache references on the stack
		final GenericReducer<IT, OT> stub = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		// run stub implementation
		while (this.running && iter.nextKey()) {
			stub.reduce(iter.getValues(), output);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		if (this.input != null) {
			this.input.close();
			this.input = null;
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#cancel()
	 */
	@Override
	public void cancel() {
		this.running = false;
	}
}