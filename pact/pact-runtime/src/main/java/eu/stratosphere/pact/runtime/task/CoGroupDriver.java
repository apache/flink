/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import eu.stratosphere.pact.common.generic.GenericCoGrouper;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.sort.SortMergeCoGroupIterator;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * CoGroup task which is executed by a Nephele task manager. The task has two
 * inputs and one or multiple outputs. It is provided with a CoGroupStub
 * implementation.
 * <p>
 * The CoGroupTask group all pairs that share the same key from both inputs. Each for each key, the sets of values that
 * were pair with that key of both inputs are handed to the <code>coGroup()</code> method of the CoGroupStub.
 * 
 * @see eu.stratosphere.pact.common.stubs.CoGroupStub
 * @author Stephan Ewen
 */
public class CoGroupDriver<IT1, IT2, OT> implements PactDriver<GenericCoGrouper<IT1, IT2, OT>, OT>
{
	private static final Log LOG = LogFactory.getLog(CoGroupDriver.class);
	
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;	// minimal memory for the task to operate
	
	
	private PactTaskContext<GenericCoGrouper<IT1, IT2, OT>, OT> taskContext;
	
	private CoGroupTaskIterator<IT1, IT2> coGroupIterator;				// the iterator that does the actual cogroup
	
	private volatile boolean running;

	// ------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#setup(eu.stratosphere.pact.runtime.task.PactTaskContext)
	 */
	@Override
	public void setup(PactTaskContext<GenericCoGrouper<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
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
	public Class<GenericCoGrouper<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericCoGrouper<IT1, IT2, OT>> clazz = (Class<GenericCoGrouper<IT1, IT2, OT>>) (Class<?>) GenericCoGrouper.class;
		return clazz;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
	 */
	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}
	
	/* (non-Javadoc)
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
		
		final MutableObjectIterator<IT1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<IT2> in2 = this.taskContext.getInput(1);
		
		// get the key positions and types
		final TypeSerializer<IT1> serializer1 = this.taskContext.getInputSerializer(0);
		final TypeSerializer<IT2> serializer2 = this.taskContext.getInputSerializer(1);
		final TypeComparator<IT1> groupComparator1 = this.taskContext.getInputComparator(0);
		final TypeComparator<IT2> groupComparator2 = this.taskContext.getInputComparator(1);
		
		TypeComparator<IT1> sortComparator1 = this.taskContext.getSecondarySortComparator(0);
		TypeComparator<IT2> sortComparator2 = this.taskContext.getSecondarySortComparator(1);
		
		if (sortComparator1 == null) {
			sortComparator1 = groupComparator1.duplicate();
		}
		if (sortComparator2 == null) {
			sortComparator2 = groupComparator2.duplicate();
		}
		
		final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory;
		try {
			final Class<? extends TypePairComparatorFactory<IT1, IT2>> factoryClass =
				config.getPairComparatorFactory(this.taskContext.getUserCodeClassLoader());
			
			if (factoryClass == null) {
				@SuppressWarnings("unchecked")
				TypePairComparatorFactory<IT1, IT2> pactRecordFactory = 
									(TypePairComparatorFactory<IT1, IT2>) PactRecordPairComparatorFactory.get();
				pairComparatorFactory = pactRecordFactory;
			} else {
				@SuppressWarnings("unchecked")
				final Class<TypePairComparatorFactory<IT1, IT2>> clazz = (Class<TypePairComparatorFactory<IT1, IT2>>) (Class<?>) TypePairComparatorFactory.class;
				pairComparatorFactory = InstantiationUtil.instantiate(factoryClass, clazz);
			}
		} catch (ClassNotFoundException cnfex) {
			throw new Exception("The class registered as TypePairComparatorFactory cloud not be loaded.", cnfex);
		}
		
		// obtain task manager's memory manager
		final MemoryManager memoryManager = this.taskContext.getMemoryManager();
		// obtain task manager's I/O manager
		final IOManager ioManager = this.taskContext.getIOManager();

		// create CoGropuTaskIterator according to provided local strategy.
		switch (ls)
		{
		case SORT_BOTH_MERGE:
		case SORT_FIRST_MERGE:
		case SORT_SECOND_MERGE:
		case MERGE:
			this.coGroupIterator = new SortMergeCoGroupIterator<IT1, IT2>(memoryManager, ioManager, 
					in1, in2, serializer1, groupComparator1, sortComparator1, serializer2, groupComparator2, sortComparator2,
					pairComparatorFactory.createComparator12(groupComparator1, groupComparator2),
					availableMemory, maxFileHandles, spillThreshold, ls, this.taskContext.getOwningNepheleTask());
			break;
			default:
				throw new Exception("Unsupported local strategy for CoGropuTask: " + ls.name());
		}
		
		
		// open CoGroupTaskIterator - this triggers the sorting and blocks until the iterator is ready
		this.coGroupIterator.open();
		
		if (LOG.isDebugEnabled())
			LOG.debug(this.taskContext.formatLogString("CoGroup task iterator ready."));
	}
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		final GenericCoGrouper<IT1, IT2, OT> coGroupStub = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		final CoGroupTaskIterator<IT1, IT2> coGroupIterator = this.coGroupIterator;
		
		while (this.running && coGroupIterator.next()) {
			coGroupStub.coGroup(coGroupIterator.getValues1(), coGroupIterator.getValues2(), collector);
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

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#cancel()
	 */
	@Override
	public void cancel() throws Exception {
		this.running = false;
		cleanup();
	}
}
