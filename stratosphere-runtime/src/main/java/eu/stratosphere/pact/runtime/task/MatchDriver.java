/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.sort.MergeMatchIterator;
import eu.stratosphere.pact.runtime.task.util.JoinTaskIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Match task which is executed by a Nephele task manager. The task has two inputs and one or multiple outputs.
 * It is provided with a JoinFunction implementation.
 * <p>
 * The MatchTask matches all pairs of records that share the same key and come from different inputs. Each pair of 
 * matching records is handed to the <code>match()</code> method of the JoinFunction.
 * 
 * @see GenericJoiner
 */
public class MatchDriver<IT1, IT2, OT> implements PactDriver<GenericJoiner<IT1, IT2, OT>, OT> {
	
	protected static final Log LOG = LogFactory.getLog(MatchDriver.class);
	
	protected PactTaskContext<GenericJoiner<IT1, IT2, OT>, OT> taskContext;
	
	private volatile JoinTaskIterator<IT1, IT2, OT> matchIterator;		// the iterator that does the actual matching
	
	protected volatile boolean running;
	
	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GenericJoiner<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	@Override
	public Class<GenericJoiner<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericJoiner<IT1, IT2, OT>> clazz = (Class<GenericJoiner<IT1, IT2, OT>>) (Class<?>) GenericJoiner.class;
		return clazz;
	}
	
	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}

	@Override
	public void prepare() throws Exception{
		final TaskConfig config = this.taskContext.getTaskConfig();
		
		// obtain task manager's memory manager and I/O manager
		final MemoryManager memoryManager = this.taskContext.getMemoryManager();
		final IOManager ioManager = this.taskContext.getIOManager();
		
		// set up memory and I/O parameters
		final long availableMemory = config.getMemoryDriver();
		final int numPages = memoryManager.computeNumberOfPages(availableMemory);
		
		// test minimum memory requirements
		final DriverStrategy ls = config.getDriverStrategy();
		
		final MutableObjectIterator<IT1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<IT2> in2 = this.taskContext.getInput(1);
		
		// get the key positions and types
		final TypeSerializer<IT1> serializer1 = this.taskContext.<IT1>getInputSerializer(0).getSerializer();
		final TypeSerializer<IT2> serializer2 = this.taskContext.<IT2>getInputSerializer(1).getSerializer();
		final TypeComparator<IT1> comparator1 = this.taskContext.getInputComparator(0);
		final TypeComparator<IT2> comparator2 = this.taskContext.getInputComparator(1);
		
		final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = config.getPairComparatorFactory(
				this.taskContext.getUserCodeClassLoader());
		if (pairComparatorFactory == null) {
			throw new Exception("Missing pair comparator factory for Match driver");
		}

		// create and return MatchTaskIterator according to provided local strategy.
		switch (ls) {
			case MERGE:
				this.matchIterator = new MergeMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1,
						serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2),
						memoryManager, ioManager, numPages, this.taskContext.getOwningNepheleTask());
				break;
			case HYBRIDHASH_BUILD_FIRST:
				this.matchIterator = new BuildFirstHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1,
					serializer2, comparator2, pairComparatorFactory.createComparator21(comparator1, comparator2),
					memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), availableMemory);
				break;
			case HYBRIDHASH_BUILD_SECOND:
				this.matchIterator = new BuildSecondHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1,
						serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2),
						memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), availableMemory);
				break;
			default:
				throw new Exception("Unsupported driver strategy for Match driver: " + ls.name());
		}
		
		// open MatchTaskIterator - this triggers the sorting or hash-table building
		// and blocks until the iterator is ready
		this.matchIterator.open();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("Match task iterator ready."));
		}
	}

	@Override
	public void run() throws Exception {
		final GenericJoiner<IT1, IT2, OT> matchStub = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		final JoinTaskIterator<IT1, IT2, OT> matchIterator = this.matchIterator;
		
		while (this.running && matchIterator.callWithNextKey(matchStub, collector));
	}

	@Override
	public void cleanup() throws Exception {
		if (this.matchIterator != null) {
			this.matchIterator.close();
			this.matchIterator = null;
		}
	}
	
	@Override
	public void cancel() {
		this.running = false;
		if (this.matchIterator != null) {
			this.matchIterator.abort();
		}
	}
}
