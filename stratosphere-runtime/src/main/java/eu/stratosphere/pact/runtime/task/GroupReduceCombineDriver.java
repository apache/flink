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

import eu.stratosphere.api.common.functions.GenericCombine;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.runtime.sort.AsynchronousPartialSorter;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Combine operator, standalone (not chained)
 * <p>
 * The CombineTask uses a combining iterator over its input. The output of the iterator is emitted.
 * 
 * @param <T> The data type consumed and produced by the combiner.
 */
public class GroupReduceCombineDriver<T> implements PactDriver<GenericCombine<T>, T> {
	
	private static final Log LOG = LogFactory.getLog(GroupReduceCombineDriver.class);

	
	private PactTaskContext<GenericCombine<T>, T> taskContext;
	
	private CloseableInputProvider<T> input;

	private TypeSerializerFactory<T> serializerFactory;

	private TypeComparator<T> comparator;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GenericCombine<T>, T> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericCombine<T>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericCombine<T>> clazz = (Class<GenericCombine<T>>) (Class<?>) GenericCombine.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}

	@Override
	public void prepare() throws Exception {
		final TaskConfig config = this.taskContext.getTaskConfig();
		final DriverStrategy ls = config.getDriverStrategy();

		final long availableMemory = config.getMemoryDriver();

		final MemoryManager memoryManager = this.taskContext.getMemoryManager();

		final MutableObjectIterator<T> in = this.taskContext.getInput(0);
		this.serializerFactory = this.taskContext.getInputSerializer(0);
		this.comparator = this.taskContext.getInputComparator(0);

		switch (ls) {
		case SORTED_GROUP_COMBINE:
			this.input = new AsynchronousPartialSorter<T>(memoryManager, in, this.taskContext.getOwningNepheleTask(),
						this.serializerFactory, this.comparator.duplicate(), availableMemory);
			break;
		// obtain and return a grouped iterator from the combining sort-merger
		default:
			throw new RuntimeException("Invalid local strategy provided for CombineTask.");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("Preprocessing done, iterator obtained."));
		}

		final KeyGroupedIterator<T> iter = new KeyGroupedIterator<T>(this.input.getIterator(),
				this.serializerFactory.getSerializer(), this.comparator);

		// cache references on the stack
		final GenericCombine<T> stub = this.taskContext.getStub();
		final Collector<T> output = this.taskContext.getOutputCollector();

		// run stub implementation
		while (this.running && iter.nextKey()) {
			stub.combine(iter.getValues(), output);
		}
	}

	@Override
	public void cleanup() throws Exception {
		if (this.input != null) {
			this.input.close();
			this.input = null;
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}
