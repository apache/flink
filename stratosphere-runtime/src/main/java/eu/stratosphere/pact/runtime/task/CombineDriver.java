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

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;

import eu.stratosphere.pact.runtime.sort.AsynchronousPartialSorter;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Combine task which is executed by a Nephele task manager.
 * <p>
 * The task is inserted into a PACT program before a ReduceTask. The combine task has a single input and one output. It
 * is provided with a ReduceFunction that implemented the <code>combine()</code> method.
 * <p>
 * The CombineTask uses a combining iterator over all key-value pairs of its input. The output of the iterator is
 * emitted.
 * 
 * @see eu.stratosphere.pact.ReduceFunction.stub.ReduceStub
 * 
 * @param <T> The data type consumed and produced by the combiner.
 */
public class CombineDriver<T> implements PactDriver<GenericGroupReduce<T, ?>, T>
{
	private static final Log LOG = LogFactory.getLog(CoGroupDriver.class);

	
	private PactTaskContext<GenericGroupReduce<T, ?>, T> taskContext;
	
	private CloseableInputProvider<T> input;

	private TypeSerializer<T> serializer;

	private TypeComparator<T> comparator;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------


	@Override
	public void setup(PactTaskContext<GenericGroupReduce<T, ?>, T> context) {
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
	public Class<GenericGroupReduce<T, ?>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericGroupReduce<T, ?>> clazz = (Class<GenericGroupReduce<T, ?>>) (Class<?>) GenericGroupReduce.class;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		final TaskConfig config = this.taskContext.getTaskConfig();
		final DriverStrategy ls = config.getDriverStrategy();

		final long availableMemory = config.getMemoryDriver();

		final MemoryManager memoryManager = this.taskContext.getMemoryManager();

		final MutableObjectIterator<T> in = this.taskContext.getInput(0);
		this.serializer = this.taskContext.getInputSerializer(0);
		this.comparator = this.taskContext.getInputComparator(0);

		switch (ls) {
		case PARTIAL_GROUP:
			this.input = new AsynchronousPartialSorter<T>(memoryManager, in, this.taskContext.getOwningNepheleTask(),
						this.serializer, this.comparator.duplicate(), availableMemory);
			break;
		// obtain and return a grouped iterator from the combining sort-merger
		default:
			throw new RuntimeException("Invalid local strategy provided for CombineTask.");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled())
			LOG.debug(this.taskContext.formatLogString("Preprocessing done, iterator obtained."));

		final KeyGroupedIterator<T> iter = new KeyGroupedIterator<T>(this.input.getIterator(),
				this.serializer, this.comparator);

		// cache references on the stack
		final GenericGroupReduce<T, ?> stub = this.taskContext.getStub();
		final Collector<T> output = this.taskContext.getOutputCollector();

		// run stub implementation
		while (this.running && iter.nextKey()) {
			stub.combine(iter.getValues(), output);
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


	@Override
	public void cancel() {
		this.running = false;
	}
}