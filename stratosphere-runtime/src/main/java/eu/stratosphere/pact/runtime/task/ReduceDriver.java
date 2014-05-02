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
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Reduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceFunction
 * implementation.
 * <p>
 * The ReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the ReduceFunction.
 * 
 * @see GenericGroupReduce
 */
public class ReduceDriver<IT, OT> implements PactDriver<GenericGroupReduce<IT, OT>, OT> {
	
	private static final Log LOG = LogFactory.getLog(ReduceDriver.class);

	private PactTaskContext<GenericGroupReduce<IT, OT>, OT> taskContext;
	
	private MutableObjectIterator<IT> input;

	private TypeSerializer<IT> serializer;

	private TypeComparator<IT> comparator;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GenericGroupReduce<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericGroupReduce<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericGroupReduce<IT, OT>> clazz = (Class<GenericGroupReduce<IT, OT>>) (Class<?>) GenericGroupReduce.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.SORTED_GROUP) {
			throw new Exception("Unrecognized driver strategy for Reduce driver: " + config.getDriverStrategy().name());
		}
		this.serializer = this.taskContext.<IT>getInputSerializer(0).getSerializer();
		this.comparator = this.taskContext.getInputComparator(0);
		this.input = this.taskContext.getInput(0);
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("Reducer preprocessing done. Running Reducer code."));
		}

		final KeyGroupedIterator<IT> iter = new KeyGroupedIterator<IT>(this.input, this.serializer, this.comparator);

		// cache references on the stack
		final GenericGroupReduce<IT, OT> stub = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		// run stub implementation
		while (this.running && iter.nextKey()) {
			stub.reduce(iter.getValues(), output);
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}