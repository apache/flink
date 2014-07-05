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

import eu.stratosphere.api.common.functions.GenericReduce;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Reduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceFunction
 * implementation.
 * <p>
 * The ReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the ReduceFunction.
 * 
 * @see GenericReduce
 */
public class AllReduceDriver<T> implements PactDriver<GenericReduce<T>, T> {
	
	private static final Log LOG = LogFactory.getLog(AllReduceDriver.class);

	private PactTaskContext<GenericReduce<T>, T> taskContext;
	
	private MutableObjectIterator<T> input;

	private TypeSerializer<T> serializer;
	
	private boolean running;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GenericReduce<T>, T> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericReduce<T>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericReduce<T>> clazz = (Class<GenericReduce<T>>) (Class<?>) GenericReduce.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		final TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.ALL_REDUCE) {
			throw new Exception("Unrecognized driver strategy for AllReduce driver: " + config.getDriverStrategy().name());
		}
		
		TypeSerializerFactory<T> serializerFactory = this.taskContext.getInputSerializer(0);
		this.serializer = serializerFactory.getSerializer();
		this.input = this.taskContext.getInput(0);
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("AllReduce preprocessing done. Running Reducer code."));
		}

		final GenericReduce<T> stub = this.taskContext.getStub();
		final T initialValue = (stub == null) ? null : stub.getInitialValue();
		final MutableObjectIterator<T> input = this.input;
		final TypeSerializer<T> serializer = this.serializer;

		T val1 = serializer.createInstance();

		if ((val1 = input.next(val1)) == null) {
			if (initialValue == null) {
				return;
			}

			val1 = initialValue;
		} else if (running && initialValue != null) {
			val1 = stub.reduce(initialValue, val1);
		}

		T val2;
		while (running && (val2 = input.next(serializer.createInstance())) != null) {
			val1 = stub.reduce(val1, val2);
		}

		this.taskContext.getOutputCollector().collect(val1);
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}