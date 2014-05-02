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

import eu.stratosphere.api.common.functions.GenericFlatMap;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Map task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapFunction
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that to the <code>map()</code> method
 * of the MapFunction.
 * 
 * @see GenericCollectorMap
 * 
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
public class FlatMapDriver<IT, OT> implements PactDriver<GenericFlatMap<IT, OT>, OT> {
	
	private PactTaskContext<GenericFlatMap<IT, OT>, OT> taskContext;
	
	private volatile boolean running;
	
	
	@Override
	public void setup(PactTaskContext<GenericFlatMap<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericFlatMap<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericFlatMap<IT, OT>> clazz = (Class<GenericFlatMap<IT, OT>>) (Class<?>) GenericFlatMap.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	@Override
	public void prepare() {
		// nothing, since a mapper does not need any preparation
	}

	@Override
	public void run() throws Exception {
		// cache references on the stack
		final MutableObjectIterator<IT> input = this.taskContext.getInput(0);
		final GenericFlatMap<IT, OT> function = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		IT record = this.taskContext.<IT>getInputSerializer(0).getSerializer().createInstance();

		while (this.running && ((record = input.next(record)) != null)) {
			function.flatMap(record, output);
		}
	}

	@Override
	public void cleanup() {
		// mappers need no cleanup, since no strategies are used.
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}