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

import eu.stratosphere.api.common.functions.GenericMapPartition;
import eu.stratosphere.pact.runtime.util.MutableToRegularIteratorWrapper;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;


/**
 * MapPartition task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapFunction
 * implementation.
 * <p>
 * The MapPartitionTask creates an iterator over all key-value pairs of its input and hands that to the <code>map_partition()</code> method
 * of the MapFunction.
 *
 * @see GenericMapPartition
 *
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
public class MapPartitionDriver<IT, OT> implements PactDriver<GenericMapPartition<IT, OT>, OT> {

	private PactTaskContext<GenericMapPartition<IT, OT>, OT> taskContext;

	private volatile boolean running;


	@Override
	public void setup(PactTaskContext<GenericMapPartition<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
		public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericMapPartition<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericMapPartition<IT, OT>> clazz = (Class<GenericMapPartition<IT, OT>>) (Class<?>) GenericMapPartition.class;
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
		final GenericMapPartition<IT, OT> function = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		final MutableToRegularIteratorWrapper<IT> inIter = new MutableToRegularIteratorWrapper<IT>(input, this.taskContext.<IT>getInputSerializer(0).getSerializer() );
		IT record = this.taskContext.<IT>getInputSerializer(0).getSerializer().createInstance();

		function.mapPartition(inIter, output);
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
