/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.runtime.util.MutableToRegularIteratorWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * MapPartition task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapFunction
 * implementation.
 * <p>
 * The MapPartitionTask creates an iterator over all key-value pairs of its input and hands that to the <code>map_partition()</code> method
 * of the MapFunction.
 *
 * @see MapPartitionFunction
 *
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
public class MapPartitionDriver<IT, OT> implements PactDriver<MapPartitionFunction<IT, OT>, OT> {

	private PactTaskContext<MapPartitionFunction<IT, OT>, OT> taskContext;

	@Override
	public void setup(PactTaskContext<MapPartitionFunction<IT, OT>, OT> context) {
		this.taskContext = context;
	}

	@Override
		public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<MapPartitionFunction<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<MapPartitionFunction<IT, OT>> clazz = (Class<MapPartitionFunction<IT, OT>>) (Class<?>) MapPartitionFunction.class;
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
		final MapPartitionFunction<IT, OT> function = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		final MutableToRegularIteratorWrapper<IT> inIter = new MutableToRegularIteratorWrapper<IT>(input, this.taskContext.<IT>getInputSerializer(0).getSerializer());
		function.mapPartition(inIter, output);
	}

	@Override
	public void cleanup() {
		// mappers need no cleanup, since no strategies are used.
	}

	@Override
	public void cancel() {}
}
