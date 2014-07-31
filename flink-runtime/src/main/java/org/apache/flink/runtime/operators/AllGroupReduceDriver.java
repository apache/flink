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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.MutableToRegularIteratorWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * GroupReduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a GroupReduceFunction
 * implementation.
 * <p>
 * The GroupReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the GroupReduceFunction.
 * 
 * @see org.apache.flink.api.common.functions.GroupReduceFunction
 */
public class AllGroupReduceDriver<IT, OT> implements PactDriver<GroupReduceFunction<IT, OT>, OT> {
	
	private static final Log LOG = LogFactory.getLog(AllGroupReduceDriver.class);

	private PactTaskContext<GroupReduceFunction<IT, OT>, OT> taskContext;
	
	private MutableObjectIterator<IT> input;

	private TypeSerializer<IT> serializer;
	
	private DriverStrategy strategy;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GroupReduceFunction<IT, OT>, OT> context) {
		this.taskContext = context;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GroupReduceFunction<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GroupReduceFunction<IT, OT>> clazz = (Class<GroupReduceFunction<IT, OT>>) (Class<?>) GroupReduceFunction.class;
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
		this.strategy = config.getDriverStrategy();
		
		if (strategy == DriverStrategy.ALL_GROUP_COMBINE) {
			if (!(this.taskContext.getStub() instanceof FlatCombineFunction)) {
				throw new Exception("Using combiner on a UDF that does not implement the combiner interface " + FlatCombineFunction.class.getName());
			}
		}
		else if (strategy != DriverStrategy.ALL_GROUP_REDUCE) {
			throw new Exception("Unrecognized driver strategy for AllGroupReduce driver: " + config.getDriverStrategy().name());
		}
		this.serializer = this.taskContext.<IT>getInputSerializer(0).getSerializer();
		this.input = this.taskContext.getInput(0);
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("AllGroupReduce preprocessing done. Running Reducer code."));
		}

		final MutableToRegularIteratorWrapper<IT> inIter = new MutableToRegularIteratorWrapper<IT>(this.input, this.serializer);

		// single UDF call with the single group
		if (inIter.hasNext()) {
			if (strategy == DriverStrategy.ALL_GROUP_REDUCE) {
				final GroupReduceFunction<IT, OT> reducer = this.taskContext.getStub();
				final Collector<OT> output = this.taskContext.getOutputCollector();
				reducer.reduce(inIter, output);
			}
			else {
				@SuppressWarnings("unchecked")
				final FlatCombineFunction<IT> combiner = (FlatCombineFunction<IT>) this.taskContext.getStub();
				@SuppressWarnings("unchecked")
				final Collector<IT> output = (Collector<IT>) this.taskContext.getOutputCollector();
				combiner.combine(inIter, output);
			}
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {}
}