/*
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.KeyGroupedIterator;
import org.apache.flink.runtime.util.KeyGroupedIteratorImmutable;
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
public class GroupReduceDriver<IT, OT> implements PactDriver<GroupReduceFunction<IT, OT>, OT> {
	
	private static final Logger LOG = LoggerFactory.getLogger(GroupReduceDriver.class);

	private PactTaskContext<GroupReduceFunction<IT, OT>, OT> taskContext;
	
	private MutableObjectIterator<IT> input;

	private TypeSerializer<IT> serializer;

	private TypeComparator<IT> comparator;
	
	private boolean mutableObjectMode = false;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GroupReduceFunction<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
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
	public int getNumberOfDriverComparators() {
		return 1;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.SORTED_GROUP_REDUCE) {
			throw new Exception("Unrecognized driver strategy for GroupReduce driver: " + config.getDriverStrategy().name());
		}
		this.serializer = this.taskContext.<IT>getInputSerializer(0).getSerializer();
		this.comparator = this.taskContext.getDriverComparator(0);
		this.input = this.taskContext.getInput(0);
		
		this.mutableObjectMode = config.getMutableObjectMode();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("GroupReduceDriver uses " + (this.mutableObjectMode ? "MUTABLE" : "IMMUTABLE") + " object mode.");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("GroupReducer preprocessing done. Running GroupReducer code."));
		}

		// cache references on the stack
		final GroupReduceFunction<IT, OT> stub = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();
		
		if (mutableObjectMode) {
			final KeyGroupedIterator<IT> iter = new KeyGroupedIterator<IT>(this.input, this.serializer, this.comparator);
			// run stub implementation
			while (this.running && iter.nextKey()) {
				stub.reduce(iter.getValues(), output);
			}
		}
		else {
			final KeyGroupedIteratorImmutable<IT> iter = new KeyGroupedIteratorImmutable<IT>(this.input, this.serializer, this.comparator);
			// run stub implementation
			while (this.running && iter.nextKey()) {
				stub.reduce(iter.getValues(), output);
			}
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}