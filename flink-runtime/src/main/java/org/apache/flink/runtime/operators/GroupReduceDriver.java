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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * GroupReduce task which is executed by a Task Manager. The task has a
 * single input and one or multiple outputs. It is provided with a GroupReduceFunction
 * implementation.
 * <p>
 * The GroupReduceDriver creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the GroupReduceFunction.
 * 
 * @see org.apache.flink.api.common.functions.GroupReduceFunction
 */
public class GroupReduceDriver<IT, OT> implements Driver<GroupReduceFunction<IT, OT>, OT> {
	
	private static final Logger LOG = LoggerFactory.getLogger(GroupReduceDriver.class);

	private TaskContext<GroupReduceFunction<IT, OT>, OT> taskContext;
	
	private MutableObjectIterator<IT> input;

	private TypeSerializer<IT> serializer;

	private TypeComparator<IT> comparator;

	private volatile boolean running;

	private boolean objectReuseEnabled = false;

	// ------------------------------------------------------------------------

	@Override
	public void setup(TaskContext<GroupReduceFunction<IT, OT>, OT> context) {
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
		final Counter numRecordsIn = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
		
		this.serializer = this.taskContext.<IT>getInputSerializer(0).getSerializer();
		this.comparator = this.taskContext.getDriverComparator(0);
		this.input = new CountingMutableObjectIterator<>(this.taskContext.<IT>getInput(0), numRecordsIn);

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("GroupReduceDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("GroupReducer preprocessing done. Running GroupReducer code."));
		}
		final Counter numRecordsOut = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

		// cache references on the stack
		final GroupReduceFunction<IT, OT> stub = this.taskContext.getStub();
		final Collector<OT> output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
		
		if (objectReuseEnabled) {
			final ReusingKeyGroupedIterator<IT> iter = new ReusingKeyGroupedIterator<IT>(this.input, this.serializer, this.comparator);
			// run stub implementation
			while (this.running && iter.nextKey()) {
				stub.reduce(iter.getValues(), output);
			}
		}
		else {
			final NonReusingKeyGroupedIterator<IT> iter = new NonReusingKeyGroupedIterator<IT>(this.input, this.comparator);
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
