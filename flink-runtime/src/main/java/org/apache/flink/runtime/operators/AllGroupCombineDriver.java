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
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.apache.flink.runtime.util.NonReusingMutableToRegularIteratorWrapper;
import org.apache.flink.runtime.util.ReusingMutableToRegularIteratorWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Non-chained driver for the partial group reduce operator that acts like a combiner with a custom output type OUT.
* Like @org.apache.flink.runtime.operators.GroupCombineDriver but without grouping and sorting. May emit partially
* reduced results.
*
* @see GroupCombineFunction
*/
public class AllGroupCombineDriver<IN, OUT> implements Driver<GroupCombineFunction<IN, OUT>, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(AllGroupCombineDriver.class);

	private TaskContext<GroupCombineFunction<IN, OUT>, OUT> taskContext;

	private boolean objectReuseEnabled = false;

	// ------------------------------------------------------------------------

	@Override
	public void setup(TaskContext<GroupCombineFunction<IN, OUT>, OUT> context) {
		this.taskContext = context;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GroupCombineFunction<IN, OUT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GroupCombineFunction<IN, OUT>> clazz = (Class<GroupCombineFunction<IN, OUT>>) (Class<?>) GroupCombineFunction.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 0;
	}

	@Override
	public void prepare() throws Exception {
		final DriverStrategy driverStrategy = this.taskContext.getTaskConfig().getDriverStrategy();
		if(driverStrategy != DriverStrategy.ALL_GROUP_COMBINE){
			throw new Exception("Invalid strategy " + driverStrategy + " for " +
					"GroupCombine.");
		}

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("GroupCombineDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("AllGroupCombine starting.");
		}

		final Counter numRecordsIn = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
		final Counter numRecordsOut = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

		final TypeSerializerFactory<IN> serializerFactory = this.taskContext.getInputSerializer(0);
		TypeSerializer<IN> serializer = serializerFactory.getSerializer();

		final MutableObjectIterator<IN> in = new CountingMutableObjectIterator<>(this.taskContext.<IN>getInput(0), numRecordsIn);
		final GroupCombineFunction<IN, OUT> reducer = this.taskContext.getStub();
		final Collector<OUT> output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

		if (objectReuseEnabled) {
			final ReusingMutableToRegularIteratorWrapper<IN> inIter = new ReusingMutableToRegularIteratorWrapper<IN>(in, serializer);

			if (inIter.hasNext()) {
					reducer.combine(inIter, output);

			}

		} else {
			final NonReusingMutableToRegularIteratorWrapper<IN> inIter = new NonReusingMutableToRegularIteratorWrapper<IN>(in, serializer);

			if (inIter.hasNext()) {
					reducer.combine(inIter, output);
			}
		}

	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void cancel() {
	}
}

