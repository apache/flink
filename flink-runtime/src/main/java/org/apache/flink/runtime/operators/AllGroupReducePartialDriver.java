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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.util.NonReusingMutableToRegularIteratorWrapper;
import org.apache.flink.runtime.util.ReusingMutableToRegularIteratorWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-chained driver for the partial group reduce operator that acts like a combiner with a custom output type OUT.
 * Like @org.apache.flink.runtime.operators.GroupReducePartialDriver but without grouping and sorting. May emit partially
 * reduced results.
 * 
 * @see org.apache.flink.api.common.functions.GroupReduceFunction
 */
public class AllGroupReducePartialDriver<IN, OUT> implements PactDriver<GroupReduceFunction<IN, OUT>, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(GroupReducePartialDriver.class);

	private PactTaskContext<GroupReduceFunction<IN, OUT>, OUT> taskContext;

	private boolean objectReuseEnabled = false;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GroupReduceFunction<IN, OUT>, OUT> context) {
		this.taskContext = context;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GroupReduceFunction<IN, OUT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GroupReduceFunction<IN, OUT>> clazz = (Class<GroupReduceFunction<IN, OUT>>) (Class<?>) GroupReduceFunction.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 0;
	}

	@Override
	public void prepare() throws Exception {
		if(this.taskContext.getTaskConfig().getDriverStrategy() != DriverStrategy.ALL_GROUP_REDUCE_PARTIAL){
			throw new Exception("Invalid strategy " + this.taskContext.getTaskConfig().getDriverStrategy() + " for " +
					"group reduce partial.");
		}

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("GroupReducePartialDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("GroupReducePartial starting.");
		}

		final TypeSerializerFactory<IN> serializerFactory = this.taskContext.getInputSerializer(0);
		TypeSerializer<IN> serializer = serializerFactory.getSerializer();

		final MutableObjectIterator<IN> in = this.taskContext.getInput(0);
		final GroupReduceFunction<IN, OUT> reducer = this.taskContext.getStub();
		final Collector<OUT> output = this.taskContext.getOutputCollector();

		if (objectReuseEnabled) {
			final ReusingMutableToRegularIteratorWrapper<IN> inIter = new ReusingMutableToRegularIteratorWrapper<IN>(in, serializer);

			if (inIter.hasNext()) {
					reducer.reduce(inIter, output);

			}

		} else {
			final NonReusingMutableToRegularIteratorWrapper<IN> inIter = new NonReusingMutableToRegularIteratorWrapper<IN>(in, serializer);

			if (inIter.hasNext()) {
					reducer.reduce(inIter, output);
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

