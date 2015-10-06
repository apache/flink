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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Reduce task which is executed by a Task Manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceFunction
 * implementation.
 * <p>
 * The ReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the ReduceFunction.
 * 
 * @see org.apache.flink.api.common.functions.ReduceFunction
 */
public class AllReduceDriver<T> implements Driver<ReduceFunction<T>, T> {
	
	private static final Logger LOG = LoggerFactory.getLogger(AllReduceDriver.class);

	private TaskContext<ReduceFunction<T>, T> taskContext;
	
	private MutableObjectIterator<T> input;

	private TypeSerializer<T> serializer;
	
	private boolean running;

	private boolean objectReuseEnabled = false;

	// ------------------------------------------------------------------------

	@Override
	public void setup(TaskContext<ReduceFunction<T>, T> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<ReduceFunction<T>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<ReduceFunction<T>> clazz = (Class<ReduceFunction<T>>) (Class<?>) ReduceFunction.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 0;
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

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("AllReduceDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("AllReduce preprocessing done. Running Reducer code."));
		}

		final ReduceFunction<T> stub = this.taskContext.getStub();
		final MutableObjectIterator<T> input = this.input;
		final TypeSerializer<T> serializer = this.serializer;


		if (objectReuseEnabled) {
			T val1 = serializer.createInstance();

			if ((val1 = input.next(val1)) == null) {
				return;
			}

			T val2 = serializer.createInstance();
			while (running && (val2 = input.next(val2)) != null) {
				val1 = stub.reduce(val1, val2);
			}

			this.taskContext.getOutputCollector().collect(val1);
		} else {
			T val1 = serializer.createInstance();

			if ((val1 = input.next(val1)) == null) {
				return;
			}

			T val2;
			while (running && (val2 = input.next(serializer.createInstance())) != null) {
				val1 = stub.reduce(val1, val2);
			}

			this.taskContext.getOutputCollector().collect(val1);
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}
