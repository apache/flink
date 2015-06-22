/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A test harness for testing a {@link OneInputStreamOperator}.
 *
 * <p>
 * This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements
 * and watermarks can be retrieved. You are free to modify these.
 */
public class OneInputStreamOperatorTestHarness<IN, OUT> {

	OneInputStreamOperator<IN, OUT> operator;

	ConcurrentLinkedQueue outputList;

	ExecutionConfig executionConfig;

	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator) {
		this.operator = operator;

		outputList = new ConcurrentLinkedQueue();

		executionConfig = new ExecutionConfig();

		StreamingRuntimeContext runtimeContext =  new StreamingRuntimeContext(
				"MockTwoInputTask",
				new MockEnvironment(3 * 1024 * 1024, new MockInputSplitProvider(), 1024),
				getClass().getClassLoader(),
				executionConfig,
				null,
				new LocalStateHandle.LocalStateHandleProvider<Serializable>(),
				new HashMap<String, Accumulator<?, ?>>());

		operator.setup(new MockOutput(), runtimeContext);
	}

	/**
	 * Get all the output from the task. This contains StreamRecords and Events interleaved. Use
	 * {@link org.apache.flink.streaming.util.TestHarnessUtil#getStreamRecordsFromOutput(java.util.List)}
	 * to extract only the StreamRecords.
	 */
	public Queue getOutput() {
		return outputList;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#open(org.apache.flink.configuration.Configuration)}
	 * with an empty {@link org.apache.flink.configuration.Configuration}.
	 */
	public void open() throws Exception {
		operator.open(new Configuration());
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#open(org.apache.flink.configuration.Configuration)}
	 * with the given {@link org.apache.flink.configuration.Configuration}.
	 */
	public void open(Configuration config) throws Exception {
		operator.open(config);
	}

	/**
	 * Calls close on the operator.
	 */
	public void close() throws Exception {
		operator.close();
	}

	public void processElement(StreamRecord<IN> element) throws Exception {
		operator.processElement(element);
	}

	public void processElements(Collection<StreamRecord<IN>> elements) throws Exception {
		for (StreamRecord<IN> element: elements) {
			operator.processElement(element);
		}
	}

	public void processWatermark(Watermark mark) throws Exception {
		operator.processWatermark(mark);
	}

	private class MockOutput implements Output<StreamRecord<OUT>> {

		private TypeSerializer<OUT> outputSerializer;

		@Override
		@SuppressWarnings("unchecked")
		public void emitWatermark(Watermark mark) {
			outputList.add(mark);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void collect(StreamRecord<OUT> element) {
			if (outputSerializer == null) {
				outputSerializer = TypeExtractor.getForObject(element.getValue()).createSerializer(executionConfig);
			}
			outputList.add(new StreamRecord<OUT>(outputSerializer.copy(element.getValue()),
					element.getTimestamp()));
		}

		@Override
		public void close() {
			// ignore
		}
	}
}
