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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.state.StateBackend;
import org.apache.flink.streaming.api.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.mockito.stubbing.OngoingStubbing;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A test harness for testing a {@link OneInputStreamOperator}.
 *
 * <p>
 * This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements
 * and watermarks can be retrieved. You are free to modify these.
 */
public class OneInputStreamOperatorTestHarness<IN, OUT> {

	final OneInputStreamOperator<IN, OUT> operator;

	final ConcurrentLinkedQueue<Object> outputList;

	final ExecutionConfig executionConfig;
	
	final Object checkpointLock;

	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator) {
		this(operator, new StreamConfig(new Configuration()));
	}
	
	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator, StreamConfig config) {
		this.operator = operator;
		this.outputList = new ConcurrentLinkedQueue<Object>();
		this.executionConfig = new ExecutionConfig();
		this.checkpointLock = new Object();

		Environment env = new MockEnvironment("MockTwoInputTask", 3 * 1024 * 1024, new MockInputSplitProvider(), 1024);
		StreamTask<?, ?> mockTask = mock(StreamTask.class);
		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(checkpointLock);
		when(mockTask.getConfiguration()).thenReturn(config);
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
		
		// ugly Java generic hacks
		@SuppressWarnings("unchecked")
		OngoingStubbing<StateBackend<?>> stubbing = 
				(OngoingStubbing<StateBackend<?>>) (OngoingStubbing<?>) when(mockTask.getStateBackend());
		stubbing.thenReturn(MemoryStateBackend.defaultInstance());

		operator.setup(mockTask, new StreamConfig(new Configuration()), new MockOutput());
	}

	/**
	 * Get all the output from the task. This contains StreamRecords and Events interleaved. Use
	 * {@link org.apache.flink.streaming.util.TestHarnessUtil#getStreamRecordsFromOutput(java.util.List)}
	 * to extract only the StreamRecords.
	 */
	public ConcurrentLinkedQueue<Object> getOutput() {
		return outputList;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#open()}
	 */
	public void open() throws Exception {
		operator.open();
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
