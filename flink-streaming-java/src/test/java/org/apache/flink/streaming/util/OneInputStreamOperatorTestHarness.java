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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
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

	final StreamConfig config;
	
	final ExecutionConfig executionConfig;
	
	final Object checkpointLock;

	StreamTask<?, ?> mockTask;
	
	
	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator) {
		this(operator, new ExecutionConfig());
	}
	
	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator, ExecutionConfig executionConfig) {
		this.operator = operator;
		this.outputList = new ConcurrentLinkedQueue<Object>();
		this.config = new StreamConfig(new Configuration());
		this.executionConfig = executionConfig;
		this.checkpointLock = new Object();

		final Environment env = new MockEnvironment("MockTwoInputTask", 3 * 1024 * 1024, new MockInputSplitProvider(), 1024);
		mockTask = mock(StreamTask.class);
		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(checkpointLock);
		when(mockTask.getConfiguration()).thenReturn(config);
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);

		try {
			doAnswer(new Answer<AbstractStateBackend>() {
				@Override
				public AbstractStateBackend answer(InvocationOnMock invocationOnMock) throws Throwable {
					final String operatorIdentifier = (String) invocationOnMock.getArguments()[0];
					final TypeSerializer<?> keySerializer = (TypeSerializer<?>) invocationOnMock.getArguments()[1];
					MemoryStateBackend backend = MemoryStateBackend.create();
					backend.initializeForJob(env, operatorIdentifier, keySerializer);
					return backend;
				}
			}).when(mockTask).createStateBackend(any(String.class), any(TypeSerializer.class));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				final long execTime = (Long) invocation.getArguments()[0];
				final Triggerable target = (Triggerable) invocation.getArguments()[1];
				
				Thread caller = new Thread() {
					@Override
					public void run() {
						final long delay = execTime - System.currentTimeMillis();
						if (delay > 0) {
							try {
								Thread.sleep(delay);
							} catch (InterruptedException ignored) {}
						}
						
						synchronized (checkpointLock) {
							try {
								target.trigger(execTime);
							} catch (Exception ignored) {}
						}
					}
				};
				caller.start();
				
				return null;
			}
		}).when(mockTask).registerTimer(anyLong(), any(Triggerable.class));
	}

	public <K> void configureForKeyedStream(KeySelector<IN, K> keySelector, TypeInformation<K> keyType) {
		ClosureCleaner.clean(keySelector, false);
		config.setStatePartitioner(0, keySelector);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));
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
		operator.setup(mockTask, config, new MockOutput());

		operator.open();
	}

	/**
	 * Calls close and dispose on the operator.
	 */
	public void close() throws Exception {
		operator.close();
		operator.dispose();
	}

	public void processElement(StreamRecord<IN> element) throws Exception {
		operator.setKeyContextElement1(element);
		operator.processElement(element);
	}

	public void processElements(Collection<StreamRecord<IN>> elements) throws Exception {
		for (StreamRecord<IN> element: elements) {
			operator.setKeyContextElement1(element);
			operator.processElement(element);
		}
	}

	public void processWatermark(Watermark mark) throws Exception {
		operator.processWatermark(mark);
	}

	private class MockOutput implements Output<StreamRecord<OUT>> {

		private TypeSerializer<OUT> outputSerializer;

		@Override
		public void emitWatermark(Watermark mark) {
			outputList.add(mark);
		}

		@Override
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
