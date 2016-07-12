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
import org.apache.flink.runtime.state.AsynchronousKvStateSnapshot;
import org.apache.flink.runtime.state.AsynchronousStateHandle;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.DefaultTimeServiceProvider;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.streaming.runtime.tasks.TimeServiceProvider;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

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

	final TimeServiceProvider timeServiceProvider;

	StreamTask<?, ?> mockTask;

	// use this as default for tests
	private AbstractStateBackend stateBackend = new MemoryStateBackend();

	/**
	 * Whether setup() was called on the operator. This is reset when calling close().
	 */
	private boolean setupCalled = false;
	
	
	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator) {
		this(operator, new ExecutionConfig());
	}

	public OneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			ExecutionConfig executionConfig) {
		this(operator, executionConfig, DefaultTimeServiceProvider.create(Executors.newSingleThreadScheduledExecutor()));
	}

	public OneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			ExecutionConfig executionConfig,
			TimeServiceProvider testTimeProvider) {
		this.operator = operator;
		this.outputList = new ConcurrentLinkedQueue<Object>();
		this.config = new StreamConfig(new Configuration());
		this.executionConfig = executionConfig;
		this.checkpointLock = new Object();

		final Environment env = new MockEnvironment("MockTwoInputTask", 3 * 1024 * 1024, new MockInputSplitProvider(), 1024);
		mockTask = mock(StreamTask.class);
		timeServiceProvider = testTimeProvider;

		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(checkpointLock);
		when(mockTask.getConfiguration()).thenReturn(config);
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
		when(mockTask.getUserCodeClassLoader()).thenReturn(this.getClass().getClassLoader());

		try {
			doAnswer(new Answer<AbstractStateBackend>() {
				@Override
				public AbstractStateBackend answer(InvocationOnMock invocationOnMock) throws Throwable {
					final String operatorIdentifier = (String) invocationOnMock.getArguments()[0];
					final TypeSerializer<?> keySerializer = (TypeSerializer<?>) invocationOnMock.getArguments()[1];
					OneInputStreamOperatorTestHarness.this.stateBackend.disposeAllStateForCurrentJob();
					OneInputStreamOperatorTestHarness.this.stateBackend.initializeForJob(env, operatorIdentifier, keySerializer);
					return OneInputStreamOperatorTestHarness.this.stateBackend;
				}
			}).when(mockTask).createStateBackend(any(String.class), any(TypeSerializer.class));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				final long execTime = (Long) invocation.getArguments()[0];
				final Triggerable target = (Triggerable) invocation.getArguments()[1];

				timeServiceProvider.registerTimer(
						execTime, new TriggerTask(checkpointLock, target, execTime));
				return null;
			}
		}).when(mockTask).registerTimer(anyLong(), any(Triggerable.class));

		doAnswer(new Answer<Long>() {
			@Override
			public Long answer(InvocationOnMock invocation) throws Throwable {
				return timeServiceProvider.getCurrentProcessingTime();
			}
		}).when(mockTask).getCurrentProcessingTime();
	}

	public void setStateBackend(AbstractStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	public Object getCheckpointLock() {
		return mockTask.getCheckpointLock();
	}

	public Environment getEnvironment() {
		return this.mockTask.getEnvironment();
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
	 * Calls
	 * {@link org.apache.flink.streaming.api.operators.StreamOperator#setup(StreamTask, StreamConfig, Output)} ()}
	 */
	public void setup() throws Exception {
		operator.setup(mockTask, config, new MockOutput());
		setupCalled = true;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#open()}. This also
	 * calls {@link org.apache.flink.streaming.api.operators.StreamOperator#setup(StreamTask, StreamConfig, Output)}
	 * if it was not called before.
	 */
	public void open() throws Exception {
		if (!setupCalled) {
			setup();
		}
		operator.open();
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#snapshotOperatorState(long, long)}
	 */
	public StreamTaskState snapshot(long checkpointId, long timestamp) throws Exception {
		StreamTaskState snapshot = operator.snapshotOperatorState(checkpointId, timestamp);
		// materialize asynchronous state handles
		if (snapshot != null) {
			if (snapshot.getFunctionState() instanceof AsynchronousStateHandle) {
				AsynchronousStateHandle<Serializable> asyncState = (AsynchronousStateHandle<Serializable>) snapshot.getFunctionState();
				snapshot.setFunctionState(asyncState.materialize());
			}
			if (snapshot.getOperatorState() instanceof AsynchronousStateHandle) {
				AsynchronousStateHandle<?> asyncState = (AsynchronousStateHandle<?>) snapshot.getOperatorState();
				snapshot.setOperatorState(asyncState.materialize());
			}
			if (snapshot.getKvStates() != null) {
				Set<String> keys = snapshot.getKvStates().keySet();
				HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> kvStates = snapshot.getKvStates();
				for (String key: keys) {
					if (kvStates.get(key) instanceof AsynchronousKvStateSnapshot) {
						AsynchronousKvStateSnapshot<?, ?, ?, ?, ?> asyncHandle = (AsynchronousKvStateSnapshot<?, ?, ?, ?, ?>) kvStates.get(key);
						kvStates.put(key, asyncHandle.materialize());
					}
				}
			}

		}
		return snapshot;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#notifyOfCompletedCheckpoint(long)}
	 */
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		operator.notifyOfCompletedCheckpoint(checkpointId);
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#restoreState(StreamTaskState)}
	 */
	public void restore(StreamTaskState snapshot, long recoveryTimestamp) throws Exception {
		operator.restoreState(snapshot);
	}

	/**
	 * Calls close and dispose on the operator.
	 */
	public void close() throws Exception {
		operator.close();
		operator.dispose();
		if (timeServiceProvider != null) {
			timeServiceProvider.shutdownService();
		}
		setupCalled = false;
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

	private static final class TriggerTask implements Runnable {

		private final Object lock;
		private final Triggerable target;
		private final long timestamp;

		TriggerTask(final Object lock, Triggerable target, long timestamp) {
			this.lock = lock;
			this.target = target;
			this.timestamp = timestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				try {
					target.trigger(timestamp);
				} catch (Throwable t) {
					try {
						throw t;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
