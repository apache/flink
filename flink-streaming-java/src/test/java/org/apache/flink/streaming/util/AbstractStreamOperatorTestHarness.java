/*
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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.checkpoint.OperatorStateRepartitioner;
import org.apache.flink.runtime.checkpoint.RoundRobinOperatorStateRepartitioner;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.ClosableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OperatorSnapshotResult;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.FutureUtil;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Base class for {@code AbstractStreamOperator} test harnesses.
 */
public class AbstractStreamOperatorTestHarness<OUT> {

	final protected StreamOperator<OUT> operator;

	final protected ConcurrentLinkedQueue<Object> outputList;

	final protected StreamConfig config;

	final protected ExecutionConfig executionConfig;

	final protected TestProcessingTimeService processingTimeService;

	final protected StreamTask<?, ?> mockTask;

	final Environment environment;

	ClosableRegistry closableRegistry;

	// use this as default for tests
	protected AbstractStateBackend stateBackend = new MemoryStateBackend();

	private final Object checkpointLock;

	private final OperatorStateRepartitioner operatorStateRepartitioner =
			RoundRobinOperatorStateRepartitioner.INSTANCE;

	/**
	 * Whether setup() was called on the operator. This is reset when calling close().
	 */
	private boolean setupCalled = false;
	private boolean initializeCalled = false;

	private volatile boolean wasFailedExternally = false;

	public AbstractStreamOperatorTestHarness(
			StreamOperator<OUT> operator,
			int maxParallelism,
			int numSubtasks,
			int subtaskIndex) throws Exception {
		this.operator = operator;
		this.outputList = new ConcurrentLinkedQueue<>();
		Configuration underlyingConfig = new Configuration();
		this.config = new StreamConfig(underlyingConfig);
		this.config.setCheckpointingEnabled(true);
		this.executionConfig = new ExecutionConfig();
		this.closableRegistry = new ClosableRegistry();
		this.checkpointLock = new Object();

		environment = new MockEnvironment(
				"MockTask",
				3 * 1024 * 1024,
				new MockInputSplitProvider(),
				1024,
				underlyingConfig,
				executionConfig,
				maxParallelism,
				numSubtasks,
				subtaskIndex);

		mockTask = mock(StreamTask.class);
		processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(0);

		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(checkpointLock);
		when(mockTask.getConfiguration()).thenReturn(config);
		when(mockTask.getTaskConfiguration()).thenReturn(underlyingConfig);
		when(mockTask.getEnvironment()).thenReturn(environment);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
		when(mockTask.getUserCodeClassLoader()).thenReturn(this.getClass().getClassLoader());
		when(mockTask.getCancelables()).thenReturn(this.closableRegistry);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				wasFailedExternally = true;
				return null;
			}
		}).when(mockTask).handleAsyncException(any(String.class), any(Throwable.class));

		try {
			doAnswer(new Answer<CheckpointStreamFactory>() {
				@Override
				public CheckpointStreamFactory answer(InvocationOnMock invocationOnMock) throws Throwable {

					final StreamOperator<?> operator = (StreamOperator<?>) invocationOnMock.getArguments()[0];
					return stateBackend.createStreamFactory(new JobID(), operator.getClass().getSimpleName());
				}
			}).when(mockTask).createCheckpointStreamFactory(any(StreamOperator.class));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

		try {
			doAnswer(new Answer<OperatorStateBackend>() {
				@Override
				public OperatorStateBackend answer(InvocationOnMock invocationOnMock) throws Throwable {
					final StreamOperator<?> operator = (StreamOperator<?>) invocationOnMock.getArguments()[0];
					final Collection<OperatorStateHandle> stateHandles = (Collection<OperatorStateHandle>) invocationOnMock.getArguments()[1];
					OperatorStateBackend osb;
					if (null == stateHandles) {
						osb = stateBackend.createOperatorStateBackend(environment, operator.getClass().getSimpleName());
					} else {
						osb = stateBackend.restoreOperatorStateBackend(environment, operator.getClass().getSimpleName(), stateHandles);
					}
					mockTask.getCancelables().registerClosable(osb);
					return osb;
				}
			}).when(mockTask).createOperatorStateBackend(any(StreamOperator.class), any(Collection.class));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

		doAnswer(new Answer<ProcessingTimeService>() {
			@Override
			public ProcessingTimeService answer(InvocationOnMock invocation) throws Throwable {
				return processingTimeService;
			}
		}).when(mockTask).getProcessingTimeService();
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

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	/**
	 * Get all the output from the task. This contains StreamRecords and Events interleaved.
	 */
	public ConcurrentLinkedQueue<Object> getOutput() {
		return outputList;
	}

	/**
	 * Get only the {@link StreamRecord StreamRecords} emitted by the operator.
	 */
	@SuppressWarnings("unchecked")
	public List<StreamRecord<? extends OUT>> extractOutputStreamRecords() {
		List<StreamRecord<? extends OUT>> resultElements = new LinkedList<>();
		for (Object e: getOutput()) {
			if (e instanceof StreamRecord) {
				resultElements.add((StreamRecord<OUT>) e);
			}
		}
		return resultElements;
	}

	/**
	 * Calls
	 * {@link StreamOperator#setup(StreamTask, StreamConfig, Output)} ()}
	 */
	public void setup() throws Exception {
		operator.setup(mockTask, config, new MockOutput());
		setupCalled = true;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#initializeState(OperatorStateHandles)}.
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#setup(StreamTask, StreamConfig, Output)}
	 * if it was not called before.
	 *
	 * <p>This will reshape the state handles to include only those key-group states
	 * in the local key-group range and the operator states that would be assigned to the local
	 * subtask.
	 */
	public void initializeState(OperatorStateHandles operatorStateHandles) throws Exception {
		if (!setupCalled) {
			setup();
		}

		if (operatorStateHandles != null) {
			int numKeyGroups = getEnvironment().getTaskInfo().getNumberOfKeyGroups();
			int numSubtasks = getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
			int subtaskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();

			// create a new OperatorStateHandles that only contains the state for our key-groups

			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
					numKeyGroups,
					numSubtasks);

			KeyGroupRange localKeyGroupRange =
					keyGroupPartitions.get(subtaskIndex);

			List<KeyGroupsStateHandle> localManagedKeyGroupState = null;
			if (operatorStateHandles.getManagedKeyedState() != null) {
				localManagedKeyGroupState = StateAssignmentOperation.getKeyGroupsStateHandles(
						operatorStateHandles.getManagedKeyedState(),
						localKeyGroupRange);
			}

			List<KeyGroupsStateHandle> localRawKeyGroupState = null;
			if (operatorStateHandles.getRawKeyedState() != null) {
				localRawKeyGroupState = StateAssignmentOperation.getKeyGroupsStateHandles(
						operatorStateHandles.getRawKeyedState(),
						localKeyGroupRange);
			}

			List<OperatorStateHandle> managedOperatorState = new ArrayList<>();
			if (operatorStateHandles.getManagedOperatorState() != null) {
				managedOperatorState.addAll(operatorStateHandles.getManagedOperatorState());
			}
			Collection<OperatorStateHandle> localManagedOperatorState = operatorStateRepartitioner.repartitionState(
					managedOperatorState,
					numSubtasks).get(subtaskIndex);

			List<OperatorStateHandle> rawOperatorState = new ArrayList<>();
			if (operatorStateHandles.getRawOperatorState() != null) {
				rawOperatorState.addAll(operatorStateHandles.getRawOperatorState());
			}
			Collection<OperatorStateHandle> localRawOperatorState = operatorStateRepartitioner.repartitionState(
					rawOperatorState,
					numSubtasks).get(subtaskIndex);

			OperatorStateHandles massagedOperatorStateHandles = new OperatorStateHandles(
					0,
					null,
					localManagedKeyGroupState,
					localRawKeyGroupState,
					localManagedOperatorState,
					localRawOperatorState);

			operator.initializeState(massagedOperatorStateHandles);
		} else {
			operator.initializeState(null);
		}
		initializeCalled = true;
	}


	/**
	 * Calls {@link StreamOperator#open()}. This also
	 * calls {@link StreamOperator#setup(StreamTask, StreamConfig, Output)}
	 * if it was not called before.
	 */
	public void open() throws Exception {
		if (!initializeCalled) {
			initializeState(null);
		}
		operator.open();
	}

	/**
	 * Calls {@link StreamOperator#snapshotState(long, long, CheckpointStreamFactory)}.
	 */
	public OperatorStateHandles snapshot(long checkpointId, long timestamp) throws Exception {

		CheckpointStreamFactory streamFactory = stateBackend.createStreamFactory(
				new JobID(),
				"test_op");

		OperatorSnapshotResult operatorStateResult = operator.snapshotState(checkpointId, timestamp, streamFactory);

		KeyGroupsStateHandle keyedManaged = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getKeyedStateManagedFuture());
		KeyGroupsStateHandle keyedRaw = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getKeyedStateRawFuture());

		OperatorStateHandle opManaged = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getOperatorStateManagedFuture());
		OperatorStateHandle opRaw = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getOperatorStateRawFuture());

		OperatorStateHandles handles = new OperatorStateHandles(
			0,
			null,
			keyedManaged != null ? Collections.singletonList(keyedManaged) : null,
			keyedRaw != null ? Collections.singletonList(keyedRaw) : null,
			opManaged != null ? Collections.singletonList(opManaged) : null,
			opRaw != null ? Collections.singletonList(opRaw) : null);
		return handles;
	}

	/**
	 * Calls {@link StreamCheckpointedOperator#snapshotState(FSDataOutputStream, long, long)} if
	 * the operator implements this interface.
	 */
	@Deprecated
	public StreamStateHandle snapshotLegacy(long checkpointId, long timestamp) throws Exception {

		CheckpointStreamFactory.CheckpointStateOutputStream outStream = stateBackend.createStreamFactory(
				new JobID(),
				"test_op").createCheckpointStateOutputStream(checkpointId, timestamp);
		if(operator instanceof StreamCheckpointedOperator) {
			((StreamCheckpointedOperator) operator).snapshotState(outStream, checkpointId, timestamp);
			return outStream.closeAndGetHandle();
		} else {
			throw new RuntimeException("Operator is not StreamCheckpointedOperator");
		}
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#notifyOfCompletedCheckpoint(long)} ()}
	 */
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		operator.notifyOfCompletedCheckpoint(checkpointId);
	}

	/**
	 * Calls {@link StreamCheckpointedOperator#restoreState(FSDataInputStream)} if
	 * the operator implements this interface.
	 */	@Deprecated
	public void restore(StreamStateHandle snapshot) throws Exception {
		if(operator instanceof StreamCheckpointedOperator) {
			try (FSDataInputStream in = snapshot.openInputStream()) {
				((StreamCheckpointedOperator) operator).restoreState(in);
			}
		} else {
			throw new RuntimeException("Operator is not StreamCheckpointedOperator");
		}
	}

	/**
	 * Calls close and dispose on the operator.
	 */
	public void close() throws Exception {
		operator.close();
		operator.dispose();
		if (processingTimeService != null) {
			processingTimeService.shutdownService();
		}
		setupCalled = false;
	}

	public void setProcessingTime(long time) throws Exception {
		processingTimeService.setCurrentTime(time);
	}

	public long getProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.config.setTimeCharacteristic(timeCharacteristic);
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return this.config.getTimeCharacteristic();
	}

	public boolean wasFailedExternally() {
		return wasFailedExternally;
	}

	private class MockOutput implements Output<StreamRecord<OUT>> {

		private TypeSerializer<OUT> outputSerializer;

		@Override
		public void emitWatermark(Watermark mark) {
			outputList.add(mark);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			outputList.add(latencyMarker);
		}

		@Override
		public void collect(StreamRecord<OUT> element) {
			if (outputSerializer == null) {
				outputSerializer = TypeExtractor.getForObject(element.getValue()).createSerializer(executionConfig);
			}
			if (element.hasTimestamp()) {
				outputList.add(new StreamRecord<>(outputSerializer.copy(element.getValue()),element.getTimestamp()));
			} else {
				outputList.add(new StreamRecord<>(outputSerializer.copy(element.getValue())));
			}
		}

		@Override
		public void close() {
			// ignore
		}
	}
}
