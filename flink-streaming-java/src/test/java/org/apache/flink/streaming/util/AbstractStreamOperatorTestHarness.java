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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorStateRepartitioner;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.RoundRobinOperatorStateRepartitioner;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorTest;
import org.apache.flink.streaming.api.operators.OperatorSnapshotResult;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for {@code AbstractStreamOperator} test harnesses.
 */
public class AbstractStreamOperatorTestHarness<OUT> implements AutoCloseable {

	protected final StreamOperator<OUT> operator;

	protected final ConcurrentLinkedQueue<Object> outputList;

	protected final Map<OutputTag<?>, ConcurrentLinkedQueue<Object>> sideOutputLists;

	protected final StreamConfig config;

	protected final ExecutionConfig executionConfig;

	protected final TestProcessingTimeService processingTimeService;

	protected final StreamTask<?, ?> mockTask;

	final Environment environment;

	CloseableRegistry closableRegistry;

	// use this as default for tests
	protected StateBackend stateBackend = new MemoryStateBackend();

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

		this(
			operator,
			new MockEnvironment(
				"MockTask",
				3 * 1024 * 1024,
				new MockInputSplitProvider(),
				1024,
				new Configuration(),
				new ExecutionConfig(),
				maxParallelism,
				numSubtasks,
				subtaskIndex));
	}

	public AbstractStreamOperatorTestHarness(
			StreamOperator<OUT> operator,
			final Environment environment) throws Exception {
		this.operator = operator;
		this.outputList = new ConcurrentLinkedQueue<>();
		this.sideOutputLists = new HashMap<>();

		Configuration underlyingConfig = environment.getTaskConfiguration();
		this.config = new StreamConfig(underlyingConfig);
		this.config.setCheckpointingEnabled(true);
		this.config.setOperatorID(new OperatorID());
		this.executionConfig = environment.getExecutionConfig();
		this.closableRegistry = new CloseableRegistry();
		this.checkpointLock = new Object();

		this.environment = Preconditions.checkNotNull(environment);

		mockTask = mock(StreamTask.class);
		processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(0);

		StreamStatusMaintainer mockStreamStatusMaintainer = new StreamStatusMaintainer() {
			StreamStatus currentStreamStatus = StreamStatus.ACTIVE;

			@Override
			public void toggleStreamStatus(StreamStatus streamStatus) {
				if (!currentStreamStatus.equals(streamStatus)) {
					currentStreamStatus = streamStatus;
				}
			}

			@Override
			public StreamStatus getStreamStatus() {
				return currentStreamStatus;
			}
		};

		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(checkpointLock);
		when(mockTask.getConfiguration()).thenReturn(config);
		when(mockTask.getTaskConfiguration()).thenReturn(underlyingConfig);
		when(mockTask.getEnvironment()).thenReturn(environment);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);

		ClassLoader cl = environment.getUserClassLoader();
		when(mockTask.getUserCodeClassLoader()).thenReturn(cl);

		when(mockTask.getCancelables()).thenReturn(this.closableRegistry);
		when(mockTask.getStreamStatusMaintainer()).thenReturn(mockStreamStatusMaintainer);

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

					osb = stateBackend.createOperatorStateBackend(
						environment,
						operator.getClass().getSimpleName());

					mockTask.getCancelables().registerClosable(osb);

					if (null != stateHandles) {
						osb.restore(stateHandles);
					}

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

	public void setStateBackend(StateBackend stateBackend) {
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

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <X> ConcurrentLinkedQueue<StreamRecord<X>> getSideOutput(OutputTag<X> tag) {
		return (ConcurrentLinkedQueue) sideOutputLists.get(tag);
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
	 * Calls {@link StreamOperator#setup(StreamTask, StreamConfig, Output)} ()}.
	 */
	public void setup() {
		setup(null);
	}

	/**
	 * Calls {@link StreamOperator#setup(StreamTask, StreamConfig, Output)} ()}.
	 */
	public void setup(TypeSerializer<OUT> outputSerializer) {
		operator.setup(mockTask, config, new MockOutput(outputSerializer));
		setupCalled = true;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#initializeState(OperatorSubtaskState)}.
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
			int numKeyGroups = getEnvironment().getTaskInfo().getMaxNumberOfParallelSubtasks();
			int numSubtasks = getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
			int subtaskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();

			// create a new OperatorStateHandles that only contains the state for our key-groups

			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
					numKeyGroups,
					numSubtasks);

			KeyGroupRange localKeyGroupRange =
					keyGroupPartitions.get(subtaskIndex);

			List<KeyedStateHandle> localManagedKeyGroupState = null;
			if (operatorStateHandles.getManagedKeyedState() != null) {
				localManagedKeyGroupState = StateAssignmentOperation.getKeyedStateHandles(
						operatorStateHandles.getManagedKeyedState(),
						localKeyGroupRange);
			}

			List<KeyedStateHandle> localRawKeyGroupState = null;
			if (operatorStateHandles.getRawKeyedState() != null) {
				localRawKeyGroupState = StateAssignmentOperation.getKeyedStateHandles(
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

			OperatorSubtaskState massagedOperatorStateHandles = new OperatorSubtaskState(
				nullToEmptyCollection(localManagedOperatorState),
				nullToEmptyCollection(localRawOperatorState),
				nullToEmptyCollection(localManagedKeyGroupState),
				nullToEmptyCollection(localRawKeyGroupState));

			operator.initializeState(massagedOperatorStateHandles);
		} else {
			operator.initializeState(null);
		}
		initializeCalled = true;
	}

	private static <T> Collection<T> nullToEmptyCollection(Collection<T> collection) {
		return collection != null ? collection : Collections.<T>emptyList();
	}

	/**
	 * Takes the different {@link OperatorStateHandles} created by calling {@link #snapshot(long, long)}
	 * on different instances of {@link AbstractStreamOperatorTestHarness} (each one representing one subtask)
	 * and repacks them into a single {@link OperatorStateHandles} so that the parallelism of the test
	 * can change arbitrarily (i.e. be able to scale both up and down).
	 *
	 * <p>After repacking the partial states, use {@link #initializeState(OperatorStateHandles)} to initialize
	 * a new instance with the resulting state. Bear in mind that for parallelism greater than one, you
	 * have to use the constructor {@link #AbstractStreamOperatorTestHarness(StreamOperator, int, int, int)}.
	 *
	 * <p><b>NOTE: </b> each of the {@code handles} in the argument list is assumed to be from a single task of a single
	 * operator (i.e. chain length of one).
	 *
	 * <p>For an example of how to use it, have a look at
	 * {@link AbstractStreamOperatorTest#testStateAndTimerStateShufflingScalingDown()}.
	 *
	 * @param handles the different states to be merged.
	 * @return the resulting state, or {@code null} if no partial states are specified.
	 */
	public static OperatorStateHandles repackageState(OperatorStateHandles... handles) throws Exception {

		if (handles.length < 1) {
			return null;
		} else if (handles.length == 1) {
			return handles[0];
		}

		List<OperatorStateHandle> mergedManagedOperatorState = new ArrayList<>(handles.length);
		List<OperatorStateHandle> mergedRawOperatorState = new ArrayList<>(handles.length);

		List<KeyedStateHandle> mergedManagedKeyedState = new ArrayList<>(handles.length);
		List<KeyedStateHandle> mergedRawKeyedState = new ArrayList<>(handles.length);

		for (OperatorStateHandles handle: handles) {

			Collection<OperatorStateHandle> managedOperatorState = handle.getManagedOperatorState();
			Collection<OperatorStateHandle> rawOperatorState = handle.getRawOperatorState();
			Collection<KeyedStateHandle> managedKeyedState = handle.getManagedKeyedState();
			Collection<KeyedStateHandle> rawKeyedState = handle.getRawKeyedState();

			if (managedOperatorState != null) {
				mergedManagedOperatorState.addAll(managedOperatorState);
			}

			if (rawOperatorState != null) {
				mergedRawOperatorState.addAll(rawOperatorState);
			}

			if (managedKeyedState != null) {
				mergedManagedKeyedState.addAll(managedKeyedState);
			}

			if (rawKeyedState != null) {
				mergedRawKeyedState.addAll(rawKeyedState);
			}
		}

		return new OperatorStateHandles(
			0,
			mergedManagedKeyedState,
			mergedRawKeyedState,
			mergedManagedOperatorState,
			mergedRawOperatorState);
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
	 * Calls {@link StreamOperator#snapshotState(long, long, CheckpointOptions)}.
	 */
	public OperatorStateHandles snapshot(long checkpointId, long timestamp) throws Exception {

		OperatorSnapshotResult operatorStateResult = operator.snapshotState(
			checkpointId,
			timestamp,
			CheckpointOptions.forFullCheckpoint());

		KeyedStateHandle keyedManaged = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getKeyedStateManagedFuture());
		KeyedStateHandle keyedRaw = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getKeyedStateRawFuture());

		OperatorStateHandle opManaged = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getOperatorStateManagedFuture());
		OperatorStateHandle opRaw = FutureUtil.runIfNotDoneAndGet(operatorStateResult.getOperatorStateRawFuture());

		return new OperatorStateHandles(
			0,
			keyedManaged != null ? Collections.singletonList(keyedManaged) : null,
			keyedRaw != null ? Collections.singletonList(keyedRaw) : null,
			opManaged != null ? Collections.singletonList(opManaged) : null,
			opRaw != null ? Collections.singletonList(opRaw) : null);
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#notifyOfCompletedCheckpoint(long)} ()}.
	 */
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		operator.notifyOfCompletedCheckpoint(checkpointId);
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

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		if (operator instanceof AbstractStreamOperator) {
			return ((AbstractStreamOperator) operator).numProcessingTimeTimers();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		if (operator instanceof AbstractStreamOperator) {
			return ((AbstractStreamOperator) operator).numEventTimeTimers();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	private class MockOutput implements Output<StreamRecord<OUT>> {

		private TypeSerializer<OUT> outputSerializer;

		private TypeSerializer sideOutputSerializer;

		MockOutput() {
			this(null);
		}

		MockOutput(TypeSerializer<OUT> outputSerializer) {
			this.outputSerializer = outputSerializer;
		}

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
				outputList.add(new StreamRecord<>(outputSerializer.copy(element.getValue()), element.getTimestamp()));
			} else {
				outputList.add(new StreamRecord<>(outputSerializer.copy(element.getValue())));
			}
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			sideOutputSerializer = TypeExtractor.getForObject(record.getValue()).createSerializer(executionConfig);

			ConcurrentLinkedQueue<Object> sideOutputList = sideOutputLists.get(outputTag);
			if (sideOutputList == null) {
				sideOutputList = new ConcurrentLinkedQueue<>();
				sideOutputLists.put(outputTag, sideOutputList);
			}
			if (record.hasTimestamp()) {
				sideOutputList.add(new StreamRecord<>(sideOutputSerializer.copy(record.getValue()), record.getTimestamp()));
			} else {
				sideOutputList.add(new StreamRecord<>(sideOutputSerializer.copy(record.getValue())));
			}

		}

		@Override
		public void close() {
			// ignore
		}
	}
}
