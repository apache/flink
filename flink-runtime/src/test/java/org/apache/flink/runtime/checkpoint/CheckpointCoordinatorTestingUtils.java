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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway.CheckpointConsumer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Testing utils for checkpoint coordinator.
 */
public class CheckpointCoordinatorTestingUtils {

	public static OperatorStateHandle generatePartitionableStateHandle(
		JobVertexID jobVertexID,
		int index,
		int namedStates,
		int partitionsPerState,
		boolean rawState) throws IOException {

		Map<String, List<? extends Serializable>> statesListsMap = new HashMap<>(namedStates);

		for (int i = 0; i < namedStates; ++i) {
			List<Integer> testStatesLists = new ArrayList<>(partitionsPerState);
			// generate state
			int seed = jobVertexID.hashCode() * index + i * namedStates;
			if (rawState) {
				seed = (seed + 1) * 31;
			}
			Random random = new Random(seed);
			for (int j = 0; j < partitionsPerState; ++j) {
				int simulatedStateValue = random.nextInt();
				testStatesLists.add(simulatedStateValue);
			}
			statesListsMap.put("state-" + i, testStatesLists);
		}

		return generatePartitionableStateHandle(statesListsMap);
	}

	static ChainedStateHandle<OperatorStateHandle> generateChainedPartitionableStateHandle(
		JobVertexID jobVertexID,
		int index,
		int namedStates,
		int partitionsPerState,
		boolean rawState) throws IOException {

		Map<String, List<? extends Serializable>> statesListsMap = new HashMap<>(namedStates);

		for (int i = 0; i < namedStates; ++i) {
			List<Integer> testStatesLists = new ArrayList<>(partitionsPerState);
			// generate state
			int seed = jobVertexID.hashCode() * index + i * namedStates;
			if (rawState) {
				seed = (seed + 1) * 31;
			}
			Random random = new Random(seed);
			for (int j = 0; j < partitionsPerState; ++j) {
				int simulatedStateValue = random.nextInt();
				testStatesLists.add(simulatedStateValue);
			}
			statesListsMap.put("state-" + i, testStatesLists);
		}

		return ChainedStateHandle.wrapSingleHandle(generatePartitionableStateHandle(statesListsMap));
	}

	static OperatorStateHandle generatePartitionableStateHandle(
		Map<String, List<? extends Serializable>> states) throws IOException {

		List<List<? extends Serializable>> namedStateSerializables = new ArrayList<>(states.size());

		for (Map.Entry<String, List<? extends Serializable>> entry : states.entrySet()) {
			namedStateSerializables.add(entry.getValue());
		}

		Tuple2<byte[], List<long[]>> serializationWithOffsets = serializeTogetherAndTrackOffsets(namedStateSerializables);

		Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>(states.size());

		int idx = 0;
		for (Map.Entry<String, List<? extends Serializable>> entry : states.entrySet()) {
			offsetsMap.put(
				entry.getKey(),
				new OperatorStateHandle.StateMetaInfo(
					serializationWithOffsets.f1.get(idx),
					OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
			++idx;
		}

		ByteStreamStateHandle streamStateHandle = new ByteStreamStateHandle(
			String.valueOf(UUID.randomUUID()),
			serializationWithOffsets.f0);

		return new OperatorStreamStateHandle(offsetsMap, streamStateHandle);
	}

	static Tuple2<byte[], List<long[]>> serializeTogetherAndTrackOffsets(
		List<List<? extends Serializable>> serializables) throws IOException {

		List<long[]> offsets = new ArrayList<>(serializables.size());
		List<byte[]> serializedGroupValues = new ArrayList<>();

		int runningGroupsOffset = 0;
		for (List<? extends Serializable> list : serializables) {

			long[] currentOffsets = new long[list.size()];
			offsets.add(currentOffsets);

			for (int i = 0; i < list.size(); ++i) {
				currentOffsets[i] = runningGroupsOffset;
				byte[] serializedValue = InstantiationUtil.serializeObject(list.get(i));
				serializedGroupValues.add(serializedValue);
				runningGroupsOffset += serializedValue.length;
			}
		}

		//write all generated values in a single byte array, which is index by groupOffsetsInFinalByteArray
		byte[] allSerializedValuesConcatenated = new byte[runningGroupsOffset];
		runningGroupsOffset = 0;
		for (byte[] serializedGroupValue : serializedGroupValues) {
			System.arraycopy(
				serializedGroupValue,
				0,
				allSerializedValuesConcatenated,
				runningGroupsOffset,
				serializedGroupValue.length);
			runningGroupsOffset += serializedGroupValue.length;
		}
		return new Tuple2<>(allSerializedValuesConcatenated, offsets);
	}

	public static void verifyStateRestore(
		JobVertexID jobVertexID, ExecutionJobVertex executionJobVertex,
		List<KeyGroupRange> keyGroupPartitions) throws Exception {

		for (int i = 0; i < executionJobVertex.getParallelism(); i++) {

			JobManagerTaskRestore taskRestore = executionJobVertex.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskRestore();
			Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

			OperatorSubtaskState operatorState = stateSnapshot.getSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID));

			ChainedStateHandle<OperatorStateHandle> expectedOpStateBackend =
				generateChainedPartitionableStateHandle(jobVertexID, i, 2, 8, false);

			assertTrue(CommonTestUtils.isStreamContentEqual(
				expectedOpStateBackend.get(0).openInputStream(),
				operatorState.getManagedOperatorState().iterator().next().openInputStream()));

			KeyGroupsStateHandle expectPartitionedKeyGroupState = generateKeyGroupState(
				jobVertexID, keyGroupPartitions.get(i), false);
			compareKeyedState(Collections.singletonList(expectPartitionedKeyGroupState), operatorState.getManagedKeyedState());
		}
	}

	static void compareKeyedState(
		Collection<KeyGroupsStateHandle> expectPartitionedKeyGroupState,
		Collection<? extends KeyedStateHandle> actualPartitionedKeyGroupState) throws Exception {

		KeyGroupsStateHandle expectedHeadOpKeyGroupStateHandle = expectPartitionedKeyGroupState.iterator().next();
		int expectedTotalKeyGroups = expectedHeadOpKeyGroupStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
		int actualTotalKeyGroups = 0;
		for (KeyedStateHandle keyedStateHandle: actualPartitionedKeyGroupState) {
			assertTrue(keyedStateHandle instanceof KeyGroupsStateHandle);

			actualTotalKeyGroups += keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
		}

		assertEquals(expectedTotalKeyGroups, actualTotalKeyGroups);

		try (FSDataInputStream inputStream = expectedHeadOpKeyGroupStateHandle.openInputStream()) {
			for (int groupId : expectedHeadOpKeyGroupStateHandle.getKeyGroupRange()) {
				long offset = expectedHeadOpKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
				inputStream.seek(offset);
				int expectedKeyGroupState =
					InstantiationUtil.deserializeObject(inputStream, Thread.currentThread().getContextClassLoader());
				for (KeyedStateHandle oneActualKeyedStateHandle : actualPartitionedKeyGroupState) {

					assertTrue(oneActualKeyedStateHandle instanceof KeyGroupsStateHandle);

					KeyGroupsStateHandle oneActualKeyGroupStateHandle = (KeyGroupsStateHandle) oneActualKeyedStateHandle;
					if (oneActualKeyGroupStateHandle.getKeyGroupRange().contains(groupId)) {
						long actualOffset = oneActualKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
						try (FSDataInputStream actualInputStream = oneActualKeyGroupStateHandle.openInputStream()) {
							actualInputStream.seek(actualOffset);
							int actualGroupState = InstantiationUtil.
								deserializeObject(actualInputStream, Thread.currentThread().getContextClassLoader());
							assertEquals(expectedKeyGroupState, actualGroupState);
						}
					}
				}
			}
		}
	}

	static void comparePartitionableState(
		List<ChainedStateHandle<OperatorStateHandle>> expected,
		List<List<Collection<OperatorStateHandle>>> actual) throws Exception {

		List<String> expectedResult = new ArrayList<>();
		for (ChainedStateHandle<OperatorStateHandle> chainedStateHandle : expected) {
			for (int i = 0; i < chainedStateHandle.getLength(); ++i) {
				OperatorStateHandle operatorStateHandle = chainedStateHandle.get(i);
				collectResult(i, operatorStateHandle, expectedResult);
			}
		}
		Collections.sort(expectedResult);

		List<String> actualResult = new ArrayList<>();
		for (List<Collection<OperatorStateHandle>> collectionList : actual) {
			if (collectionList != null) {
				for (int i = 0; i < collectionList.size(); ++i) {
					Collection<OperatorStateHandle> stateHandles = collectionList.get(i);
					Assert.assertNotNull(stateHandles);
					for (OperatorStateHandle operatorStateHandle : stateHandles) {
						collectResult(i, operatorStateHandle, actualResult);
					}
				}
			}
		}

		Collections.sort(actualResult);
		Assert.assertEquals(expectedResult, actualResult);
	}

	static void collectResult(int opIdx, OperatorStateHandle operatorStateHandle, List<String> resultCollector) throws Exception {
		try (FSDataInputStream in = operatorStateHandle.openInputStream()) {
			for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry : operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {
				for (long offset : entry.getValue().getOffsets()) {
					in.seek(offset);
					Integer state = InstantiationUtil.
						deserializeObject(in, Thread.currentThread().getContextClassLoader());
					resultCollector.add(opIdx + " : " + entry.getKey() + " : " + state);
				}
			}
		}
	}

	static ExecutionJobVertex mockExecutionJobVertex(
		JobVertexID jobVertexID,
		int parallelism,
		int maxParallelism) throws Exception {

		return mockExecutionJobVertex(
			jobVertexID,
			Collections.singletonList(OperatorID.fromJobVertexID(jobVertexID)),
			parallelism,
			maxParallelism
		);
	}

	static ExecutionJobVertex mockExecutionJobVertex(
		JobVertexID jobVertexID,
		List<OperatorID> jobVertexIDs,
		int parallelism,
		int maxParallelism) throws Exception {
		final ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);

		ExecutionVertex[] executionVertices = new ExecutionVertex[parallelism];

		for (int i = 0; i < parallelism; i++) {
			executionVertices[i] = mockExecutionVertex(
				new ExecutionAttemptID(),
				jobVertexID,
				jobVertexIDs,
				parallelism,
				maxParallelism,
				ExecutionState.RUNNING);

			when(executionVertices[i].getParallelSubtaskIndex()).thenReturn(i);
		}

		when(executionJobVertex.getJobVertexId()).thenReturn(jobVertexID);
		when(executionJobVertex.getTaskVertices()).thenReturn(executionVertices);
		when(executionJobVertex.getParallelism()).thenReturn(parallelism);
		when(executionJobVertex.getMaxParallelism()).thenReturn(maxParallelism);
		when(executionJobVertex.isMaxParallelismConfigured()).thenReturn(true);
		when(executionJobVertex.getOperatorIDs()).thenReturn(jobVertexIDs);
		when(executionJobVertex.getUserDefinedOperatorIDs()).thenReturn(Arrays.asList(new OperatorID[jobVertexIDs.size()]));

		return executionJobVertex;
	}

	static ExecutionVertex mockExecutionVertex(ExecutionAttemptID attemptID) throws Exception {
		return mockExecutionVertex(attemptID, (LogicalSlot) null);
	}

	static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		CheckpointConsumer checkpointConsumer) throws Exception {

		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		taskManagerGateway.setCheckpointConsumer(checkpointConsumer);
		return mockExecutionVertex(attemptID, taskManagerGateway);
	}

	static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		TaskManagerGateway taskManagerGateway) throws Exception {

		TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();
		slotBuilder.setTaskManagerGateway(taskManagerGateway);
		LogicalSlot	slot = slotBuilder.createTestingLogicalSlot();
		return mockExecutionVertex(attemptID, slot);
	}

	static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		@Nullable LogicalSlot slot) throws Exception {

		JobVertexID jobVertexID = new JobVertexID();
		return mockExecutionVertex(
			attemptID,
			jobVertexID,
			Collections.singletonList(OperatorID.fromJobVertexID(jobVertexID)),
			slot,
			1,
			1,
			ExecutionState.RUNNING);
	}

	static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		JobVertexID jobVertexID,
		List<OperatorID> jobVertexIDs,
		int parallelism,
		int maxParallelism,
		ExecutionState state,
		ExecutionState ... successiveStates) throws Exception {

		return mockExecutionVertex(
			attemptID,
			jobVertexID,
			jobVertexIDs,
			null,
			parallelism,
			maxParallelism,
			state,
			successiveStates);
	}

	static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		JobVertexID jobVertexID,
		List<OperatorID> jobVertexIDs,
		@Nullable LogicalSlot slot,
		int parallelism,
		int maxParallelism,
		ExecutionState state,
		ExecutionState ... successiveStates) throws Exception {

		ExecutionVertex vertex = mock(ExecutionVertex.class);

		final Execution exec = spy(new Execution(
			mock(Executor.class),
			vertex,
			1,
			1L,
			1L,
			Time.milliseconds(500L)
		));
		if (slot != null) {
			// is there a better way to do this?
			//noinspection unchecked
			AtomicReferenceFieldUpdater<Execution, LogicalSlot> slotUpdater =
				(AtomicReferenceFieldUpdater<Execution, LogicalSlot>)
					Whitebox.getInternalState(exec, "ASSIGNED_SLOT_UPDATER");
			slotUpdater.compareAndSet(exec, null, slot);
		}

		when(exec.getAttemptId()).thenReturn(attemptID);
		when(exec.getState()).thenReturn(state, successiveStates);

		when(vertex.getJobvertexId()).thenReturn(jobVertexID);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(vertex.getMaxParallelism()).thenReturn(maxParallelism);

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getOperatorIDs()).thenReturn(jobVertexIDs);

		when(vertex.getJobVertex()).thenReturn(jobVertex);

		return vertex;
	}

	static TaskStateSnapshot mockSubtaskState(
		JobVertexID jobVertexID,
		int index,
		KeyGroupRange keyGroupRange) throws IOException {

		OperatorStateHandle partitionableState = generatePartitionableStateHandle(jobVertexID, index, 2, 8, false);
		KeyGroupsStateHandle partitionedKeyGroupState = generateKeyGroupState(jobVertexID, keyGroupRange, false);

		TaskStateSnapshot subtaskStates = spy(new TaskStateSnapshot());
		OperatorSubtaskState subtaskState = spy(new OperatorSubtaskState(
			partitionableState, null, partitionedKeyGroupState, null)
		);

		subtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID), subtaskState);

		return subtaskStates;
	}

	public static KeyGroupsStateHandle generateKeyGroupState(
		JobVertexID jobVertexID,
		KeyGroupRange keyGroupPartition, boolean rawState) throws IOException {

		List<Integer> testStatesLists = new ArrayList<>(keyGroupPartition.getNumberOfKeyGroups());

		// generate state for one keygroup
		for (int keyGroupIndex : keyGroupPartition) {
			int vertexHash = jobVertexID.hashCode();
			int seed = rawState ? (vertexHash * (31 + keyGroupIndex)) : (vertexHash + keyGroupIndex);
			Random random = new Random(seed);
			int simulatedStateValue = random.nextInt();
			testStatesLists.add(simulatedStateValue);
		}

		return generateKeyGroupState(keyGroupPartition, testStatesLists);
	}

	public static KeyGroupsStateHandle generateKeyGroupState(
		KeyGroupRange keyGroupRange,
		List<? extends Serializable> states) throws IOException {

		Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() == states.size());

		Tuple2<byte[], List<long[]>> serializedDataWithOffsets =
			serializeTogetherAndTrackOffsets(Collections.<List<? extends Serializable>>singletonList(states));

		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, serializedDataWithOffsets.f1.get(0));

		ByteStreamStateHandle allSerializedStatesHandle = new ByteStreamStateHandle(
			String.valueOf(UUID.randomUUID()),
			serializedDataWithOffsets.f0);

		return new KeyGroupsStateHandle(keyGroupRangeOffsets, allSerializedStatesHandle);
	}

	static Execution mockExecution() {
		Execution mock = mock(Execution.class);
		when(mock.getAttemptId()).thenReturn(new ExecutionAttemptID());
		when(mock.getState()).thenReturn(ExecutionState.RUNNING);
		return mock;
	}

	static Execution mockExecution(CheckpointConsumer checkpointConsumer) {
		ExecutionVertex executionVertex = mock(ExecutionVertex.class);
		final JobID jobId = new JobID();
		when(executionVertex.getJobId()).thenReturn(jobId);
		Execution mock = mock(Execution.class);
		ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
		when(mock.getAttemptId()).thenReturn(executionAttemptID);
		when(mock.getState()).thenReturn(ExecutionState.RUNNING);
		when(mock.getVertex()).thenReturn(executionVertex);
		doAnswer((InvocationOnMock invocation) -> {
			final Object[] args = invocation.getArguments();
			checkpointConsumer.accept(
				executionAttemptID,
				jobId,
				(long) args[0],
				(long) args[1],
				(CheckpointOptions) args[2],
				false);
			return null;
		}).when(mock).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));
		return mock;
	}

	static ExecutionVertex mockExecutionVertex(Execution execution, JobVertexID vertexId, int subtask, int parallelism) {
		ExecutionVertex mock = mock(ExecutionVertex.class);
		when(mock.getJobvertexId()).thenReturn(vertexId);
		when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
		when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
		when(mock.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(mock.getMaxParallelism()).thenReturn(parallelism);
		return mock;
	}

	static ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(vertices.length);
		when(vertex.getMaxParallelism()).thenReturn(vertices.length);
		when(vertex.getJobVertexId()).thenReturn(id);
		when(vertex.getTaskVertices()).thenReturn(vertices);
		when(vertex.getOperatorIDs()).thenReturn(Collections.singletonList(OperatorID.fromJobVertexID(id)));
		when(vertex.getUserDefinedOperatorIDs()).thenReturn(Collections.<OperatorID>singletonList(null));

		for (ExecutionVertex v : vertices) {
			when(v.getJobVertex()).thenReturn(vertex);
		}
		return vertex;
	}

	/**
	 * A test implementation of {@link SimpleVersionedSerializer} for String type.
	 */
	public static final class StringSerializer implements SimpleVersionedSerializer<String> {

		static final int VERSION = 77;

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public byte[] serialize(String checkpointData) throws IOException {
			return checkpointData.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public String deserialize(int version, byte[] serialized) throws IOException {
			if (version != VERSION) {
				throw new IOException("version mismatch");
			}
			return new String(serialized, StandardCharsets.UTF_8);
		}
	}
}
