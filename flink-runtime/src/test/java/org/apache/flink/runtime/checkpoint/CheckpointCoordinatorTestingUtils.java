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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphCheckpointPlanCalculatorContext;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

/** Testing utils for checkpoint coordinator. */
public class CheckpointCoordinatorTestingUtils {

    public static OperatorStateHandle generatePartitionableStateHandle(
            JobVertexID jobVertexID,
            int index,
            int namedStates,
            int partitionsPerState,
            boolean rawState)
            throws IOException {

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
            boolean rawState)
            throws IOException {

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

        return ChainedStateHandle.wrapSingleHandle(
                generatePartitionableStateHandle(statesListsMap));
    }

    static OperatorStateHandle generatePartitionableStateHandle(
            Map<String, List<? extends Serializable>> states) throws IOException {

        List<List<? extends Serializable>> namedStateSerializables = new ArrayList<>(states.size());

        for (Map.Entry<String, List<? extends Serializable>> entry : states.entrySet()) {
            namedStateSerializables.add(entry.getValue());
        }

        Tuple2<byte[], List<long[]>> serializationWithOffsets =
                serializeTogetherAndTrackOffsets(namedStateSerializables);

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

        return new OperatorStreamStateHandle(
                offsetsMap, generateByteStreamStateHandle(serializationWithOffsets.f0));
    }

    private static ByteStreamStateHandle generateByteStreamStateHandle(byte[] bytes) {
        return new ByteStreamStateHandle(String.valueOf(UUID.randomUUID()), bytes);
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

        // write all generated values in a single byte array, which is index by
        // groupOffsetsInFinalByteArray
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
            JobVertexID jobVertexID,
            ExecutionJobVertex executionJobVertex,
            List<KeyGroupRange> keyGroupPartitions)
            throws Exception {

        for (int i = 0; i < executionJobVertex.getParallelism(); i++) {

            JobManagerTaskRestore taskRestore =
                    executionJobVertex
                            .getTaskVertices()[i]
                            .getCurrentExecutionAttempt()
                            .getTaskRestore();
            Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
            TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

            OperatorSubtaskState operatorState =
                    stateSnapshot.getSubtaskStateByOperatorID(
                            OperatorID.fromJobVertexID(jobVertexID));

            ChainedStateHandle<OperatorStateHandle> expectedOpStateBackend =
                    generateChainedPartitionableStateHandle(jobVertexID, i, 2, 8, false);

            assertTrue(
                    CommonTestUtils.isStreamContentEqual(
                            expectedOpStateBackend.get(0).openInputStream(),
                            operatorState
                                    .getManagedOperatorState()
                                    .iterator()
                                    .next()
                                    .openInputStream()));

            KeyGroupsStateHandle expectPartitionedKeyGroupState =
                    generateKeyGroupState(jobVertexID, keyGroupPartitions.get(i), false);
            compareKeyedState(
                    Collections.singletonList(expectPartitionedKeyGroupState),
                    operatorState.getManagedKeyedState());
        }
    }

    static void compareKeyedState(
            Collection<KeyGroupsStateHandle> expectPartitionedKeyGroupState,
            Collection<? extends KeyedStateHandle> actualPartitionedKeyGroupState)
            throws Exception {

        KeyGroupsStateHandle expectedHeadOpKeyGroupStateHandle =
                expectPartitionedKeyGroupState.iterator().next();
        int expectedTotalKeyGroups =
                expectedHeadOpKeyGroupStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
        int actualTotalKeyGroups = 0;
        for (KeyedStateHandle keyedStateHandle : actualPartitionedKeyGroupState) {
            assertTrue(keyedStateHandle instanceof KeyGroupsStateHandle);

            actualTotalKeyGroups += keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
        }

        assertEquals(expectedTotalKeyGroups, actualTotalKeyGroups);

        try (FSDataInputStream inputStream = expectedHeadOpKeyGroupStateHandle.openInputStream()) {
            for (int groupId : expectedHeadOpKeyGroupStateHandle.getKeyGroupRange()) {
                long offset = expectedHeadOpKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
                inputStream.seek(offset);
                int expectedKeyGroupState =
                        InstantiationUtil.deserializeObject(
                                inputStream, Thread.currentThread().getContextClassLoader());
                for (KeyedStateHandle oneActualKeyedStateHandle : actualPartitionedKeyGroupState) {

                    assertTrue(oneActualKeyedStateHandle instanceof KeyGroupsStateHandle);

                    KeyGroupsStateHandle oneActualKeyGroupStateHandle =
                            (KeyGroupsStateHandle) oneActualKeyedStateHandle;
                    if (oneActualKeyGroupStateHandle.getKeyGroupRange().contains(groupId)) {
                        long actualOffset =
                                oneActualKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
                        try (FSDataInputStream actualInputStream =
                                oneActualKeyGroupStateHandle.openInputStream()) {
                            actualInputStream.seek(actualOffset);
                            int actualGroupState =
                                    InstantiationUtil.deserializeObject(
                                            actualInputStream,
                                            Thread.currentThread().getContextClassLoader());
                            assertEquals(expectedKeyGroupState, actualGroupState);
                        }
                    }
                }
            }
        }
    }

    static void comparePartitionableState(
            List<ChainedStateHandle<OperatorStateHandle>> expected,
            List<List<Collection<OperatorStateHandle>>> actual)
            throws Exception {

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

    static void collectResult(
            int opIdx, OperatorStateHandle operatorStateHandle, List<String> resultCollector)
            throws Exception {
        try (FSDataInputStream in = operatorStateHandle.openInputStream()) {
            for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry :
                    operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {
                for (long offset : entry.getValue().getOffsets()) {
                    in.seek(offset);
                    Integer state =
                            InstantiationUtil.deserializeObject(
                                    in, Thread.currentThread().getContextClassLoader());
                    resultCollector.add(opIdx + " : " + entry.getKey() + " : " + state);
                }
            }
        }
    }

    static TaskStateSnapshot mockSubtaskState(
            JobVertexID jobVertexID, int index, KeyGroupRange keyGroupRange) throws IOException {

        OperatorStateHandle partitionableState =
                generatePartitionableStateHandle(jobVertexID, index, 2, 8, false);
        KeyGroupsStateHandle partitionedKeyGroupState =
                generateKeyGroupState(jobVertexID, keyGroupRange, false);

        TaskStateSnapshot subtaskStates = spy(new TaskStateSnapshot());
        OperatorSubtaskState subtaskState =
                spy(
                        OperatorSubtaskState.builder()
                                .setManagedOperatorState(partitionableState)
                                .setManagedKeyedState(partitionedKeyGroupState)
                                .build());

        subtaskStates.putSubtaskStateByOperatorID(
                OperatorID.fromJobVertexID(jobVertexID), subtaskState);

        return subtaskStates;
    }

    public static KeyGroupsStateHandle generateKeyGroupState(
            JobVertexID jobVertexID, KeyGroupRange keyGroupPartition, boolean rawState)
            throws IOException {

        List<Integer> testStatesLists = new ArrayList<>(keyGroupPartition.getNumberOfKeyGroups());

        // generate state for one keygroup
        for (int keyGroupIndex : keyGroupPartition) {
            int vertexHash = jobVertexID.hashCode();
            int seed =
                    rawState ? (vertexHash * (31 + keyGroupIndex)) : (vertexHash + keyGroupIndex);
            Random random = new Random(seed);
            int simulatedStateValue = random.nextInt();
            testStatesLists.add(simulatedStateValue);
        }

        return generateKeyGroupState(keyGroupPartition, testStatesLists);
    }

    public static KeyGroupsStateHandle generateKeyGroupState(
            KeyGroupRange keyGroupRange, List<? extends Serializable> states) throws IOException {

        Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() == states.size());

        Tuple2<byte[], List<long[]>> serializedDataWithOffsets =
                serializeTogetherAndTrackOffsets(
                        Collections.<List<? extends Serializable>>singletonList(states));

        KeyGroupRangeOffsets keyGroupRangeOffsets =
                new KeyGroupRangeOffsets(keyGroupRange, serializedDataWithOffsets.f1.get(0));

        ByteStreamStateHandle allSerializedStatesHandle =
                generateByteStreamStateHandle(serializedDataWithOffsets.f0);

        return new KeyGroupsStateHandle(keyGroupRangeOffsets, allSerializedStatesHandle);
    }

    static class TriggeredCheckpoint {
        final JobID jobId;
        final long checkpointId;
        final long timestamp;
        final CheckpointOptions checkpointOptions;

        public TriggeredCheckpoint(
                JobID jobId,
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions) {
            this.jobId = jobId;
            this.checkpointId = checkpointId;
            this.timestamp = timestamp;
            this.checkpointOptions = checkpointOptions;
        }
    }

    static class NotifiedCheckpoint {
        final JobID jobId;
        final long checkpointId;
        final long timestamp;

        public NotifiedCheckpoint(JobID jobId, long checkpointId, long timestamp) {
            this.jobId = jobId;
            this.checkpointId = checkpointId;
            this.timestamp = timestamp;
        }
    }

    static class CheckpointRecorderTaskManagerGateway extends SimpleAckingTaskManagerGateway {

        private final Map<ExecutionAttemptID, List<TriggeredCheckpoint>> triggeredCheckpoints =
                new HashMap<>();

        private final Map<ExecutionAttemptID, List<NotifiedCheckpoint>>
                notifiedCompletedCheckpoints = new HashMap<>();

        private final Map<ExecutionAttemptID, List<NotifiedCheckpoint>> notifiedAbortCheckpoints =
                new HashMap<>();

        @Override
        public void triggerCheckpoint(
                ExecutionAttemptID attemptId,
                JobID jobId,
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions) {
            triggeredCheckpoints
                    .computeIfAbsent(attemptId, k -> new ArrayList<>())
                    .add(
                            new TriggeredCheckpoint(
                                    jobId, checkpointId, timestamp, checkpointOptions));
        }

        @Override
        public void notifyCheckpointComplete(
                ExecutionAttemptID attemptId, JobID jobId, long checkpointId, long timestamp) {
            notifiedCompletedCheckpoints
                    .computeIfAbsent(attemptId, k -> new ArrayList<>())
                    .add(new NotifiedCheckpoint(jobId, checkpointId, timestamp));
        }

        @Override
        public void notifyCheckpointAborted(
                ExecutionAttemptID attemptId, JobID jobId, long checkpointId, long timestamp) {
            notifiedAbortCheckpoints
                    .computeIfAbsent(attemptId, k -> new ArrayList<>())
                    .add(new NotifiedCheckpoint(jobId, checkpointId, timestamp));
        }

        public void resetCount() {
            triggeredCheckpoints.clear();
            notifiedAbortCheckpoints.clear();
            notifiedCompletedCheckpoints.clear();
        }

        public List<TriggeredCheckpoint> getTriggeredCheckpoints(ExecutionAttemptID attemptId) {
            return triggeredCheckpoints.getOrDefault(attemptId, Collections.emptyList());
        }

        public TriggeredCheckpoint getOnlyTriggeredCheckpoint(ExecutionAttemptID attemptId) {
            List<TriggeredCheckpoint> triggeredCheckpoints = getTriggeredCheckpoints(attemptId);
            assertEquals(
                    "There should be exactly one checkpoint triggered for " + attemptId,
                    1,
                    triggeredCheckpoints.size());
            return triggeredCheckpoints.get(0);
        }

        public List<NotifiedCheckpoint> getNotifiedCompletedCheckpoints(
                ExecutionAttemptID attemptId) {
            return notifiedCompletedCheckpoints.getOrDefault(attemptId, Collections.emptyList());
        }

        public NotifiedCheckpoint getOnlyNotifiedCompletedCheckpoint(ExecutionAttemptID attemptId) {
            List<NotifiedCheckpoint> completedCheckpoints =
                    getNotifiedCompletedCheckpoints(attemptId);
            assertEquals(
                    "There should be exactly one checkpoint notified completed for " + attemptId,
                    1,
                    completedCheckpoints.size());
            return completedCheckpoints.get(0);
        }

        public List<NotifiedCheckpoint> getNotifiedAbortedCheckpoints(
                ExecutionAttemptID attemptId) {
            return notifiedAbortCheckpoints.getOrDefault(attemptId, Collections.emptyList());
        }

        public NotifiedCheckpoint getOnlyNotifiedAbortedCheckpoint(ExecutionAttemptID attemptId) {
            List<NotifiedCheckpoint> abortedCheckpoints = getNotifiedAbortedCheckpoints(attemptId);
            assertEquals(
                    "There should be exactly one checkpoint notified aborted for " + attemptId,
                    1,
                    abortedCheckpoints.size());
            return abortedCheckpoints.get(0);
        }
    }

    static class CheckpointExecutionGraphBuilder {
        private final List<JobVertex> sourceVertices = new ArrayList<>();
        private final List<JobVertex> nonSourceVertices = new ArrayList<>();
        private boolean transitToRunning;
        private TaskManagerGateway taskManagerGateway;
        private ComponentMainThreadExecutor mainThreadExecutor;

        CheckpointExecutionGraphBuilder() {
            this.transitToRunning = true;
            this.mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
        }

        public CheckpointExecutionGraphBuilder addJobVertex(JobVertexID id) {
            return addJobVertex(id, true);
        }

        public CheckpointExecutionGraphBuilder addJobVertex(JobVertexID id, boolean isSource) {
            return addJobVertex(id, 1, 32768, Collections.emptyList(), isSource);
        }

        public CheckpointExecutionGraphBuilder addJobVertex(
                JobVertexID id, int parallelism, int maxParallelism) {
            return addJobVertex(id, parallelism, maxParallelism, Collections.emptyList(), true);
        }

        public CheckpointExecutionGraphBuilder addJobVertex(
                JobVertexID id,
                int parallelism,
                int maxParallelism,
                List<OperatorIDPair> operators,
                boolean isSource) {

            JobVertex jobVertex =
                    operators.size() == 0
                            ? new JobVertex("anon", id)
                            : new JobVertex("anon", id, operators);
            jobVertex.setParallelism(parallelism);
            jobVertex.setMaxParallelism(maxParallelism);
            jobVertex.setInvokableClass(NoOpInvokable.class);

            return addJobVertex(jobVertex, isSource);
        }

        public CheckpointExecutionGraphBuilder addJobVertex(JobVertex jobVertex, boolean isSource) {
            if (isSource) {
                sourceVertices.add(jobVertex);
            } else {
                nonSourceVertices.add(jobVertex);
            }

            return this;
        }

        public CheckpointExecutionGraphBuilder setTaskManagerGateway(
                TaskManagerGateway taskManagerGateway) {
            this.taskManagerGateway = taskManagerGateway;
            return this;
        }

        public CheckpointExecutionGraphBuilder setTransitToRunning(boolean transitToRunning) {
            this.transitToRunning = transitToRunning;
            return this;
        }

        public CheckpointExecutionGraphBuilder setMainThreadExecutor(
                ComponentMainThreadExecutor mainThreadExecutor) {
            this.mainThreadExecutor = mainThreadExecutor;
            return this;
        }

        ExecutionGraph build() throws Exception {
            // Lets connect source vertices and non-source vertices
            for (JobVertex source : sourceVertices) {
                for (JobVertex nonSource : nonSourceVertices) {
                    nonSource.connectNewDataSetAsInput(
                            source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
                }
            }

            List<JobVertex> allVertices = new ArrayList<>();
            allVertices.addAll(sourceVertices);
            allVertices.addAll(nonSourceVertices);

            ExecutionGraph executionGraph =
                    ExecutionGraphTestUtils.createSimpleTestGraph(
                            allVertices.toArray(new JobVertex[0]));
            executionGraph.start(mainThreadExecutor);

            if (taskManagerGateway != null) {
                executionGraph
                        .getAllExecutionVertices()
                        .forEach(
                                task -> {
                                    LogicalSlot slot =
                                            new TestingLogicalSlotBuilder()
                                                    .setTaskManagerGateway(taskManagerGateway)
                                                    .createTestingLogicalSlot();
                                    task.tryAssignResource(slot);
                                });
            }

            if (transitToRunning) {
                executionGraph.transitionToRunning();
                executionGraph
                        .getAllExecutionVertices()
                        .forEach(
                                task ->
                                        task.getCurrentExecutionAttempt()
                                                .transitionState(ExecutionState.RUNNING));
            }

            return executionGraph;
        }
    }

    /** A helper builder for {@link CheckpointCoordinator} to deduplicate test codes. */
    public static class CheckpointCoordinatorBuilder {
        private CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        private ExecutionGraph executionGraph;

        private Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint =
                Collections.emptyList();

        private CheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();

        private CompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);

        private CheckpointStorage checkpointStorage = new MemoryStateBackend();

        private Executor ioExecutor = Executors.directExecutor();

        private CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();

        private ScheduledExecutor timer = new ManuallyTriggeredScheduledExecutor();

        private SharedStateRegistryFactory sharedStateRegistryFactory =
                SharedStateRegistry.DEFAULT_FACTORY;

        private CheckpointFailureManager failureManager =
                new CheckpointFailureManager(0, NoOpFailJobCall.INSTANCE);

        private boolean allowCheckpointsAfterTasksFinished;

        public CheckpointCoordinatorBuilder setCheckpointCoordinatorConfiguration(
                CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration) {
            this.checkpointCoordinatorConfiguration = checkpointCoordinatorConfiguration;
            return this;
        }

        public CheckpointCoordinatorBuilder setExecutionGraph(ExecutionGraph executionGraph) {
            this.executionGraph = executionGraph;
            return this;
        }

        public CheckpointCoordinatorBuilder setCoordinatorsToCheckpoint(
                Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint) {
            this.coordinatorsToCheckpoint = coordinatorsToCheckpoint;
            return this;
        }

        public CheckpointCoordinatorBuilder setCheckpointsCleaner(
                CheckpointsCleaner checkpointsCleaner) {
            this.checkpointsCleaner = checkpointsCleaner;
            return this;
        }

        public CheckpointCoordinatorBuilder setCheckpointIDCounter(
                CheckpointIDCounter checkpointIDCounter) {
            this.checkpointIDCounter = checkpointIDCounter;
            return this;
        }

        public CheckpointCoordinatorBuilder setCheckpointFailureManager(
                CheckpointFailureManager checkpointFailureManager) {
            this.failureManager = checkpointFailureManager;
            return this;
        }

        public CheckpointCoordinatorBuilder setCompletedCheckpointStore(
                CompletedCheckpointStore completedCheckpointStore) {
            this.completedCheckpointStore = completedCheckpointStore;
            return this;
        }

        public CheckpointCoordinatorBuilder setIoExecutor(Executor ioExecutor) {
            this.ioExecutor = ioExecutor;
            return this;
        }

        public CheckpointCoordinatorBuilder setTimer(ScheduledExecutor timer) {
            this.timer = timer;
            return this;
        }

        public CheckpointCoordinatorBuilder setSharedStateRegistryFactory(
                SharedStateRegistryFactory sharedStateRegistryFactory) {
            this.sharedStateRegistryFactory = sharedStateRegistryFactory;
            return this;
        }

        public CheckpointCoordinatorBuilder setFailureManager(
                CheckpointFailureManager failureManager) {
            this.failureManager = failureManager;
            return this;
        }

        public CheckpointCoordinatorBuilder setCheckpointStorage(CheckpointStorage stateBackEnd) {
            this.checkpointStorage = stateBackEnd;
            return this;
        }

        public CheckpointCoordinatorBuilder setAllowCheckpointsAfterTasksFinished(
                boolean allowCheckpointsAfterTasksFinished) {
            this.allowCheckpointsAfterTasksFinished = allowCheckpointsAfterTasksFinished;
            return this;
        }

        public CheckpointCoordinator build() throws Exception {
            if (executionGraph == null) {
                executionGraph =
                        new CheckpointExecutionGraphBuilder()
                                .addJobVertex(new JobVertexID())
                                .build();
            }

            DefaultCheckpointPlanCalculator checkpointPlanCalculator =
                    new DefaultCheckpointPlanCalculator(
                            executionGraph.getJobID(),
                            new ExecutionGraphCheckpointPlanCalculatorContext(executionGraph),
                            executionGraph.getVerticesTopologically());
            checkpointPlanCalculator.setAllowCheckpointsAfterTasksFinished(
                    allowCheckpointsAfterTasksFinished);

            return new CheckpointCoordinator(
                    executionGraph.getJobID(),
                    checkpointCoordinatorConfiguration,
                    coordinatorsToCheckpoint,
                    checkpointIDCounter,
                    completedCheckpointStore,
                    checkpointStorage,
                    ioExecutor,
                    checkpointsCleaner,
                    timer,
                    sharedStateRegistryFactory,
                    failureManager,
                    checkpointPlanCalculator,
                    new ExecutionAttemptMappingProvider(executionGraph.getAllExecutionVertices()));
        }
    }

    /** A test implementation of {@link SimpleVersionedSerializer} for String type. */
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

    // ----------------- Mock class builders ---------------

    public static final class MockOperatorCheckpointCoordinatorContextBuilder {
        private BiConsumer<Long, CompletableFuture<byte[]>> onCallingCheckpointCoordinator = null;
        private Consumer<Long> onCallingAfterSourceBarrierInjection = null;
        private OperatorID operatorID = null;

        public MockOperatorCheckpointCoordinatorContextBuilder setOnCallingCheckpointCoordinator(
                BiConsumer<Long, CompletableFuture<byte[]>> onCallingCheckpointCoordinator) {
            this.onCallingCheckpointCoordinator = onCallingCheckpointCoordinator;
            return this;
        }

        public MockOperatorCheckpointCoordinatorContextBuilder
                setOnCallingAfterSourceBarrierInjection(
                        Consumer<Long> onCallingAfterSourceBarrierInjection) {
            this.onCallingAfterSourceBarrierInjection = onCallingAfterSourceBarrierInjection;
            return this;
        }

        public MockOperatorCheckpointCoordinatorContextBuilder setOperatorID(
                OperatorID operatorID) {
            this.operatorID = operatorID;
            return this;
        }

        public MockOperatorCoordinatorCheckpointContext build() {
            return new MockOperatorCoordinatorCheckpointContext(
                    onCallingCheckpointCoordinator,
                    onCallingAfterSourceBarrierInjection,
                    operatorID);
        }
    }

    // ----------------- Mock classes --------------------

    /**
     * The class works together with {@link MockOperatorCheckpointCoordinatorContextBuilder} to
     * construct a mock OperatorCoordinatorCheckpointContext.
     */
    public static final class MockOperatorCoordinatorCheckpointContext
            implements OperatorCoordinatorCheckpointContext {
        private final BiConsumer<Long, CompletableFuture<byte[]>> onCallingCheckpointCoordinator;
        private final Consumer<Long> onCallingAfterSourceBarrierInjection;
        private final OperatorID operatorID;
        private final List<Long> completedCheckpoints;
        private final List<Long> abortedCheckpoints;

        private MockOperatorCoordinatorCheckpointContext(
                BiConsumer<Long, CompletableFuture<byte[]>> onCallingCheckpointCoordinator,
                Consumer<Long> onCallingAfterSourceBarrierInjection,
                OperatorID operatorID) {
            this.onCallingCheckpointCoordinator = onCallingCheckpointCoordinator;
            this.onCallingAfterSourceBarrierInjection = onCallingAfterSourceBarrierInjection;
            this.operatorID = operatorID;
            this.completedCheckpoints = new ArrayList<>();
            this.abortedCheckpoints = new ArrayList<>();
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
                throws Exception {
            if (onCallingCheckpointCoordinator != null) {
                onCallingCheckpointCoordinator.accept(checkpointId, result);
            }
        }

        @Override
        public void afterSourceBarrierInjection(long checkpointId) {
            if (onCallingAfterSourceBarrierInjection != null) {
                onCallingAfterSourceBarrierInjection.accept(checkpointId);
            }
        }

        @Override
        public void abortCurrentTriggering() {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            completedCheckpoints.add(checkpointId);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            abortedCheckpoints.add(checkpointId);
        }

        @Override
        public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
                throws Exception {}

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public OperatorID operatorId() {
            return operatorID;
        }

        @Override
        public int maxParallelism() {
            return 1;
        }

        @Override
        public int currentParallelism() {
            return 1;
        }

        public List<Long> getCompletedCheckpoints() {
            return completedCheckpoints;
        }

        public List<Long> getAbortedCheckpoints() {
            return abortedCheckpoints;
        }
    }
}
