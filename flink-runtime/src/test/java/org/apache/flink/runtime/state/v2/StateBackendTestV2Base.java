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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.asyncprocessing.AsyncFutureImpl;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateExecutionController;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.RunnableFuture;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link AsyncKeyedStateBackend} produced by various {@link StateBackend}s.
 *
 * <p>NOTE: Please ensure to close and dispose any created keyed state backend in tests.
 */
@SuppressWarnings("serial")
public abstract class StateBackendTestV2Base<B extends AbstractStateBackend> {

    protected MockEnvironment env;
    protected JobID jobID;

    // lazily initialized stream storage
    private CheckpointStreamFactory checkpointStreamFactory;

    @BeforeEach
    void before() throws Exception {
        jobID = new JobID();
        env = buildMockEnv();
    }

    protected MockEnvironment buildMockEnv() throws Exception {
        MockEnvironment mockEnvironment =
                MockEnvironment.builder()
                        .setTaskStateManager(getTestTaskStateManager())
                        .setJobID(jobID)
                        .build();
        mockEnvironment.setCheckpointStorageAccess(getCheckpointStorageAccess());
        return mockEnvironment;
    }

    protected TestTaskStateManager getTestTaskStateManager() throws IOException {
        return TestTaskStateManager.builder().build();
    }

    @AfterEach
    void after() {
        IOUtils.closeQuietly(env);
    }

    protected abstract ConfigurableStateBackend getStateBackend() throws Exception;

    protected abstract void restoreJob() throws Exception;

    protected CheckpointStorage getCheckpointStorage() throws Exception {
        ConfigurableStateBackend stateBackend = getStateBackend();
        if (stateBackend instanceof CheckpointStorage) {
            return (CheckpointStorage) stateBackend;
        }

        throw new IllegalStateException(
                "The state backend under test does not implement CheckpointStorage."
                        + "Please override 'createCheckpointStorage' and provide an appropriate"
                        + "checkpoint storage instance");
    }

    protected CheckpointStorageAccess getCheckpointStorageAccess() throws Exception {
        return getCheckpointStorage().createCheckpointStorage(jobID);
    }

    protected CheckpointStreamFactory createStreamFactory() throws Exception {
        if (checkpointStreamFactory == null) {
            checkpointStreamFactory =
                    getCheckpointStorage()
                            .createCheckpointStorage(jobID)
                            .resolveCheckpointStorageLocation(
                                    1L, CheckpointStorageLocationReference.getDefault());
        }

        return checkpointStreamFactory;
    }

    protected <K> AsyncKeyedStateBackend<K> createAsyncKeyedBackend(
            int subtaskId,
            int parallelism,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        env.setCheckpointStorageAccess(getCheckpointStorageAccess());

        return getStateBackend()
                .createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                jobID,
                                String.format("test_op_%d_%d", subtaskId, parallelism),
                                keySerializer,
                                keyGroupRange.getNumberOfKeyGroups(),
                                keyGroupRange,
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                getMetricGroup(),
                                getCustomInitializationMetrics(),
                                Collections.emptyList(),
                                new CloseableRegistry(),
                                1.0d));
    }

    protected StateBackend.CustomInitializationMetrics getCustomInitializationMetrics() {
        return (name, value) -> {};
    }

    protected <K> AsyncKeyedStateBackend<K> restoreAsyncKeyedBackend(
            int subtaskId,
            int parallelism,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            List<KeyedStateHandle> state,
            Environment env)
            throws Exception {

        return getStateBackend()
                .createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                jobID,
                                String.format("test_op_%d_%d", subtaskId, parallelism),
                                keySerializer,
                                keyGroupRange.getNumberOfKeyGroups(),
                                keyGroupRange,
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                getMetricGroup(),
                                getCustomInitializationMetrics(),
                                state,
                                new CloseableRegistry(),
                                1.0d));
    }

    protected MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
    }

    @TestTemplate
    void testAsyncKeyedStateBackendSnapshot() throws Exception {
        int mockRecordCount = 20;

        int jobMaxParallelism = 128;
        int aecBatchSize = 1;
        long aecBufferTimeout = 1;
        int aecMaxInFlightRecords = 1000;

        ArrayList<RecordContext<Integer>> recordContexts = new ArrayList<>(mockRecordCount);

        AsyncKeyedStateBackend<Integer> backend = null;
        StateExecutionController<Integer> aec;

        TestAsyncFrameworkExceptionHandler testExceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        KeyedStateHandle stateHandle;

        try {
            backend =
                    createAsyncKeyedBackend(
                            0,
                            1,
                            IntSerializer.INSTANCE,
                            new KeyGroupRange(0, jobMaxParallelism - 1),
                            env);
            aec =
                    new StateExecutionController<>(
                            new SyncMailboxExecutor(),
                            testExceptionHandler,
                            backend.createStateExecutor(),
                            new DeclarationManager(),
                            EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                            jobMaxParallelism,
                            aecBatchSize,
                            aecBufferTimeout,
                            aecMaxInFlightRecords,
                            null,
                            null);
            backend.setup(aec);

            ValueStateDescriptor<Integer> stateDescriptor =
                    new ValueStateDescriptor<>("test", BasicTypeInfo.INT_TYPE_INFO);

            ValueState<Integer> valueState =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            stateDescriptor);

            for (int i = 0; i < mockRecordCount; i++) {
                recordContexts.add(aec.buildContext(i, i));
                recordContexts.get(i).retain();
            }

            for (int i = 0; i < mockRecordCount; i++) {
                aec.setCurrentContext(recordContexts.get(i));
                valueState.update(i);
            }

            // verify state value before snapshot and release record contexts
            for (int i = 0; i < mockRecordCount; i++) {
                aec.setCurrentContext(recordContexts.get(i));
                assertThat(valueState.value()).isEqualTo(i);
                recordContexts.get(i).release();
            }

            aec.drainInflightRecords(0);

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    backend.snapshot(
                            1L,
                            System.currentTimeMillis(),
                            createStreamFactory(),
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            if (!snapshot.isDone()) {
                snapshot.run();
            }
            SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
            stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

            recordContexts.clear();
            for (int i = 0; i < mockRecordCount; i++) {
                recordContexts.add(aec.buildContext(i, i));
                recordContexts.get(i).retain();
            }

            for (int i = 0; i < mockRecordCount; i++) {
                aec.setCurrentContext(recordContexts.get(i));
                valueState.update(i + 1);
            }

            // verify state value after snapshot and release record contexts
            for (int i = 0; i < mockRecordCount; i++) {
                aec.setCurrentContext(recordContexts.get(i));
                assertThat(valueState.value()).isEqualTo(i + 1);
                recordContexts.get(i).release();
            }

        } finally {
            if (null != backend) {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }

        assertThat(stateHandle).isNotNull();

        backend = null;
        restoreJob();

        try {
            backend =
                    restoreAsyncKeyedBackend(
                            0,
                            1,
                            IntSerializer.INSTANCE,
                            new KeyGroupRange(0, jobMaxParallelism - 1),
                            Collections.singletonList(stateHandle),
                            env);
            aec =
                    new StateExecutionController<>(
                            new SyncMailboxExecutor(),
                            testExceptionHandler,
                            backend.createStateExecutor(),
                            new DeclarationManager(),
                            EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                            jobMaxParallelism,
                            aecBatchSize,
                            aecBufferTimeout,
                            aecMaxInFlightRecords,
                            null,
                            null);
            backend.setup(aec);

            ValueStateDescriptor<Integer> stateDescriptor =
                    new ValueStateDescriptor<>("test", BasicTypeInfo.INT_TYPE_INFO);

            ValueState<Integer> valueState =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            stateDescriptor);

            recordContexts.clear();
            for (int i = 0; i < mockRecordCount; i++) {
                recordContexts.add(aec.buildContext(i, i));
                recordContexts.get(i).retain();
            }

            // verify state value after restore from snapshot and release record contexts
            for (int i = 0; i < mockRecordCount; i++) {
                aec.setCurrentContext(recordContexts.get(i));
                assertThat(valueState.value()).isEqualTo(i);
                recordContexts.get(i).release();
            }
        } finally {
            if (null != backend) {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }

        assertThat(testExceptionHandler.exception).isNull();
    }

    @TestTemplate
    void testAsyncStateBackendScaleUp() throws Exception {
        testKeyGroupSnapshotRestore(2, 5, 10);
    }

    @TestTemplate
    void testAsyncStateBackendScaleDown() throws Exception {
        testKeyGroupSnapshotRestore(4, 3, 10);
    }

    private void testKeyGroupSnapshotRestore(
            int sourceParallelism, int targetParallelism, int maxParallelism) throws Exception {

        int aecBatchSize = 1;
        long aecBufferTimeout = 1;
        int aecMaxInFlightRecords = 1000;
        Random random = new Random();
        List<ValueStateDescriptor<Integer>> stateDescriptors = new ArrayList<>(maxParallelism);
        List<Integer> keyInKeyGroups = new ArrayList<>(maxParallelism);
        List<Integer> expectedValue = new ArrayList<>(maxParallelism);
        for (int i = 0; i < maxParallelism; ++i) {
            // all states have different name to mock that all the parallelisms of one operator have
            // different states.
            stateDescriptors.add(
                    new ValueStateDescriptor<>("state" + i, BasicTypeInfo.INT_TYPE_INFO));
        }

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        StateExecutionController<Integer> aec;

        TestAsyncFrameworkExceptionHandler testExceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        List<KeyedStateHandle> snapshots = new ArrayList<>(sourceParallelism);
        for (int i = 0; i < sourceParallelism; ++i) {
            KeyGroupRange range =
                    KeyGroupRange.of(
                            maxParallelism * i / sourceParallelism,
                            maxParallelism * (i + 1) / sourceParallelism - 1);
            AsyncKeyedStateBackend<Integer> backend =
                    createAsyncKeyedBackend(
                            i, sourceParallelism, IntSerializer.INSTANCE, range, env);
            aec =
                    new StateExecutionController<>(
                            new SyncMailboxExecutor(),
                            testExceptionHandler,
                            backend.createStateExecutor(),
                            new DeclarationManager(),
                            EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                            maxParallelism,
                            aecBatchSize,
                            aecBufferTimeout,
                            aecMaxInFlightRecords,
                            null,
                            null);
            backend.setup(aec);

            try {
                for (int j = range.getStartKeyGroup(); j <= range.getEndKeyGroup(); ++j) {
                    ValueState<Integer> state =
                            backend.getOrCreateKeyedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    stateDescriptors.get(j));
                    int keyInKeyGroup =
                            getKeyInKeyGroup(random, maxParallelism, KeyGroupRange.of(j, j));
                    RecordContext recordContext = aec.buildContext(keyInKeyGroup, keyInKeyGroup);
                    recordContext.retain();
                    aec.setCurrentContext(recordContext);
                    keyInKeyGroups.add(keyInKeyGroup);
                    state.update(keyInKeyGroup);
                    expectedValue.add(keyInKeyGroup);
                    recordContext.release();
                }

                // snapshot
                snapshots.add(
                        runSnapshot(
                                backend.snapshot(
                                        0,
                                        0,
                                        streamFactory,
                                        CheckpointOptions.forCheckpointWithDefaultLocation()),
                                sharedStateRegistry));
            } finally {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }

        // redistribute the stateHandle
        List<KeyGroupRange> keyGroupRangesRestore = new ArrayList<>();
        for (int i = 0; i < targetParallelism; ++i) {
            keyGroupRangesRestore.add(
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            maxParallelism, targetParallelism, i));
        }
        List<List<KeyedStateHandle>> keyGroupStatesAfterDistribute =
                new ArrayList<>(targetParallelism);
        for (int i = 0; i < targetParallelism; ++i) {
            List<KeyedStateHandle> keyedStateHandles = new ArrayList<>();
            StateAssignmentOperation.extractIntersectingState(
                    snapshots, keyGroupRangesRestore.get(i), keyedStateHandles);
            keyGroupStatesAfterDistribute.add(keyedStateHandles);
        }
        // restore and verify
        for (int i = 0; i < targetParallelism; ++i) {
            AsyncKeyedStateBackend<Integer> backend =
                    restoreAsyncKeyedBackend(
                            i,
                            targetParallelism,
                            IntSerializer.INSTANCE,
                            keyGroupRangesRestore.get(i),
                            keyGroupStatesAfterDistribute.get(i),
                            env);
            aec =
                    new StateExecutionController<>(
                            new SyncMailboxExecutor(),
                            testExceptionHandler,
                            backend.createStateExecutor(),
                            new DeclarationManager(),
                            EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                            maxParallelism,
                            aecBatchSize,
                            aecBufferTimeout,
                            aecMaxInFlightRecords,
                            null,
                            null);
            backend.setup(aec);

            try {
                KeyGroupRange range = keyGroupRangesRestore.get(i);
                for (int j = range.getStartKeyGroup(); j <= range.getEndKeyGroup(); ++j) {
                    ValueState<Integer> state =
                            backend.getOrCreateKeyedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    stateDescriptors.get(j));
                    RecordContext recordContext =
                            aec.buildContext(keyInKeyGroups.get(j), keyInKeyGroups.get(j));
                    recordContext.retain();
                    aec.setCurrentContext(recordContext);
                    assertThat(state.value()).isEqualTo(expectedValue.get(j));
                    recordContext.release();
                }
            } finally {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }
    }

    @TestTemplate
    void testKeyGroupedInternalPriorityQueue() throws Exception {
        testKeyGroupedInternalPriorityQueue(false);
    }

    @TestTemplate
    void testKeyGroupedInternalPriorityQueueAddAll() throws Exception {
        testKeyGroupedInternalPriorityQueue(true);
    }

    void testKeyGroupedInternalPriorityQueue(boolean addAll) throws Exception {
        String fieldName = "key-grouped-priority-queue";
        AsyncKeyedStateBackend<Integer> backend =
                createAsyncKeyedBackend(
                        0, 1, IntSerializer.INSTANCE, new KeyGroupRange(0, 127), env);
        try {
            KeyGroupedInternalPriorityQueue<TestType> priorityQueue =
                    backend.create(fieldName, new TestType.V1TestTypeSerializer());

            TestType elementA42 = new TestType("a", 42);
            TestType elementA44 = new TestType("a", 44);
            TestType elementB1 = new TestType("b", 1);
            TestType elementB3 = new TestType("b", 3);

            TestType[] elements = {
                elementA44, elementB1, elementB1, elementB3, elementA42,
            };

            if (addAll) {
                priorityQueue.addAll(asList(elements));
            } else {
                assertThat(priorityQueue.add(elements[0])).isTrue();
                assertThat(priorityQueue.add(elements[1])).isTrue();
                assertThat(priorityQueue.add(elements[2])).isFalse();
                assertThat(priorityQueue.add(elements[3])).isFalse();
                assertThat(priorityQueue.add(elements[4])).isFalse();
            }
            assertThat(priorityQueue.isEmpty()).isFalse();
            assertThat(priorityQueue.getSubsetForKeyGroup(81))
                    .containsExactlyInAnyOrder(elementA42, elementA44);
            assertThat(priorityQueue.getSubsetForKeyGroup(22))
                    .containsExactlyInAnyOrder(elementB1, elementB3);

            assertThat(priorityQueue.peek()).isEqualTo(elementB1);
            assertThat(priorityQueue.poll()).isEqualTo(elementB1);
            assertThat(priorityQueue.peek()).isEqualTo(elementB3);

            List<TestType> actualList = new ArrayList<>();
            try (CloseableIterator<TestType> iterator = priorityQueue.iterator()) {
                iterator.forEachRemaining(actualList::add);
            }

            assertThat(actualList).containsExactlyInAnyOrder(elementB3, elementA42, elementA44);

            assertThat(priorityQueue.size()).isEqualTo(3);

            assertThat(priorityQueue.remove(elementB1)).isFalse();
            assertThat(priorityQueue.remove(elementB3)).isTrue();
            assertThat(priorityQueue.peek()).isEqualTo(elementA42);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testValueStateWorkWithTtl() throws Exception {
        TestAsyncFrameworkExceptionHandler testExceptionHandler =
                new TestAsyncFrameworkExceptionHandler();
        AsyncKeyedStateBackend<Long> backend =
                createAsyncKeyedBackend(
                        0, 1, LongSerializer.INSTANCE, new KeyGroupRange(0, 127), env);
        StateExecutionController<Long> aec =
                new StateExecutionController<>(
                        new SyncMailboxExecutor(),
                        testExceptionHandler,
                        backend.createStateExecutor(),
                        new DeclarationManager(),
                        EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                        128,
                        1,
                        -1,
                        1,
                        null,
                        null);
        backend.setup(aec);
        try {
            ValueStateDescriptor<Long> kvId =
                    new ValueStateDescriptor<>("id", TypeInformation.of(Long.class));
            kvId.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofSeconds(1)).build());

            ValueState<Long> state =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            RecordContext recordContext = aec.buildContext("record-1", 1L);
            recordContext.retain();
            aec.setCurrentContext(recordContext);
            state.update(1L);
            assertThat(state.value()).isEqualTo(1L);
            Thread.sleep(1000);
            assertThat(state.value()).isNull();
            recordContext.release();

            RecordContext recordContext1 = aec.buildContext("record-2", 2L);
            aec.setCurrentContext(recordContext1);
            state.asyncUpdate(2L)
                    .thenAccept(
                            (val) -> {
                                state.asyncValue()
                                        .thenAccept(
                                                (val1) -> {
                                                    assertThat(val1).isEqualTo(2);
                                                    Thread.sleep(1000);
                                                    state.asyncValue()
                                                            .thenAccept(
                                                                    (val2) -> {
                                                                        assertThat(val2).isNull();
                                                                    });
                                                });
                            });
            Thread.sleep(3000);
            recordContext1.release();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Returns an Integer key in specified keyGroupRange. */
    private int getKeyInKeyGroup(Random random, int maxParallelism, KeyGroupRange keyGroupRange) {
        int keyInKG = random.nextInt();
        int kg = KeyGroupRangeAssignment.assignToKeyGroup(keyInKG, maxParallelism);
        while (!keyGroupRange.contains(kg)) {
            keyInKG = random.nextInt();
            kg = KeyGroupRangeAssignment.assignToKeyGroup(keyInKG, maxParallelism);
        }
        return keyInKG;
    }

    private static KeyedStateHandle runSnapshot(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture,
            SharedStateRegistry sharedStateRegistry)
            throws Exception {

        if (!snapshotRunnableFuture.isDone()) {
            snapshotRunnableFuture.run();
        }

        SnapshotResult<KeyedStateHandle> snapshotResult = snapshotRunnableFuture.get();
        KeyedStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();
        if (jobManagerOwnedSnapshot != null) {
            jobManagerOwnedSnapshot.registerSharedStates(sharedStateRegistry, 0L);
        }
        return jobManagerOwnedSnapshot;
    }

    static class TestAsyncFrameworkExceptionHandler
            implements AsyncFutureImpl.AsyncFrameworkExceptionHandler {
        String message = null;
        Throwable exception = null;

        public void handleException(String message, Throwable exception) {
            this.message = message;
            this.exception = exception;
        }
    }
}
