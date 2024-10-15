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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.state.StateFutureImpl;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
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
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
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

    // lazily initialized stream storage
    private CheckpointStreamFactory checkpointStreamFactory;

    @BeforeEach
    void before() throws Exception {
        env = buildMockEnv();
    }

    private MockEnvironment buildMockEnv() throws Exception {
        MockEnvironment mockEnvironment =
                MockEnvironment.builder().setTaskStateManager(getTestTaskStateManager()).build();
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
        return getCheckpointStorage().createCheckpointStorage(new JobID());
    }

    protected CheckpointStreamFactory createStreamFactory() throws Exception {
        if (checkpointStreamFactory == null) {
            checkpointStreamFactory =
                    getCheckpointStorage()
                            .createCheckpointStorage(new JobID())
                            .resolveCheckpointStorageLocation(
                                    1L, CheckpointStorageLocationReference.getDefault());
        }

        return checkpointStreamFactory;
    }

    protected <K> AsyncKeyedStateBackend<K> createAsyncKeyedBackend(
            TypeSerializer<K> keySerializer, int numberOfKeyGroups, Environment env)
            throws Exception {

        env.setCheckpointStorageAccess(getCheckpointStorageAccess());

        return getStateBackend()
                .createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                new JobID(),
                                "test_op",
                                keySerializer,
                                numberOfKeyGroups,
                                new KeyGroupRange(0, numberOfKeyGroups - 1),
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
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            List<KeyedStateHandle> state,
            Environment env)
            throws Exception {

        return getStateBackend()
                .createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                new JobID(),
                                "test_op",
                                keySerializer,
                                numberOfKeyGroups,
                                new KeyGroupRange(0, numberOfKeyGroups - 1),
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
        AsyncExecutionController<Integer> aec;

        TestAsyncFrameworkExceptionHandler testExceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        KeyedStateHandle stateHandle;

        try {
            backend = createAsyncKeyedBackend(IntSerializer.INSTANCE, jobMaxParallelism, env);
            aec =
                    new AsyncExecutionController<>(
                            new SyncMailboxExecutor(),
                            testExceptionHandler,
                            backend.createStateExecutor(),
                            jobMaxParallelism,
                            aecBatchSize,
                            aecBufferTimeout,
                            aecMaxInFlightRecords,
                            null);
            backend.setup(aec);

            ValueStateDescriptor<Integer> stateDescriptor =
                    new ValueStateDescriptor<>("test", BasicTypeInfo.INT_TYPE_INFO);

            ValueState<Integer> valueState =
                    backend.createState(
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

        try {
            backend =
                    restoreAsyncKeyedBackend(
                            IntSerializer.INSTANCE,
                            jobMaxParallelism,
                            Collections.singletonList(stateHandle),
                            env);
            aec =
                    new AsyncExecutionController<>(
                            new SyncMailboxExecutor(),
                            testExceptionHandler,
                            backend.createStateExecutor(),
                            jobMaxParallelism,
                            aecBatchSize,
                            aecBufferTimeout,
                            aecMaxInFlightRecords,
                            null);
            backend.setup(aec);

            ValueStateDescriptor<Integer> stateDescriptor =
                    new ValueStateDescriptor<>("test", BasicTypeInfo.INT_TYPE_INFO);

            ValueState<Integer> valueState =
                    backend.createState(
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
                createAsyncKeyedBackend(IntSerializer.INSTANCE, 128, env);
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

    void testValueStateWorkWithTtl() throws Exception {
        TestAsyncFrameworkExceptionHandler testExceptionHandler =
                new TestAsyncFrameworkExceptionHandler();
        AsyncKeyedStateBackend<Long> backend =
                createAsyncKeyedBackend(LongSerializer.INSTANCE, 128, env);
        AsyncExecutionController<Long> aec =
                new AsyncExecutionController<>(
                        new SyncMailboxExecutor(),
                        testExceptionHandler,
                        backend.createStateExecutor(),
                        128,
                        1,
                        -1,
                        1,
                        null);
        backend.setup(aec);
        try {
            ValueStateDescriptor<Long> kvId =
                    new ValueStateDescriptor<>("id", TypeInformation.of(Long.class));
            kvId.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofSeconds(1)).build());

            ValueState<Long> state =
                    backend.createState(
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

    static class TestAsyncFrameworkExceptionHandler
            implements StateFutureImpl.AsyncFrameworkExceptionHandler {
        String message = null;
        Throwable exception = null;

        public void handleException(String message, Throwable exception) {
            this.message = message;
            this.exception = exception;
        }
    }
}
