/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.runtime.util.BlockingCheckpointOutputStream;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OperatorStateBackendTest {

    @RegisterExtension
    private static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(Executors::newCachedThreadPool);

    private final ClassLoader classLoader = getClass().getClassLoader();
    private final Collection<OperatorStateHandle> emptyStateHandles = Collections.emptyList();

    @Test
    void testCreateOnAbstractStateBackend() throws Exception {
        // we use the memory state backend as a subclass of the AbstractStateBackend
        final AbstractStateBackend abstractStateBackend = new MemoryStateBackend();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        final OperatorStateBackend operatorStateBackend =
                abstractStateBackend.createOperatorStateBackend(
                        createMockEnvironment(),
                        "test-operator",
                        emptyStateHandles,
                        cancelStreamRegistry);

        assertThat(operatorStateBackend).isNotNull();
        assertThat(operatorStateBackend.getRegisteredStateNames()).isEmpty();
        assertThat(operatorStateBackend.getRegisteredBroadcastStateNames()).isEmpty();
    }

    @Test
    void testRegisterStatesWithoutTypeSerializer() throws Exception {
        // prepare an execution config with a non standard type registered
        final Class<?> registeredType = FutureTask.class;

        // validate the precondition of this test - if this condition fails, we need to pick a
        // different
        // example serializer
        assertThat(
                        new KryoSerializer<>(File.class, new ExecutionConfig())
                                        .getKryo()
                                        .getDefaultSerializer(registeredType)
                                instanceof com.esotericsoftware.kryo.serializers.JavaSerializer)
                .isFalse();

        final ExecutionConfig cfg = new ExecutionConfig();
        cfg.registerTypeWithKryoSerializer(
                registeredType, com.esotericsoftware.kryo.serializers.JavaSerializer.class);

        final OperatorStateBackend operatorStateBackend =
                new DefaultOperatorStateBackendBuilder(
                                classLoader, cfg, false, emptyStateHandles, new CloseableRegistry())
                        .build();

        ListStateDescriptor<File> stateDescriptor = new ListStateDescriptor<>("test", File.class);
        ListStateDescriptor<String> stateDescriptor2 =
                new ListStateDescriptor<>("test2", String.class);

        ListState<File> listState = operatorStateBackend.getListState(stateDescriptor);
        assertThat(listState).isNotNull();

        ListState<String> listState2 = operatorStateBackend.getListState(stateDescriptor2);
        assertThat(listState2).isNotNull();

        assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(2);

        // make sure that type registrations are forwarded
        TypeSerializer<?> serializer =
                ((PartitionableListState<?>) listState)
                        .getStateMetaInfo()
                        .getPartitionStateSerializer();
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getSerializer(registeredType))
                .isInstanceOf(com.esotericsoftware.kryo.serializers.JavaSerializer.class);

        Iterator<String> it = listState2.get().iterator();
        assertThat(it).isExhausted();
        listState2.add("kevin");
        listState2.add("sunny");

        it = listState2.get().iterator();
        assertThat(it.next()).isEqualTo("kevin");
        assertThat(it.next()).isEqualTo("sunny");
        assertThat(it).isExhausted();
    }

    @Test
    void testRegisterStates() throws Exception {
        final OperatorStateBackend operatorStateBackend =
                new DefaultOperatorStateBackendBuilder(
                                classLoader,
                                new ExecutionConfig(),
                                false,
                                emptyStateHandles,
                                new CloseableRegistry())
                        .build();

        ListStateDescriptor<Serializable> stateDescriptor1 =
                new ListStateDescriptor<>("test1", new JavaSerializer<>());
        ListStateDescriptor<Serializable> stateDescriptor2 =
                new ListStateDescriptor<>("test2", new JavaSerializer<>());
        ListStateDescriptor<Serializable> stateDescriptor3 =
                new ListStateDescriptor<>("test3", new JavaSerializer<>());
        ListState<Serializable> listState1 = operatorStateBackend.getListState(stateDescriptor1);
        assertThat(listState1).isNotNull();
        assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(1);
        Iterator<Serializable> it = listState1.get().iterator();
        assertThat(it).isExhausted();
        listState1.add(42);
        listState1.add(4711);

        it = listState1.get().iterator();
        assertThat(it.next()).isEqualTo(42);
        assertThat(it.next()).isEqualTo(4711);
        assertThat(it).isExhausted();

        ListState<Serializable> listState2 = operatorStateBackend.getListState(stateDescriptor2);
        assertThat(listState2).isNotNull();
        assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(2);
        assertThat(it).isExhausted();
        listState2.add(7);
        listState2.add(13);
        listState2.add(23);

        it = listState2.get().iterator();
        assertThat(it.next()).isEqualTo(7);
        assertThat(it.next()).isEqualTo(13);
        assertThat(it.next()).isEqualTo(23);
        assertThat(it).isExhausted();

        ListState<Serializable> listState3 =
                operatorStateBackend.getUnionListState(stateDescriptor3);
        assertThat(listState3).isNotNull();
        assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(3);
        assertThat(it).isExhausted();
        listState3.add(17);
        listState3.add(3);
        listState3.add(123);

        it = listState3.get().iterator();
        assertThat(it.next()).isEqualTo(17);
        assertThat(it.next()).isEqualTo(3);
        assertThat(it.next()).isEqualTo(123);
        assertThat(it).isExhausted();

        ListState<Serializable> listState1b = operatorStateBackend.getListState(stateDescriptor1);
        assertThat(listState1b).isNotNull();
        listState1b.add(123);
        it = listState1b.get().iterator();
        assertThat(it.next()).isEqualTo(42);
        assertThat(it.next()).isEqualTo(4711);
        assertThat(it.next()).isEqualTo(123);
        assertThat(it).isExhausted();

        it = listState1.get().iterator();
        assertThat(it.next()).isEqualTo(42);
        assertThat(it.next()).isEqualTo(4711);
        assertThat(it.next()).isEqualTo(123);
        assertThat(it).isExhausted();

        it = listState1b.get().iterator();
        assertThat(it.next()).isEqualTo(42);
        assertThat(it.next()).isEqualTo(4711);
        assertThat(it.next()).isEqualTo(123);
        assertThat(it).isExhausted();

        assertThatThrownBy(() -> operatorStateBackend.getUnionListState(stateDescriptor2))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> operatorStateBackend.getListState(stateDescriptor3))
                .isInstanceOf(IllegalStateException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testCorrectClassLoaderUsedOnSnapshot() throws Exception {

        AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

        final Environment env = createMockEnvironment();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        OperatorStateBackend operatorStateBackend =
                abstractStateBackend.createOperatorStateBackend(
                        env, "test-op-name", emptyStateHandles, cancelStreamRegistry);

        AtomicInteger copyCounter = new AtomicInteger(0);
        TypeSerializer<Integer> serializer =
                new VerifyingIntSerializer(
                        env.getUserCodeClassLoader().asClassLoader(), copyCounter);

        // write some state
        ListStateDescriptor<Integer> stateDescriptor =
                new ListStateDescriptor<>("test", serializer);
        ListState<Integer> listState = operatorStateBackend.getListState(stateDescriptor);

        listState.add(42);

        AtomicInteger keyCopyCounter = new AtomicInteger(0);
        AtomicInteger valueCopyCounter = new AtomicInteger(0);

        TypeSerializer<Integer> keySerializer =
                new VerifyingIntSerializer(
                        env.getUserCodeClassLoader().asClassLoader(), keyCopyCounter);
        TypeSerializer<Integer> valueSerializer =
                new VerifyingIntSerializer(
                        env.getUserCodeClassLoader().asClassLoader(), valueCopyCounter);

        MapStateDescriptor<Integer, Integer> broadcastStateDesc =
                new MapStateDescriptor<>("test-broadcast", keySerializer, valueSerializer);

        BroadcastState<Integer, Integer> broadcastState =
                operatorStateBackend.getBroadcastState(broadcastStateDesc);
        broadcastState.put(1, 2);
        broadcastState.put(3, 4);
        broadcastState.put(5, 6);

        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(4096);
        RunnableFuture<SnapshotResult<OperatorStateHandle>> runnableFuture =
                operatorStateBackend.snapshot(
                        1, 1, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
        FutureUtils.runIfNotDoneAndGet(runnableFuture);

        // make sure that the copy method has been called
        assertThat(copyCounter.get()).isGreaterThan(0);
        assertThat(keyCopyCounter.get()).isGreaterThan(0);
        assertThat(valueCopyCounter.get()).isGreaterThan(0);
    }

    /** Int serializer which verifies that the given classloader is set for the copy operation */
    private static final class VerifyingIntSerializer extends TypeSerializer<Integer> {

        private static final long serialVersionUID = -5344563614550163898L;

        private transient ClassLoader classLoader;
        private transient AtomicInteger atomicInteger;

        private VerifyingIntSerializer(ClassLoader classLoader, AtomicInteger atomicInteger) {
            this.classLoader = Preconditions.checkNotNull(classLoader);
            this.atomicInteger = Preconditions.checkNotNull(atomicInteger);
        }

        @Override
        public boolean isImmutableType() {
            // otherwise the copy method won't be called for the deepCopy operation
            return false;
        }

        @Override
        public TypeSerializer<Integer> duplicate() {
            return this;
        }

        @Override
        public Integer createInstance() {
            return 0;
        }

        @Override
        public Integer copy(Integer from) {
            assertThat(classLoader).isEqualTo(Thread.currentThread().getContextClassLoader());
            atomicInteger.incrementAndGet();
            return IntSerializer.INSTANCE.copy(from);
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            assertThat(classLoader).isEqualTo(Thread.currentThread().getContextClassLoader());
            atomicInteger.incrementAndGet();
            return IntSerializer.INSTANCE.copy(from, reuse);
        }

        @Override
        public int getLength() {
            return IntSerializer.INSTANCE.getLength();
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            IntSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            return IntSerializer.INSTANCE.deserialize(source);
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            return IntSerializer.INSTANCE.deserialize(reuse, source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            assertThat(classLoader).isEqualTo(Thread.currentThread().getContextClassLoader());
            atomicInteger.incrementAndGet();
            IntSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof VerifyingIntSerializer;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return new VerifyingIntSerializerSnapshot();
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class VerifyingIntSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Integer> {
        public VerifyingIntSerializerSnapshot() {
            super(
                    () ->
                            new VerifyingIntSerializer(
                                    Thread.currentThread().getContextClassLoader(),
                                    new AtomicInteger()));
        }
    }

    @Test
    void testSnapshotEmpty() throws Exception {
        final AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        final OperatorStateBackend operatorStateBackend =
                abstractStateBackend.createOperatorStateBackend(
                        createMockEnvironment(),
                        "testOperator",
                        emptyStateHandles,
                        cancelStreamRegistry);

        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(4096);

        RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot =
                operatorStateBackend.snapshot(
                        0L,
                        0L,
                        streamFactory,
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        SnapshotResult<OperatorStateHandle> snapshotResult =
                FutureUtils.runIfNotDoneAndGet(snapshot);
        OperatorStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();
        assertThat(stateHandle).isNull();
    }

    @Test
    void testSnapshotBroadcastStateWithEmptyOperatorState() throws Exception {
        final AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

        OperatorStateBackend operatorStateBackend =
                abstractStateBackend.createOperatorStateBackend(
                        createMockEnvironment(),
                        "testOperator",
                        emptyStateHandles,
                        new CloseableRegistry());

        final MapStateDescriptor<Integer, Integer> broadcastStateDesc =
                new MapStateDescriptor<>(
                        "test-broadcast", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

        final Map<Integer, Integer> expected = new HashMap<>(3);
        expected.put(1, 2);
        expected.put(3, 4);
        expected.put(5, 6);

        final BroadcastState<Integer, Integer> broadcastState =
                operatorStateBackend.getBroadcastState(broadcastStateDesc);
        broadcastState.putAll(expected);

        final CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(4096);
        OperatorStateHandle stateHandle = null;

        try {
            RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot =
                    operatorStateBackend.snapshot(
                            0L,
                            0L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            SnapshotResult<OperatorStateHandle> snapshotResult =
                    FutureUtils.runIfNotDoneAndGet(snapshot);
            stateHandle = snapshotResult.getJobManagerOwnedSnapshot();
            assertThat(stateHandle).isNotNull();

            final Map<Integer, Integer> retrieved = new HashMap<>();

            operatorStateBackend =
                    recreateOperatorStateBackend(
                            operatorStateBackend,
                            abstractStateBackend,
                            StateObjectCollection.singleton(stateHandle));
            BroadcastState<Integer, Integer> retrievedState =
                    operatorStateBackend.getBroadcastState(broadcastStateDesc);
            for (Map.Entry<Integer, Integer> e : retrievedState.entries()) {
                retrieved.put(e.getKey(), e.getValue());
            }
            assertThat(retrieved).isEqualTo(expected);

            // remove an element from both expected and stored state.
            retrievedState.remove(1);
            expected.remove(1);

            snapshot =
                    operatorStateBackend.snapshot(
                            1L,
                            1L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshotResult = FutureUtils.runIfNotDoneAndGet(snapshot);

            stateHandle.discardState();
            stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

            retrieved.clear();
            operatorStateBackend =
                    recreateOperatorStateBackend(
                            operatorStateBackend,
                            abstractStateBackend,
                            StateObjectCollection.singleton(stateHandle));
            retrievedState = operatorStateBackend.getBroadcastState(broadcastStateDesc);
            for (Map.Entry<Integer, Integer> e : retrievedState.immutableEntries()) {
                retrieved.put(e.getKey(), e.getValue());
            }
            assertThat(retrieved).isEqualTo(expected);

            // remove all elements from both expected and stored state.
            retrievedState.clear();
            expected.clear();

            snapshot =
                    operatorStateBackend.snapshot(
                            2L,
                            2L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshotResult = FutureUtils.runIfNotDoneAndGet(snapshot);
            if (stateHandle != null) {
                stateHandle.discardState();
            }
            stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

            retrieved.clear();
            operatorStateBackend =
                    recreateOperatorStateBackend(
                            operatorStateBackend,
                            abstractStateBackend,
                            StateObjectCollection.singleton(stateHandle));
            retrievedState = operatorStateBackend.getBroadcastState(broadcastStateDesc);
            for (Map.Entry<Integer, Integer> e : retrievedState.immutableEntries()) {
                retrieved.put(e.getKey(), e.getValue());
            }
            assertThat(expected).isEmpty();
            assertThat(retrieved).isEqualTo(expected);
            if (stateHandle != null) {
                stateHandle.discardState();
                stateHandle = null;
            }
        } finally {
            operatorStateBackend.close();
            operatorStateBackend.dispose();
            if (stateHandle != null) {
                stateHandle.discardState();
            }
        }
    }

    @Test
    void testSnapshotRestoreSync() throws Exception {
        AbstractStateBackend abstractStateBackend = new MemoryStateBackend(2 * 4096);

        OperatorStateBackend operatorStateBackend =
                abstractStateBackend.createOperatorStateBackend(
                        createMockEnvironment(),
                        "test-op-name",
                        emptyStateHandles,
                        new CloseableRegistry());
        ListStateDescriptor<Serializable> stateDescriptor1 =
                new ListStateDescriptor<>("test1", new JavaSerializer<>());
        ListStateDescriptor<Serializable> stateDescriptor2 =
                new ListStateDescriptor<>("test2", new JavaSerializer<>());
        ListStateDescriptor<Serializable> stateDescriptor3 =
                new ListStateDescriptor<>("test3", new JavaSerializer<>());

        MapStateDescriptor<Serializable, Serializable> broadcastStateDescriptor1 =
                new MapStateDescriptor<>("test4", new JavaSerializer<>(), new JavaSerializer<>());
        MapStateDescriptor<Serializable, Serializable> broadcastStateDescriptor2 =
                new MapStateDescriptor<>("test5", new JavaSerializer<>(), new JavaSerializer<>());
        MapStateDescriptor<Serializable, Serializable> broadcastStateDescriptor3 =
                new MapStateDescriptor<>("test6", new JavaSerializer<>(), new JavaSerializer<>());

        ListState<Serializable> listState1 = operatorStateBackend.getListState(stateDescriptor1);
        ListState<Serializable> listState2 = operatorStateBackend.getListState(stateDescriptor2);
        ListState<Serializable> listState3 =
                operatorStateBackend.getUnionListState(stateDescriptor3);

        BroadcastState<Serializable, Serializable> broadcastState1 =
                operatorStateBackend.getBroadcastState(broadcastStateDescriptor1);
        BroadcastState<Serializable, Serializable> broadcastState2 =
                operatorStateBackend.getBroadcastState(broadcastStateDescriptor2);
        BroadcastState<Serializable, Serializable> broadcastState3 =
                operatorStateBackend.getBroadcastState(broadcastStateDescriptor3);

        listState1.add(42);
        listState1.add(4711);

        listState2.add(7);
        listState2.add(13);
        listState2.add(23);

        listState3.add(17);
        listState3.add(18);
        listState3.add(19);
        listState3.add(20);

        broadcastState1.put(1, 2);
        broadcastState1.put(2, 5);

        broadcastState2.put(2, 5);

        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(2 * 4096);
        RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot =
                operatorStateBackend.snapshot(
                        1L,
                        1L,
                        streamFactory,
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        SnapshotResult<OperatorStateHandle> snapshotResult =
                FutureUtils.runIfNotDoneAndGet(snapshot);
        OperatorStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

        try {

            operatorStateBackend.close();
            operatorStateBackend.dispose();

            operatorStateBackend =
                    abstractStateBackend.createOperatorStateBackend(
                            createMockEnvironment(),
                            "testOperator",
                            StateObjectCollection.singleton(stateHandle),
                            new CloseableRegistry());

            assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(3);
            assertThat(operatorStateBackend.getRegisteredBroadcastStateNames()).hasSize(3);

            listState1 = operatorStateBackend.getListState(stateDescriptor1);
            listState2 = operatorStateBackend.getListState(stateDescriptor2);
            listState3 = operatorStateBackend.getUnionListState(stateDescriptor3);

            broadcastState1 = operatorStateBackend.getBroadcastState(broadcastStateDescriptor1);
            broadcastState2 = operatorStateBackend.getBroadcastState(broadcastStateDescriptor2);
            broadcastState3 = operatorStateBackend.getBroadcastState(broadcastStateDescriptor3);

            assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(3);
            assertThat(operatorStateBackend.getRegisteredBroadcastStateNames()).hasSize(3);

            Iterator<Serializable> it = listState1.get().iterator();
            assertThat(it.next()).isEqualTo(42);
            assertThat(it.next()).isEqualTo(4711);
            assertThat(it).isExhausted();

            it = listState2.get().iterator();
            assertThat(it.next()).isEqualTo(7);
            assertThat(it.next()).isEqualTo(13);
            assertThat(it.next()).isEqualTo(23);
            assertThat(it).isExhausted();

            it = listState3.get().iterator();
            assertThat(it.next()).isEqualTo(17);
            assertThat(it.next()).isEqualTo(18);
            assertThat(it.next()).isEqualTo(19);
            assertThat(it.next()).isEqualTo(20);
            assertThat(it).isExhausted();

            Iterator<Map.Entry<Serializable, Serializable>> bIt = broadcastState1.iterator();
            assertThat(bIt).hasNext();
            Map.Entry<Serializable, Serializable> entry = bIt.next();
            assertThat(entry.getKey()).isEqualTo(1);
            assertThat(entry.getValue()).isEqualTo(2);

            assertThat(bIt).hasNext();
            entry = bIt.next();
            assertThat(entry.getKey()).isEqualTo(2);
            assertThat(entry.getValue()).isEqualTo(5);
            assertThat(bIt).isExhausted();

            bIt = broadcastState2.iterator();
            assertThat(bIt).hasNext();
            entry = bIt.next();
            assertThat(entry.getKey()).isEqualTo(2);
            assertThat(entry.getValue()).isEqualTo(5);
            assertThat(bIt).isExhausted();

            bIt = broadcastState3.iterator();
            assertThat(bIt).isExhausted();

            operatorStateBackend.close();
            operatorStateBackend.dispose();
        } finally {
            stateHandle.discardState();
        }
    }

    @Test
    void testSnapshotRestoreAsync() throws Exception {
        OperatorStateBackend operatorStateBackend =
                new DefaultOperatorStateBackendBuilder(
                                OperatorStateBackendTest.class.getClassLoader(),
                                new ExecutionConfig(),
                                true,
                                emptyStateHandles,
                                new CloseableRegistry())
                        .build();

        ListStateDescriptor<MutableType> stateDescriptor1 =
                new ListStateDescriptor<>("test1", new JavaSerializer<MutableType>());
        ListStateDescriptor<MutableType> stateDescriptor2 =
                new ListStateDescriptor<>("test2", new JavaSerializer<MutableType>());
        ListStateDescriptor<MutableType> stateDescriptor3 =
                new ListStateDescriptor<>("test3", new JavaSerializer<MutableType>());

        MapStateDescriptor<MutableType, MutableType> broadcastStateDescriptor1 =
                new MapStateDescriptor<>(
                        "test4",
                        new JavaSerializer<MutableType>(),
                        new JavaSerializer<MutableType>());
        MapStateDescriptor<MutableType, MutableType> broadcastStateDescriptor2 =
                new MapStateDescriptor<>(
                        "test5",
                        new JavaSerializer<MutableType>(),
                        new JavaSerializer<MutableType>());
        MapStateDescriptor<MutableType, MutableType> broadcastStateDescriptor3 =
                new MapStateDescriptor<>(
                        "test6",
                        new JavaSerializer<MutableType>(),
                        new JavaSerializer<MutableType>());

        ListState<MutableType> listState1 = operatorStateBackend.getListState(stateDescriptor1);
        ListState<MutableType> listState2 = operatorStateBackend.getListState(stateDescriptor2);
        ListState<MutableType> listState3 =
                operatorStateBackend.getUnionListState(stateDescriptor3);

        BroadcastState<MutableType, MutableType> broadcastState1 =
                operatorStateBackend.getBroadcastState(broadcastStateDescriptor1);
        BroadcastState<MutableType, MutableType> broadcastState2 =
                operatorStateBackend.getBroadcastState(broadcastStateDescriptor2);
        BroadcastState<MutableType, MutableType> broadcastState3 =
                operatorStateBackend.getBroadcastState(broadcastStateDescriptor3);

        listState1.add(MutableType.of(42));
        listState1.add(MutableType.of(4711));

        listState2.add(MutableType.of(7));
        listState2.add(MutableType.of(13));
        listState2.add(MutableType.of(23));

        listState3.add(MutableType.of(17));
        listState3.add(MutableType.of(18));
        listState3.add(MutableType.of(19));
        listState3.add(MutableType.of(20));

        broadcastState1.put(MutableType.of(1), MutableType.of(2));
        broadcastState1.put(MutableType.of(2), MutableType.of(5));

        broadcastState2.put(MutableType.of(2), MutableType.of(5));

        BlockerCheckpointStreamFactory streamFactory =
                new BlockerCheckpointStreamFactory(1024 * 1024);

        OneShotLatch waiterLatch = new OneShotLatch();
        OneShotLatch blockerLatch = new OneShotLatch();

        streamFactory.setWaiterLatch(waiterLatch);
        streamFactory.setBlockerLatch(blockerLatch);

        RunnableFuture<SnapshotResult<OperatorStateHandle>> runnableFuture =
                operatorStateBackend.snapshot(
                        1, 1, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

        ExecutorService executorService = EXECUTOR_EXTENSION.getExecutor();

        executorService.submit(runnableFuture);

        // wait until the async checkpoint is in the write code, then continue
        waiterLatch.await();

        // do some mutations to the state, to test if our snapshot will NOT reflect them

        listState1.add(MutableType.of(77));

        broadcastState1.put(MutableType.of(32), MutableType.of(97));

        int n = 0;

        for (MutableType mutableType : listState2.get()) {
            if (++n == 2) {
                // allow the write code to continue, so that we could do changes while state is
                // written in parallel.
                blockerLatch.trigger();
            }
            mutableType.setValue(mutableType.getValue() + 10);
        }

        listState3.clear();
        broadcastState2.clear();

        operatorStateBackend.getListState(
                new ListStateDescriptor<>("test4", new JavaSerializer<MutableType>()));

        // run the snapshot
        SnapshotResult<OperatorStateHandle> snapshotResult = runnableFuture.get();
        OperatorStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

        try {

            operatorStateBackend.close();
            operatorStateBackend.dispose();

            AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);
            CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

            operatorStateBackend =
                    abstractStateBackend.createOperatorStateBackend(
                            createMockEnvironment(),
                            "testOperator",
                            StateObjectCollection.singleton(stateHandle),
                            cancelStreamRegistry);

            assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(3);
            assertThat(operatorStateBackend.getRegisteredBroadcastStateNames()).hasSize(3);

            listState1 = operatorStateBackend.getListState(stateDescriptor1);
            listState2 = operatorStateBackend.getListState(stateDescriptor2);
            listState3 = operatorStateBackend.getUnionListState(stateDescriptor3);

            broadcastState1 = operatorStateBackend.getBroadcastState(broadcastStateDescriptor1);
            broadcastState2 = operatorStateBackend.getBroadcastState(broadcastStateDescriptor2);
            broadcastState3 = operatorStateBackend.getBroadcastState(broadcastStateDescriptor3);

            assertThat(operatorStateBackend.getRegisteredStateNames()).hasSize(3);
            assertThat(operatorStateBackend.getRegisteredBroadcastStateNames()).hasSize(3);

            Iterator<MutableType> it = listState1.get().iterator();
            assertThat(it.next().value).isEqualTo(42);
            assertThat(it.next().value).isEqualTo(4711);
            assertThat(it).isExhausted();

            it = listState2.get().iterator();
            assertThat(it.next().value).isEqualTo(7);
            assertThat(it.next().value).isEqualTo(13);
            assertThat(it.next().value).isEqualTo(23);
            assertThat(it).isExhausted();

            it = listState3.get().iterator();
            assertThat(it.next().value).isEqualTo(17);
            assertThat(it.next().value).isEqualTo(18);
            assertThat(it.next().value).isEqualTo(19);
            assertThat(it.next().value).isEqualTo(20);
            assertThat(it).isExhausted();

            Iterator<Map.Entry<MutableType, MutableType>> bIt = broadcastState1.iterator();
            assertThat(bIt).hasNext();
            Map.Entry<MutableType, MutableType> entry = bIt.next();
            assertThat(entry.getKey().value).isOne();
            assertThat(entry.getValue().value).isEqualTo(2);
            assertThat(bIt).hasNext();
            entry = bIt.next();
            assertThat(entry.getKey().value).isEqualTo(2);
            assertThat(entry.getValue().value).isEqualTo(5);
            assertThat(bIt).isExhausted();

            bIt = broadcastState2.iterator();
            assertThat(bIt).hasNext();
            entry = bIt.next();
            assertThat(entry.getKey().value).isEqualTo(2);
            assertThat(entry.getValue().value).isEqualTo(5);
            assertThat(bIt).isExhausted();

            bIt = broadcastState3.iterator();
            assertThat(bIt).isExhausted();

            operatorStateBackend.close();
            operatorStateBackend.dispose();
        } finally {
            stateHandle.discardState();
        }

        executorService.shutdown();
    }

    @Test
    void testSnapshotAsyncClose() throws Exception {
        DefaultOperatorStateBackend operatorStateBackend =
                new DefaultOperatorStateBackendBuilder(
                                OperatorStateBackendTest.class.getClassLoader(),
                                new ExecutionConfig(),
                                true,
                                emptyStateHandles,
                                new CloseableRegistry())
                        .build();

        ListStateDescriptor<MutableType> stateDescriptor1 =
                new ListStateDescriptor<>("test1", new JavaSerializer<MutableType>());

        ListState<MutableType> listState1 = operatorStateBackend.getListState(stateDescriptor1);

        listState1.add(MutableType.of(42));
        listState1.add(MutableType.of(4711));

        MapStateDescriptor<MutableType, MutableType> broadcastStateDescriptor1 =
                new MapStateDescriptor<>(
                        "test4",
                        new JavaSerializer<MutableType>(),
                        new JavaSerializer<MutableType>());

        BroadcastState<MutableType, MutableType> broadcastState1 =
                operatorStateBackend.getBroadcastState(broadcastStateDescriptor1);
        broadcastState1.put(MutableType.of(1), MutableType.of(2));
        broadcastState1.put(MutableType.of(2), MutableType.of(5));

        BlockerCheckpointStreamFactory streamFactory =
                new BlockerCheckpointStreamFactory(1024 * 1024);

        OneShotLatch waiterLatch = new OneShotLatch();
        OneShotLatch blockerLatch = new OneShotLatch();

        streamFactory.setWaiterLatch(waiterLatch);
        streamFactory.setBlockerLatch(blockerLatch);

        RunnableFuture<SnapshotResult<OperatorStateHandle>> runnableFuture =
                operatorStateBackend.snapshot(
                        1, 1, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

        EXECUTOR_EXTENSION.getExecutor().submit(runnableFuture);

        // wait until the async checkpoint is in the write code, then continue
        waiterLatch.await();

        operatorStateBackend.close();

        blockerLatch.trigger();

        assertThatThrownBy(() -> runnableFuture.get(60, TimeUnit.SECONDS))
                .isInstanceOf(CancellationException.class);
    }

    @Test
    void testSnapshotAsyncCancel() throws Exception {
        DefaultOperatorStateBackend operatorStateBackend =
                new DefaultOperatorStateBackendBuilder(
                                OperatorStateBackendTest.class.getClassLoader(),
                                new ExecutionConfig(),
                                true,
                                emptyStateHandles,
                                new CloseableRegistry())
                        .build();

        ListStateDescriptor<MutableType> stateDescriptor1 =
                new ListStateDescriptor<>("test1", new JavaSerializer<MutableType>());

        ListState<MutableType> listState1 = operatorStateBackend.getListState(stateDescriptor1);

        listState1.add(MutableType.of(42));
        listState1.add(MutableType.of(4711));

        BlockerCheckpointStreamFactory streamFactory =
                new BlockerCheckpointStreamFactory(1024 * 1024);

        OneShotLatch waiterLatch = new OneShotLatch();
        OneShotLatch blockerLatch = new OneShotLatch();

        streamFactory.setWaiterLatch(waiterLatch);
        streamFactory.setBlockerLatch(blockerLatch);

        RunnableFuture<SnapshotResult<OperatorStateHandle>> runnableFuture =
                operatorStateBackend.snapshot(
                        1, 1, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

        EXECUTOR_EXTENSION.getExecutor().submit(runnableFuture);

        // wait until the async checkpoint is in the stream's write code, then continue
        waiterLatch.await();

        // cancel the future, which should close the underlying stream
        runnableFuture.cancel(true);

        for (BlockingCheckpointOutputStream stream : streamFactory.getAllCreatedStreams()) {
            assertThat(stream.isClosed()).isTrue();
        }

        // we allow the stream under test to proceed
        blockerLatch.trigger();

        assertThatThrownBy(() -> runnableFuture.get(60, TimeUnit.SECONDS))
                .isInstanceOf(CancellationException.class);
    }

    static final class MutableType implements Serializable {

        private static final long serialVersionUID = 1L;

        private int value;

        public MutableType() {
            this(0);
        }

        public MutableType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MutableType that = (MutableType) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }

        static MutableType of(int value) {
            return new MutableType(value);
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static Environment createMockEnvironment() {
        Environment env = mock(Environment.class);
        when(env.getExecutionConfig()).thenReturn(new ExecutionConfig());
        when(env.getUserCodeClassLoader())
                .thenReturn(TestingUserCodeClassLoader.newBuilder().build());
        return env;
    }

    private static OperatorStateBackend recreateOperatorStateBackend(
            OperatorStateBackend oldOperatorStateBackend,
            AbstractStateBackend abstractStateBackend,
            Collection<OperatorStateHandle> toRestore)
            throws Exception {
        oldOperatorStateBackend.close();
        oldOperatorStateBackend.dispose();
        return abstractStateBackend.createOperatorStateBackend(
                createMockEnvironment(), "testOperator", toRestore, new CloseableRegistry());
    }
}
