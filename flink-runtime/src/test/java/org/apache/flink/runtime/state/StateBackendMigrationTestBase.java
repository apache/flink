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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests for the {@link KeyedStateBackend} and {@link OperatorStateBackend} as produced by various
 * {@link StateBackend}s.
 *
 * <p>The tests in this test base focuses on the verification of state serializers usage when they
 * are either compatible or requiring state migration after restoring the state backends.
 */
@SuppressWarnings("serial")
public abstract class StateBackendMigrationTestBase<B extends StateBackend> {

    protected abstract B getStateBackend() throws Exception;

    protected CheckpointStorage getCheckpointStorage() throws Exception {
        StateBackend stateBackend = getStateBackend();
        if (stateBackend instanceof CheckpointStorage) {
            return (CheckpointStorage) stateBackend;
        }

        throw new IllegalStateException(
                "The state backend under test does not implement CheckpointStorage."
                        + "Please override 'createCheckpointStorage' and provide an appropriate"
                        + "checkpoint storage instance");
    }

    /**
     * @return true if key serializer supports checking. If not, expected exceptions will likely not
     *     be thrown.
     */
    protected boolean supportsKeySerializerCheck() {
        return true;
    }

    @TempDir protected Path tempFolder;

    @BeforeEach
    void before() {
        env = MockEnvironment.builder().build();
    }

    @AfterEach
    void after() {
        IOUtils.closeQuietly(env);
    }

    // lazily initialized stream storage
    private CheckpointStorageLocation checkpointStorageLocation;

    private MockEnvironment env;

    // -------------------------------------------------------------------------------
    //  Tests for keyed ValueState
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testKeyedValueStateMigration() throws Exception {
        final String stateName = "test-name";

        testKeyedValueStateUpgrade(
                new ValueStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ValueStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer()));
    }

    @TestTemplate
    void testKeyedValueStateSerializerReconfiguration() throws Exception {
        final String stateName = "test-name";

        testKeyedValueStateUpgrade(
                new ValueStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ValueStateDescriptor<>(
                        stateName,
                        // the test fails if this serializer is used instead of a reconfigured new
                        // serializer
                        new TestType.ReconfigurationRequiringTestTypeSerializer()));
    }

    @TestTemplate
    void testKeyedValueStateRegistrationFailsIfNewStateSerializerIsIncompatible() {
        final String stateName = "test-name";

        assertThatThrownBy(
                        () ->
                                testKeyedValueStateUpgrade(
                                        new ValueStateDescriptor<>(
                                                stateName, new TestType.V1TestTypeSerializer()),
                                        new ValueStateDescriptor<>(
                                                stateName,
                                                new TestType.IncompatibleTestTypeSerializer())))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    private void testKeyedValueStateUpgrade(
            ValueStateDescriptor<TestType> initialAccessDescriptor,
            ValueStateDescriptor<TestType> newAccessDescriptorAfterRestore)
            throws Exception {

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            ValueState<TestType> valueState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            CustomVoidNamespaceSerializer.INSTANCE,
                            initialAccessDescriptor);

            backend.setCurrentKey(1);
            valueState.update(new TestType("foo", 1456));
            backend.setCurrentKey(2);
            valueState.update(new TestType("bar", 478));
            backend.setCurrentKey(3);
            valueState.update(new TestType("hello", 189));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            backend.dispose();

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

            valueState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            CustomVoidNamespaceSerializer.INSTANCE,
                            newAccessDescriptorAfterRestore);

            // make sure that reading and writing each key state works with the new serializer
            backend.setCurrentKey(1);
            assertThat(valueState.value()).isEqualTo(new TestType("foo", 1456));
            valueState.update(new TestType("newValue1", 751));

            backend.setCurrentKey(2);
            assertThat(valueState.value()).isEqualTo(new TestType("bar", 478));
            valueState.update(new TestType("newValue2", 167));

            backend.setCurrentKey(3);
            assertThat(valueState.value()).isEqualTo(new TestType("hello", 189));
            valueState.update(new TestType("newValue3", 444));

            // do another snapshot to verify the snapshot logic after migration
            snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    2L,
                                    3L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            snapshot.discardState();
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Tests for keyed ListState
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testKeyedListStateMigration() throws Exception {
        final String stateName = "test-name";

        testKeyedListStateUpgrade(
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ListStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer()));
    }

    @TestTemplate
    void testKeyedListStateSerializerReconfiguration() throws Exception {
        final String stateName = "test-name";

        testKeyedListStateUpgrade(
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ListStateDescriptor<>(
                        stateName,
                        // the test fails if this serializer is used instead of a reconfigured new
                        // serializer
                        new TestType.ReconfigurationRequiringTestTypeSerializer()));
    }

    @TestTemplate
    void testKeyedListStateRegistrationFailsIfNewStateSerializerIsIncompatible() {
        final String stateName = "test-name";

        assertThatThrownBy(
                        () ->
                                testKeyedListStateUpgrade(
                                        new ListStateDescriptor<>(
                                                stateName, new TestType.V1TestTypeSerializer()),
                                        new ListStateDescriptor<>(
                                                stateName,
                                                new TestType.IncompatibleTestTypeSerializer())))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    private void testKeyedListStateUpgrade(
            ListStateDescriptor<TestType> initialAccessDescriptor,
            ListStateDescriptor<TestType> newAccessDescriptorAfterRestore)
            throws Exception {

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            ListState<TestType> listState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            CustomVoidNamespaceSerializer.INSTANCE,
                            initialAccessDescriptor);

            backend.setCurrentKey(1);
            listState.add(new TestType("key-1", 1));
            listState.add(new TestType("key-1", 2));
            listState.add(new TestType("key-1", 3));

            backend.setCurrentKey(2);
            listState.add(new TestType("key-2", 1));

            backend.setCurrentKey(3);
            listState.add(new TestType("key-3", 1));
            listState.add(new TestType("key-3", 2));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            backend.dispose();

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

            listState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            CustomVoidNamespaceSerializer.INSTANCE,
                            newAccessDescriptorAfterRestore);

            // make sure that reading and writing each key state works with the new serializer
            backend.setCurrentKey(1);
            Iterator<TestType> iterable1 = listState.get().iterator();
            assertThat(iterable1.next()).isEqualTo(new TestType("key-1", 1));
            assertThat(iterable1.next()).isEqualTo(new TestType("key-1", 2));
            assertThat(iterable1.next()).isEqualTo(new TestType("key-1", 3));
            assertThat(iterable1).isExhausted();
            listState.add(new TestType("new-key-1", 123));

            backend.setCurrentKey(2);
            Iterator<TestType> iterable2 = listState.get().iterator();
            assertThat(iterable2.next()).isEqualTo(new TestType("key-2", 1));
            assertThat(iterable2).isExhausted();
            listState.add(new TestType("new-key-2", 456));

            backend.setCurrentKey(3);
            Iterator<TestType> iterable3 = listState.get().iterator();
            assertThat(iterable3.next()).isEqualTo(new TestType("key-3", 1));
            assertThat(iterable3.next()).isEqualTo(new TestType("key-3", 2));
            assertThat(iterable3).isExhausted();
            listState.add(new TestType("new-key-3", 777));

            // do another snapshot to verify the snapshot logic after migration
            snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    2L,
                                    3L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            snapshot.discardState();

        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Tests for keyed MapState
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testKeyedMapStateAsIs() throws Exception {
        final String stateName = "test-name";

        testKeyedMapStateUpgrade(
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer()),
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer()));
    }

    @TestTemplate
    void testKeyedMapStateStateMigration() throws Exception {
        final String stateName = "test-name";

        testKeyedMapStateUpgrade(
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer()),
                new MapStateDescriptor<>(
                        stateName,
                        IntSerializer.INSTANCE,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer()));
    }

    @TestTemplate
    void testKeyedMapStateSerializerReconfiguration() throws Exception {
        final String stateName = "test-name";

        testKeyedMapStateUpgrade(
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer()),
                new MapStateDescriptor<>(
                        stateName,
                        IntSerializer.INSTANCE,
                        // restore with a V2 serializer that has a different schema
                        new TestType.ReconfigurationRequiringTestTypeSerializer()));
    }

    @TestTemplate
    void testKeyedMapStateRegistrationFailsIfNewStateSerializerIsIncompatible() {
        final String stateName = "test-name";

        assertThatThrownBy(
                        () ->
                                testKeyedMapStateUpgrade(
                                        new MapStateDescriptor<>(
                                                stateName,
                                                IntSerializer.INSTANCE,
                                                new TestType.V1TestTypeSerializer()),
                                        new MapStateDescriptor<>(
                                                stateName,
                                                IntSerializer.INSTANCE,
                                                // restore with a V2 serializer that has a different
                                                // schema
                                                new TestType.IncompatibleTestTypeSerializer())))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    private Iterator<Map.Entry<Integer, TestType>> sortedIterator(
            Iterator<Map.Entry<Integer, TestType>> iterator) {
        TreeSet<Map.Entry<Integer, TestType>> set =
                new TreeSet<Map.Entry<Integer, TestType>>(
                        Comparator.comparing(Map.Entry<Integer, TestType>::getKey));
        iterator.forEachRemaining(set::add);
        return set.iterator();
    }

    private void testKeyedMapStateUpgrade(
            MapStateDescriptor<Integer, TestType> initialAccessDescriptor,
            MapStateDescriptor<Integer, TestType> newAccessDescriptorAfterRestore)
            throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            MapState<Integer, TestType> mapState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            CustomVoidNamespaceSerializer.INSTANCE,
                            initialAccessDescriptor);

            backend.setCurrentKey(1);
            mapState.put(1, new TestType("key-1", 1));
            mapState.put(2, new TestType("key-1", 2));
            mapState.put(3, new TestType("key-1", 3));

            backend.setCurrentKey(2);
            mapState.put(1, new TestType("key-2", 1));

            backend.setCurrentKey(3);
            mapState.put(1, new TestType("key-3", 1));
            mapState.put(2, new TestType("key-3", 2));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            backend.dispose();

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

            mapState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            CustomVoidNamespaceSerializer.INSTANCE,
                            newAccessDescriptorAfterRestore);

            // make sure that reading and writing each key state works with the new serializer
            backend.setCurrentKey(1);
            // sort iterator because the order of elements is otherwise not deterministic
            Iterator<Map.Entry<Integer, TestType>> iterable1 = sortedIterator(mapState.iterator());

            Map.Entry<Integer, TestType> actual = iterable1.next();
            assertThat(actual.getKey()).isOne();
            assertThat(actual.getValue()).isEqualTo(new TestType("key-1", 1));

            actual = iterable1.next();
            assertThat(actual.getKey()).isEqualTo(2);
            assertThat(actual.getValue()).isEqualTo(new TestType("key-1", 2));

            actual = iterable1.next();
            assertThat(actual.getKey()).isEqualTo(3);
            assertThat(actual.getValue()).isEqualTo(new TestType("key-1", 3));

            assertThat(iterable1).isExhausted();

            mapState.put(123, new TestType("new-key-1", 123));

            backend.setCurrentKey(2);
            Iterator<Map.Entry<Integer, TestType>> iterable2 = mapState.iterator();

            actual = iterable2.next();
            assertThat(actual.getKey()).isOne();
            assertThat(actual.getValue()).isEqualTo(new TestType("key-2", 1));
            assertThat(iterable2).isExhausted();

            mapState.put(456, new TestType("new-key-2", 456));

            backend.setCurrentKey(3);
            // sort iterator because the order of elements is otherwise not deterministic
            Iterator<Map.Entry<Integer, TestType>> iterable3 = sortedIterator(mapState.iterator());

            actual = iterable3.next();
            assertThat(actual.getKey()).isOne();
            assertThat(actual.getValue()).isEqualTo(new TestType("key-3", 1));

            actual = iterable3.next();
            assertThat(actual.getKey()).isEqualTo(2);
            assertThat(actual.getValue()).isEqualTo(new TestType("key-3", 2));

            assertThat(iterable3).isExhausted();
            mapState.put(777, new TestType("new-key-3", 777));

            // do another snapshot to verify the snapshot logic after migration
            snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    2L,
                                    3L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            snapshot.discardState();

        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Tests for keyed priority queue state
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testPriorityQueueStateCreationFailsIfNewSerializerIsNotCompatible() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            InternalPriorityQueue<TestType> internalPriorityQueue =
                    backend.create("testPriorityQueue", new TestType.V1TestTypeSerializer());

            internalPriorityQueue.add(new TestType("key-1", 123));
            internalPriorityQueue.add(new TestType("key-2", 346));
            internalPriorityQueue.add(new TestType("key-1", 777));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            backend.dispose();

            CheckpointableKeyedStateBackend<Integer> restoredBackend =
                    restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

            assertThatThrownBy(
                            () ->
                                    restoredBackend.create(
                                            "testPriorityQueue",
                                            new TestType.IncompatibleTestTypeSerializer()))
                    .hasCauseInstanceOf(StateMigrationException.class);

        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Tests for key serializer in keyed state backends
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testStateBackendRestoreFailsIfNewKeySerializerRequiresMigration() throws Exception {
        assumeThat(supportsKeySerializerCheck()).isTrue();
        assertThatThrownBy(
                        () ->
                                testKeySerializerUpgrade(
                                        new TestType.V1TestTypeSerializer(),
                                        new TestType.V2TestTypeSerializer()))
                .hasCauseInstanceOf(StateMigrationException.class);
    }

    @TestTemplate
    void testStateBackendRestoreSucceedsIfNewKeySerializerRequiresReconfiguration()
            throws Exception {
        assumeThat(supportsKeySerializerCheck()).isTrue();
        testKeySerializerUpgrade(
                new TestType.V1TestTypeSerializer(),
                new TestType.ReconfigurationRequiringTestTypeSerializer());
    }

    @TestTemplate
    void testStateBackendRestoreFailsIfNewKeySerializerIsIncompatible() throws Exception {
        assumeThat(supportsKeySerializerCheck()).isTrue();

        assertThatThrownBy(
                        () ->
                                testKeySerializerUpgrade(
                                        new TestType.V1TestTypeSerializer(),
                                        new TestType.IncompatibleTestTypeSerializer()))
                .hasCauseInstanceOf(StateMigrationException.class);
    }

    private void testKeySerializerUpgrade(
            TypeSerializer<TestType> initialKeySerializer,
            TypeSerializer<TestType> newKeySerializer)
            throws Exception {

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        CheckpointableKeyedStateBackend<TestType> backend =
                createKeyedBackend(initialKeySerializer);

        final String stateName = "test-name";
        try {
            ValueStateDescriptor<Integer> kvId =
                    new ValueStateDescriptor<>(stateName, Integer.class);
            ValueState<Integer> valueState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(new TestType("foo", 123));
            valueState.update(1);
            backend.setCurrentKey(new TestType("bar", 456));
            valueState.update(5);

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            backend.dispose();

            backend = restoreKeyedBackend(newKeySerializer, snapshot);

            valueState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

            // access and check previous state
            backend.setCurrentKey(new TestType("foo", 123));
            assertThat(valueState.value().intValue()).isOne();
            backend.setCurrentKey(new TestType("bar", 456));
            assertThat(valueState.value().intValue()).isEqualTo(5);

            // do another snapshot to verify the snapshot logic after migration
            snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    2L,
                                    3L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            snapshot.discardState();
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Tests for namespace serializer in keyed state backends
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testKeyedStateRegistrationFailsIfNewNamespaceSerializerRequiresMigration()
            throws Exception {
        assertThatThrownBy(
                        () ->
                                testNamespaceSerializerUpgrade(
                                        new TestType.V1TestTypeSerializer(),
                                        new TestType.V2TestTypeSerializer()))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    @TestTemplate
    void testKeyedStateRegistrationSucceedsIfNewNamespaceSerializerRequiresReconfiguration()
            throws Exception {
        testNamespaceSerializerUpgrade(
                new TestType.V1TestTypeSerializer(),
                new TestType.ReconfigurationRequiringTestTypeSerializer());
    }

    @TestTemplate
    void testKeyedStateRegistrationFailsIfNewNamespaceSerializerIsIncompatible() throws Exception {
        assertThatThrownBy(
                        () ->
                                testNamespaceSerializerUpgrade(
                                        new TestType.V1TestTypeSerializer(),
                                        new TestType.IncompatibleTestTypeSerializer()))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    private void testNamespaceSerializerUpgrade(
            TypeSerializer<TestType> initialNamespaceSerializer,
            TypeSerializer<TestType> newNamespaceSerializerAfterRestore)
            throws Exception {

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        final String stateName = "test-name";
        try {
            ValueStateDescriptor<Integer> kvId =
                    new ValueStateDescriptor<>(stateName, Integer.class);
            ValueState<Integer> valueState =
                    backend.getPartitionedState(
                            new TestType("namespace", 123), initialNamespaceSerializer, kvId);

            backend.setCurrentKey(1);
            valueState.update(10);
            backend.setCurrentKey(5);
            valueState.update(50);

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // test incompatible namespace serializer; start with a freshly restored backend
            backend.dispose();
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

            valueState =
                    backend.getPartitionedState(
                            new TestType("namespace", 123),
                            newNamespaceSerializerAfterRestore,
                            kvId);

            // access and check previous state
            backend.setCurrentKey(1);
            assertThat(valueState.value().intValue()).isEqualTo(10);
            valueState.update(10);
            backend.setCurrentKey(5);
            assertThat(valueState.value().intValue()).isEqualTo(50);

            // do another snapshot to verify the snapshot logic after migration
            snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    2L,
                                    3L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            snapshot.discardState();
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Operator state backend partitionable list state tests
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testOperatorParitionableListStateMigration() throws Exception {
        final String stateName = "partitionable-list-state";

        testOperatorPartitionableListStateUpgrade(
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ListStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer()));
    }

    @TestTemplate
    void testOperatorParitionableListStateSerializerReconfiguration() throws Exception {
        final String stateName = "partitionable-list-state";

        testOperatorPartitionableListStateUpgrade(
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ListStateDescriptor<>(
                        stateName,
                        // restore with a new serializer that requires reconfiguration
                        new TestType.ReconfigurationRequiringTestTypeSerializer()));
    }

    @TestTemplate
    void testOperatorParitionableListStateRegistrationFailsIfNewSerializerIsIncompatible()
            throws Exception {
        final String stateName = "partitionable-list-state";

        assertThatThrownBy(
                        () ->
                                testOperatorPartitionableListStateUpgrade(
                                        new ListStateDescriptor<>(
                                                stateName, new TestType.V1TestTypeSerializer()),
                                        new ListStateDescriptor<>(
                                                stateName,
                                                // restore with a new incompatible serializer
                                                new TestType.IncompatibleTestTypeSerializer())))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    private void testOperatorPartitionableListStateUpgrade(
            ListStateDescriptor<TestType> initialAccessDescriptor,
            ListStateDescriptor<TestType> newAccessDescriptorAfterRestore)
            throws Exception {

        CheckpointStreamFactory streamFactory = createStreamFactory();

        OperatorStateBackend backend = createOperatorStateBackend();

        try {
            ListState<TestType> state = backend.getListState(initialAccessDescriptor);

            state.add(new TestType("foo", 13));
            state.add(new TestType("bar", 278));

            OperatorStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()));
            backend.dispose();

            backend = restoreOperatorStateBackend(snapshot);

            state = backend.getListState(newAccessDescriptorAfterRestore);

            // make sure that reading and writing each state partition works with the new serializer
            TypeSerializer internalListCopySerializer =
                    ((PartitionableListState<?>) state)
                            .getInternalListCopySerializer()
                            .getElementSerializer();
            TypeSerializer previousSerializer = initialAccessDescriptor.getElementSerializer();
            TypeSerializer newSerializerForRestoredState =
                    newAccessDescriptorAfterRestore.getElementSerializer();
            internalCopySerializerTest(
                    previousSerializer, newSerializerForRestoredState, internalListCopySerializer);

            Iterator<TestType> iterator = state.get().iterator();
            assertThat(iterator.next()).isEqualTo(new TestType("foo", 13));
            assertThat(iterator.next()).isEqualTo(new TestType("bar", 278));
            assertThat(iterator).isExhausted();
            state.add(new TestType("new-entry", 777));
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Operator state backend union list state tests
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testOperatorUnionListStateMigration() throws Exception {
        final String stateName = "union-list-state";

        testOperatorUnionListStateUpgrade(
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ListStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer()));
    }

    @TestTemplate
    void testOperatorUnionListStateSerializerReconfiguration() throws Exception {
        final String stateName = "union-list-state";

        testOperatorUnionListStateUpgrade(
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer()),
                new ListStateDescriptor<>(
                        stateName,
                        // restore with a new serializer that requires reconfiguration
                        new TestType.ReconfigurationRequiringTestTypeSerializer()));
    }

    @TestTemplate
    void testOperatorUnionListStateRegistrationFailsIfNewSerializerIsIncompatible() {
        final String stateName = "union-list-state";

        assertThatThrownBy(
                        () ->
                                testOperatorUnionListStateUpgrade(
                                        new ListStateDescriptor<>(
                                                stateName, new TestType.V1TestTypeSerializer()),
                                        new ListStateDescriptor<>(
                                                stateName,
                                                // restore with a new incompatible serializer
                                                new TestType.IncompatibleTestTypeSerializer())))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    private void testOperatorUnionListStateUpgrade(
            ListStateDescriptor<TestType> initialAccessDescriptor,
            ListStateDescriptor<TestType> newAccessDescriptorAfterRestore)
            throws Exception {

        CheckpointStreamFactory streamFactory = createStreamFactory();

        OperatorStateBackend backend = createOperatorStateBackend();

        try {
            ListState<TestType> state = backend.getUnionListState(initialAccessDescriptor);

            state.add(new TestType("foo", 13));
            state.add(new TestType("bar", 278));

            OperatorStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()));
            backend.dispose();

            backend = restoreOperatorStateBackend(snapshot);

            state = backend.getUnionListState(newAccessDescriptorAfterRestore);

            // the state backend should have decided whether or not it needs to perform state
            // migration;
            // make sure that reading and writing each state partition works with the new serializer

            TypeSerializer internalListCopySerializer =
                    ((PartitionableListState<?>) state)
                            .getInternalListCopySerializer()
                            .getElementSerializer();
            TypeSerializer previousSerializer = initialAccessDescriptor.getElementSerializer();
            TypeSerializer newSerializerForRestoredState =
                    newAccessDescriptorAfterRestore.getElementSerializer();
            internalCopySerializerTest(
                    previousSerializer, newSerializerForRestoredState, internalListCopySerializer);

            Iterator<TestType> iterator = state.get().iterator();
            assertThat(iterator.next()).isEqualTo(new TestType("foo", 13));
            assertThat(iterator.next()).isEqualTo(new TestType("bar", 278));
            assertThat(iterator).isExhausted();
            state.add(new TestType("new-entry", 777));
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------------
    //  Operator state backend broadcast state tests
    // -------------------------------------------------------------------------------

    @TestTemplate
    void testBroadcastStateValueMigration() throws Exception {
        final String stateName = "broadcast-state";

        testBroadcastStateValueUpgrade(
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer()),
                new MapStateDescriptor<>(
                        stateName,
                        IntSerializer.INSTANCE,
                        // new value serializer is a V2 serializer with a different schema
                        new TestType.V2TestTypeSerializer()));
    }

    @TestTemplate
    void testBroadcastStateKeyMigration() throws Exception {
        final String stateName = "broadcast-state";

        testBroadcastStateKeyUpgrade(
                new MapStateDescriptor<>(
                        stateName, new TestType.V1TestTypeSerializer(), IntSerializer.INSTANCE),
                new MapStateDescriptor<>(
                        stateName,
                        // new key serializer is a V2 serializer with a different schema
                        new TestType.V2TestTypeSerializer(),
                        IntSerializer.INSTANCE));
    }

    @TestTemplate
    void testBroadcastStateValueSerializerReconfiguration() throws Exception {
        final String stateName = "broadcast-state";

        testBroadcastStateValueUpgrade(
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer()),
                new MapStateDescriptor<>(
                        stateName,
                        IntSerializer.INSTANCE,
                        // new value serializer is a new serializer that requires reconfiguration
                        new TestType.ReconfigurationRequiringTestTypeSerializer()));
    }

    @TestTemplate
    void testBroadcastStateKeySerializerReconfiguration() throws Exception {
        final String stateName = "broadcast-state";

        testBroadcastStateKeyUpgrade(
                new MapStateDescriptor<>(
                        stateName, new TestType.V1TestTypeSerializer(), IntSerializer.INSTANCE),
                new MapStateDescriptor<>(
                        stateName,
                        // new key serializer is a new serializer that requires reconfiguration
                        new TestType.ReconfigurationRequiringTestTypeSerializer(),
                        IntSerializer.INSTANCE));
    }

    @TestTemplate
    void testBroadcastStateRegistrationFailsIfNewValueSerializerIsIncompatible() {
        final String stateName = "broadcast-state";

        assertThatThrownBy(
                        () ->
                                testBroadcastStateValueUpgrade(
                                        new MapStateDescriptor<>(
                                                stateName,
                                                IntSerializer.INSTANCE,
                                                new TestType.V1TestTypeSerializer()),
                                        new MapStateDescriptor<>(
                                                stateName,
                                                IntSerializer.INSTANCE,
                                                // new value serializer is incompatible
                                                new TestType.IncompatibleTestTypeSerializer())))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    @TestTemplate
    void testBroadcastStateRegistrationFailsIfNewKeySerializerIsIncompatible() {
        final String stateName = "broadcast-state";

        assertThatThrownBy(
                        () ->
                                testBroadcastStateKeyUpgrade(
                                        new MapStateDescriptor<>(
                                                stateName,
                                                new TestType.V1TestTypeSerializer(),
                                                IntSerializer.INSTANCE),
                                        new MapStateDescriptor<>(
                                                stateName,
                                                // new key serializer is incompatible
                                                new TestType.IncompatibleTestTypeSerializer(),
                                                IntSerializer.INSTANCE)))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    private void testBroadcastStateValueUpgrade(
            MapStateDescriptor<Integer, TestType> initialAccessDescriptor,
            MapStateDescriptor<Integer, TestType> newAccessDescriptorAfterRestore)
            throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();

        OperatorStateBackend backend = createOperatorStateBackend();

        try {
            BroadcastState<Integer, TestType> state =
                    backend.getBroadcastState(initialAccessDescriptor);

            state.put(3, new TestType("foo", 13));
            state.put(5, new TestType("bar", 278));

            OperatorStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()));
            backend.dispose();

            backend = restoreOperatorStateBackend(snapshot);

            state = backend.getBroadcastState(newAccessDescriptorAfterRestore);

            // the state backend should have decided whether or not it needs to perform state
            // migration;
            // make sure that reading and writing each broadcast entry works with the new serializer
            MapSerializer internalMapCopySerializer =
                    ((HeapBroadcastState) state).getInternalMapCopySerializer();
            MapSerializer previousSerializer =
                    new MapSerializer<>(
                            initialAccessDescriptor.getKeySerializer(),
                            internalMapCopySerializer.getValueSerializer());
            MapSerializer newSerializerForRestoredState =
                    new MapSerializer(
                            newAccessDescriptorAfterRestore.getKeySerializer(),
                            newAccessDescriptorAfterRestore.getValueSerializer());
            internalCopySerializerTest(
                    previousSerializer, newSerializerForRestoredState, internalMapCopySerializer);

            assertThat(state.get(3)).isEqualTo(new TestType("foo", 13));
            assertThat(state.get(5)).isEqualTo(new TestType("bar", 278));
            state.put(17, new TestType("new-entry", 777));
        } finally {
            backend.dispose();
        }
    }

    private void testBroadcastStateKeyUpgrade(
            MapStateDescriptor<TestType, Integer> initialAccessDescriptor,
            MapStateDescriptor<TestType, Integer> newAccessDescriptorAfterRestore)
            throws Exception {

        CheckpointStreamFactory streamFactory = createStreamFactory();

        OperatorStateBackend backend = createOperatorStateBackend();

        try {
            BroadcastState<TestType, Integer> state =
                    backend.getBroadcastState(initialAccessDescriptor);

            state.put(new TestType("foo", 13), 3);
            state.put(new TestType("bar", 278), 5);

            OperatorStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()));
            backend.dispose();

            backend = restoreOperatorStateBackend(snapshot);

            state = backend.getBroadcastState(newAccessDescriptorAfterRestore);

            // the state backend should have decided whether or not it needs to perform state
            // migration;
            // make sure that reading and writing each broadcast entry works with the new serializer
            assertThat(state.get(new TestType("foo", 13))).isEqualTo(3);
            assertThat(state.get(new TestType("bar", 278))).isEqualTo(5);
            state.put(new TestType("new-entry", 777), 17);
        } finally {
            backend.dispose();
        }
    }

    void internalCopySerializerTest(
            TypeSerializer previousSerializer,
            TypeSerializer newSerializerForRestoredState,
            TypeSerializer internalCopySerializer) {
        StateSerializerProvider testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        previousSerializer.snapshotConfiguration());
        testProvider.registerNewSerializerForRestoredState(newSerializerForRestoredState);

        assertThat(internalCopySerializer.getClass())
                .isEqualTo(testProvider.currentSchemaSerializer().getClass());
    }

    @TestTemplate
    void testStateMigrationAfterChangingTTL() throws Exception {
        final String stateName = "test-ttl";

        ValueStateDescriptor<TestType> initialAccessDescriptor =
                new ValueStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer());
        initialAccessDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());

        ValueStateDescriptor<TestType> newAccessDescriptorAfterRestore =
                new ValueStateDescriptor<>(stateName, new TestType.V2TestTypeSerializer());
        newAccessDescriptorAfterRestore.enableTimeToLive(
                StateTtlConfig.newBuilder(Time.days(2)).build());

        testKeyedValueStateUpgrade(initialAccessDescriptor, newAccessDescriptorAfterRestore);
    }

    @TestTemplate
    void testStateMigrationAfterChangingTTLFromEnablingToDisabling() {
        final String stateName = "test-ttl";

        ValueStateDescriptor<TestType> initialAccessDescriptor =
                new ValueStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer());
        initialAccessDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());

        ValueStateDescriptor<TestType> newAccessDescriptorAfterRestore =
                new ValueStateDescriptor<>(stateName, new TestType.V2TestTypeSerializer());

        assertThatThrownBy(
                        () ->
                                testKeyedValueStateUpgrade(
                                        initialAccessDescriptor, newAccessDescriptorAfterRestore))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    @TestTemplate
    void testStateMigrationAfterChangingTTLFromDisablingToEnabling() {
        final String stateName = "test-ttl";

        ValueStateDescriptor<TestType> initialAccessDescriptor =
                new ValueStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer());

        ValueStateDescriptor<TestType> newAccessDescriptorAfterRestore =
                new ValueStateDescriptor<>(stateName, new TestType.V2TestTypeSerializer());
        newAccessDescriptorAfterRestore.enableTimeToLive(
                StateTtlConfig.newBuilder(Time.days(1)).build());

        assertThatThrownBy(
                        () ->
                                testKeyedValueStateUpgrade(
                                        initialAccessDescriptor, newAccessDescriptorAfterRestore))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(IllegalStateException.class),
                        e -> assertThat(e).hasCauseInstanceOf(IllegalStateException.class));
    }

    // -------------------------------------------------------------------------------
    //  Test types, serializers, and serializer snapshots
    // -------------------------------------------------------------------------------

    public static class CustomVoidNamespaceSerializer extends TypeSerializer<VoidNamespace> {

        private static final long serialVersionUID = 1L;

        public static final CustomVoidNamespaceSerializer INSTANCE =
                new CustomVoidNamespaceSerializer();

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public VoidNamespace createInstance() {
            return VoidNamespace.get();
        }

        @Override
        public VoidNamespace copy(VoidNamespace from) {
            return VoidNamespace.get();
        }

        @Override
        public VoidNamespace copy(VoidNamespace from, VoidNamespace reuse) {
            return VoidNamespace.get();
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(VoidNamespace record, DataOutputView target) throws IOException {
            // Make progress in the stream, write one byte.
            //
            // We could just skip writing anything here, because of the way this is
            // used with the state backends, but if it is ever used somewhere else
            // (even though it is unlikely to happen), it would be a problem.
            target.write(0);
        }

        @Override
        public VoidNamespace deserialize(DataInputView source) throws IOException {
            source.readByte();
            return VoidNamespace.get();
        }

        @Override
        public VoidNamespace deserialize(VoidNamespace reuse, DataInputView source)
                throws IOException {
            source.readByte();
            return VoidNamespace.get();
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.write(source.readByte());
        }

        @Override
        public TypeSerializer<VoidNamespace> duplicate() {
            return this;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CustomVoidNamespaceSerializer;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public TypeSerializerSnapshot<VoidNamespace> snapshotConfiguration() {
            return new CustomVoidNamespaceSerializerSnapshot();
        }
    }

    public static class CustomVoidNamespaceSerializerSnapshot
            implements TypeSerializerSnapshot<VoidNamespace> {

        @Override
        public TypeSerializer<VoidNamespace> restoreSerializer() {
            return new CustomVoidNamespaceSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<VoidNamespace> resolveSchemaCompatibility(
                TypeSerializer<VoidNamespace> newSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {}

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {}

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CustomVoidNamespaceSerializerSnapshot;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public int getCurrentVersion() {
            return 0;
        }
    }

    private CheckpointStreamFactory createStreamFactory() throws Exception {
        if (checkpointStorageLocation == null) {
            CheckpointStorageAccess checkpointStorageAccess =
                    getCheckpointStorage().createCheckpointStorage(new JobID());
            checkpointStorageAccess.initializeBaseLocationsForCheckpoint();
            env.setCheckpointStorageAccess(checkpointStorageAccess);
            checkpointStorageLocation = checkpointStorageAccess.initializeLocationForCheckpoint(1L);
        }
        return checkpointStorageLocation;
    }

    // -------------------------------------------------------------------------------
    //  Keyed state backend utilities
    // -------------------------------------------------------------------------------

    private <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer) throws Exception {
        return createKeyedBackend(keySerializer, env);
    }

    private <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, Environment env) throws Exception {
        return createKeyedBackend(keySerializer, 10, new KeyGroupRange(0, 9), env);
    }

    private <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {
        CheckpointableKeyedStateBackend<K> backend =
                getStateBackend()
                        .createKeyedStateBackend(
                                env,
                                new JobID(),
                                "test_op",
                                keySerializer,
                                numberOfKeyGroups,
                                keyGroupRange,
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry());
        return backend;
    }

    private <K> CheckpointableKeyedStateBackend<K> restoreKeyedBackend(
            TypeSerializer<K> keySerializer, KeyedStateHandle state) throws Exception {
        return restoreKeyedBackend(keySerializer, state, env);
    }

    private <K> CheckpointableKeyedStateBackend<K> restoreKeyedBackend(
            TypeSerializer<K> keySerializer, KeyedStateHandle state, Environment env)
            throws Exception {
        return restoreKeyedBackend(
                keySerializer, 10, new KeyGroupRange(0, 9), Collections.singletonList(state), env);
    }

    private <K> CheckpointableKeyedStateBackend<K> restoreKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            List<KeyedStateHandle> state,
            Environment env)
            throws Exception {
        CheckpointableKeyedStateBackend<K> backend =
                getStateBackend()
                        .createKeyedStateBackend(
                                env,
                                new JobID(),
                                "test_op",
                                keySerializer,
                                numberOfKeyGroups,
                                keyGroupRange,
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                state,
                                new CloseableRegistry());
        return backend;
    }

    private KeyedStateHandle runSnapshot(
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

    // -------------------------------------------------------------------------------
    //  Operator state backend utilities
    // -------------------------------------------------------------------------------

    private OperatorStateBackend createOperatorStateBackend() throws Exception {
        return getStateBackend()
                .createOperatorStateBackend(
                        env, "test_op", Collections.emptyList(), new CloseableRegistry());
    }

    private OperatorStateBackend createOperatorStateBackend(Collection<OperatorStateHandle> state)
            throws Exception {
        return getStateBackend()
                .createOperatorStateBackend(env, "test_op", state, new CloseableRegistry());
    }

    private OperatorStateBackend restoreOperatorStateBackend(OperatorStateHandle state)
            throws Exception {
        OperatorStateBackend operatorStateBackend =
                createOperatorStateBackend(StateObjectCollection.singleton(state));
        return operatorStateBackend;
    }

    private OperatorStateHandle runSnapshot(
            RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshotRunnableFuture)
            throws Exception {

        if (!snapshotRunnableFuture.isDone()) {
            snapshotRunnableFuture.run();
        }

        return snapshotRunnableFuture.get().getJobManagerOwnedSnapshot();
    }
}
