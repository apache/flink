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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateRegistryListener;
import org.apache.flink.runtime.state.heap.AbstractHeapState;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the {@link KeyedStateBackend} and {@link OperatorStateBackend} as produced by various
 * {@link StateBackend}s.
 *
 * <p>NOTE: Please ensure to close and dispose any created keyed state backend in tests.
 */
@SuppressWarnings("serial")
public abstract class StateBackendTestBase<B extends AbstractStateBackend> {

    @BeforeEach
    void before() throws Exception {
        env = buildMockEnv();
    }

    @AfterEach
    void after() {
        IOUtils.closeQuietly(env);
    }

    // lazily initialized stream storage
    private CheckpointStreamFactory checkpointStreamFactory;

    protected MockEnvironment env;

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

    protected abstract boolean isSerializerPresenceRequiredOnRestore();

    protected abstract boolean supportsAsynchronousSnapshots();

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

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer) throws Exception {
        return createKeyedBackend(keySerializer, env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, Environment env) throws Exception {
        return createKeyedBackend(keySerializer, 10, new KeyGroupRange(0, 9), env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        env.setCheckpointStorageAccess(getCheckpointStorageAccess());
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

    protected <K> CheckpointableKeyedStateBackend<K> restoreKeyedBackend(
            TypeSerializer<K> keySerializer, KeyedStateHandle state) throws Exception {
        return restoreKeyedBackend(keySerializer, state, env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> restoreKeyedBackend(
            TypeSerializer<K> keySerializer, KeyedStateHandle state, Environment env)
            throws Exception {
        return restoreKeyedBackend(
                keySerializer, 10, new KeyGroupRange(0, 9), Collections.singletonList(state), env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> restoreKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            List<KeyedStateHandle> state,
            Environment env)
            throws Exception {

        return getStateBackend()
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
    }

    @TestTemplate
    void testEnableStateLatencyTracking() throws Exception {
        ConfigurableStateBackend stateBackend = getStateBackend();
        Configuration config = new Configuration();
        config.setBoolean(StateBackendOptions.LATENCY_TRACK_ENABLED, true);
        StateBackend configuredBackend =
                stateBackend.configure(config, Thread.currentThread().getContextClassLoader());
        KeyGroupRange groupRange = new KeyGroupRange(0, 1);
        CheckpointableKeyedStateBackend<Integer> keyedStateBackend =
                configuredBackend.createKeyedStateBackend(
                        env,
                        new JobID(),
                        "test_op",
                        IntSerializer.INSTANCE,
                        groupRange.getNumberOfKeyGroups(),
                        groupRange,
                        env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        new CloseableRegistry());
        try {
            KeyedStateBackend<Integer> nested =
                    keyedStateBackend instanceof TestableKeyedStateBackend
                            ? ((TestableKeyedStateBackend<Integer>) keyedStateBackend)
                                    .getDelegatedKeyedStateBackend(true)
                            : keyedStateBackend;
            assertThat(
                            ((AbstractKeyedStateBackend<Integer>) nested)
                                    .getLatencyTrackingStateConfig()
                                    .isEnabled())
                    .isTrue();
        } finally {
            IOUtils.closeQuietly(keyedStateBackend);
            keyedStateBackend.dispose();
        }
    }

    @TestTemplate
    void testIsSafeToReuseState() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            assertThat(backend.isSafeToReuseKVState()).isEqualTo(isSafeToReuseKVState());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
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
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
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
            assertThat(priorityQueue.getSubsetForKeyGroup(1))
                    .containsExactlyInAnyOrder(elementA42, elementA44);
            assertThat(priorityQueue.getSubsetForKeyGroup(8))
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
    void testGetKeys() throws Exception {
        final int namespace1ElementsNum = 1000;
        final int namespace2ElementsNum = 1000;
        String fieldName = "get-keys-test";
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            final String ns1 = "ns1";
            ValueState<Integer> keyedState1 =
                    backend.getPartitionedState(
                            ns1,
                            StringSerializer.INSTANCE,
                            new ValueStateDescriptor<>(fieldName, IntSerializer.INSTANCE));

            for (int key = 0; key < namespace1ElementsNum; key++) {
                backend.setCurrentKey(key);
                keyedState1.update(key * 2);
            }

            final String ns2 = "ns2";
            ValueState<Integer> keyedState2 =
                    backend.getPartitionedState(
                            ns2,
                            StringSerializer.INSTANCE,
                            new ValueStateDescriptor<>(fieldName, IntSerializer.INSTANCE));

            for (int key = namespace1ElementsNum;
                    key < namespace1ElementsNum + namespace2ElementsNum;
                    key++) {
                backend.setCurrentKey(key);
                keyedState2.update(key * 2);
            }

            // valid for namespace1
            try (Stream<Integer> keysStream = backend.getKeys(fieldName, ns1).sorted()) {
                PrimitiveIterator.OfInt actualIterator =
                        keysStream.mapToInt(value -> value.intValue()).iterator();

                for (int expectedKey = 0; expectedKey < namespace1ElementsNum; expectedKey++) {
                    assertThat(actualIterator).hasNext();
                    assertThat(actualIterator.nextInt()).isEqualTo(expectedKey);
                }

                assertThat(actualIterator).isExhausted();
            }

            // valid for namespace2
            try (Stream<Integer> keysStream = backend.getKeys(fieldName, ns2).sorted()) {
                PrimitiveIterator.OfInt actualIterator =
                        keysStream.mapToInt(value -> value.intValue()).iterator();

                for (int expectedKey = namespace1ElementsNum;
                        expectedKey < namespace1ElementsNum + namespace2ElementsNum;
                        expectedKey++) {
                    assertThat(actualIterator).hasNext();
                    assertThat(actualIterator.nextInt()).isEqualTo(expectedKey);
                }

                assertThat(actualIterator).isExhausted();
            }
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testGetKeysAndNamespaces() throws Exception {
        final int elementsNum = 1000;
        String fieldName = "get-keys-test";
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            String[] namespaces = new String[] {"ns1", "ns2"};
            InternalValueState<Integer, String, Integer> keyedState =
                    backend.createOrUpdateInternalState(
                            StringSerializer.INSTANCE,
                            new ValueStateDescriptor<>(fieldName, IntSerializer.INSTANCE));

            for (int key = 0; key < elementsNum; key++) {
                backend.setCurrentKey(key);
                for (String ns : namespaces) {
                    keyedState.setCurrentNamespace(ns);
                    keyedState.update(key * 2);
                }
            }

            try (Stream<Tuple2<Integer, String>> stream = backend.getKeysAndNamespaces(fieldName)) {
                final Map<String, Set<Integer>> keysByNamespace = new HashMap<>();
                stream.forEach(
                        entry -> {
                            assertThat(entry.f1)
                                    .withFailMessage("Unexpected namespace")
                                    .isIn(namespaces);
                            assertThat(entry.f0)
                                    .withFailMessage("Unexpected key")
                                    .isGreaterThanOrEqualTo(0)
                                    .isLessThan(elementsNum);

                            Set<Integer> keys =
                                    keysByNamespace.computeIfAbsent(entry.f1, k -> new HashSet<>());
                            assertThat(keys.add(entry.f0))
                                    .withFailMessage("Duplicate key for namespace")
                                    .isTrue();
                        });
                assertThat(keysByNamespace.size())
                        .withFailMessage("Unexpected namespaces count")
                        .isEqualTo(namespaces.length);
            }

            // make snapshot
            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    createStreamFactory(),
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            new SharedStateRegistryImpl());

            IOUtils.closeQuietly(backend);
            backend.dispose();

            // restore backend
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

            try (Stream<Tuple2<Integer, String>> stream = backend.getKeysAndNamespaces(fieldName)) {
                final Map<String, Set<Integer>> keysByNamespace = new HashMap<>();
                stream.forEach(
                        entry -> {
                            assertThat(entry.f1)
                                    .withFailMessage("Unexpected namespace")
                                    .isIn(namespaces);
                            assertThat(entry.f0)
                                    .withFailMessage("Unexpected key")
                                    .isGreaterThanOrEqualTo(0)
                                    .isLessThan(elementsNum);

                            Set<Integer> keys =
                                    keysByNamespace.computeIfAbsent(entry.f1, k -> new HashSet<>());
                            assertThat(keys.add(entry.f0))
                                    .withFailMessage("Duplicate key for namespace")
                                    .isTrue();
                        });
                assertThat(keysByNamespace.size())
                        .withFailMessage("Unexpected namespaces count")
                        .isEqualTo(namespaces.length);
            }
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testBackendUsesRegisteredKryoDefaultSerializer() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);

        try {
            // cast because our test serializer is not typed to TestPojo
            env.getExecutionConfig()
                    .addDefaultKryoSerializer(
                            TestPojo.class, (Class) ExceptionThrowingTestSerializer.class);

            TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

            // make sure that we are in fact using the KryoSerializer
            assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                    .isInstanceOf(KryoSerializer.class);

            ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // we will be expecting ExpectedKryoTestException to be thrown,
            // because the ExceptionThrowingTestSerializer should be used
            int numExceptions = 0;

            backend.setCurrentKey(1);

            try {
                // backends that eagerly serializes (such as RocksDB) will fail here
                state.update(new TestPojo("u1", 1));
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            try {
                // backends that lazily serializes (such as memory state backend) will fail here
                runSnapshot(
                        backend.snapshot(
                                682375462378L,
                                2,
                                streamFactory,
                                CheckpointOptions.forCheckpointWithDefaultLocation()),
                        sharedStateRegistry);
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            assertThat(numExceptions)
                    .withFailMessage("Didn't see the expected Kryo exception.")
                    .isGreaterThan(0);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testBackendUsesRegisteredKryoDefaultSerializerUsingGetOrCreate() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);

        try {
            // cast because our test serializer is not typed to TestPojo
            env.getExecutionConfig()
                    .addDefaultKryoSerializer(
                            TestPojo.class, (Class) ExceptionThrowingTestSerializer.class);

            TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

            // make sure that we are in fact using the KryoSerializer
            assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                    .isInstanceOf(KryoSerializer.class);

            pojoType.createSerializer(env.getExecutionConfig());

            ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

            ValueState<TestPojo> state =
                    backend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, kvId);
            assertThat(state).isInstanceOf(InternalValueState.class);
            ((InternalValueState) state).setCurrentNamespace(VoidNamespace.INSTANCE);

            // we will be expecting ExpectedKryoTestException to be thrown,
            // because the ExceptionThrowingTestSerializer should be used
            int numExceptions = 0;

            backend.setCurrentKey(1);

            try {
                // backends that eagerly serializes (such as RocksDB) will fail here
                state.update(new TestPojo("u1", 1));
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            try {
                // backends that lazily serializes (such as memory state backend) will fail here
                runSnapshot(
                        backend.snapshot(
                                682375462378L,
                                2,
                                streamFactory,
                                CheckpointOptions.forCheckpointWithDefaultLocation()),
                        sharedStateRegistry);
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            assertThat(numExceptions)
                    .withFailMessage("Didn't see the expected Kryo exception.")
                    .isGreaterThan(0);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testBackendUsesRegisteredKryoSerializer() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        env.getExecutionConfig()
                .registerTypeWithKryoSerializer(
                        TestPojo.class, ExceptionThrowingTestSerializer.class);

        TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

        // make sure that we are in fact using the KryoSerializer
        assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                .isInstanceOf(KryoSerializer.class);

        ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);
        try {
            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // we will be expecting ExpectedKryoTestException to be thrown,
            // because the ExceptionThrowingTestSerializer should be used
            int numExceptions = 0;

            backend.setCurrentKey(1);

            try {
                // backends that eagerly serializes (such as RocksDB) will fail here
                state.update(new TestPojo("u1", 1));
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            try {
                // backends that lazily serializes (such as memory state backend) will fail here
                runSnapshot(
                        backend.snapshot(
                                682375462378L,
                                2,
                                streamFactory,
                                CheckpointOptions.forCheckpointWithDefaultLocation()),
                        sharedStateRegistry);
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            assertThat(numExceptions)
                    .withFailMessage("Didn't see the expected Kryo exception.")
                    .isGreaterThan(0);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testBackendUsesRegisteredKryoSerializerUsingGetOrCreate() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        env.getExecutionConfig()
                .registerTypeWithKryoSerializer(
                        TestPojo.class, ExceptionThrowingTestSerializer.class);

        TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

        // make sure that we are in fact using the KryoSerializer
        assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                .isInstanceOf(KryoSerializer.class);

        ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);
        try {
            ValueState<TestPojo> state =
                    backend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, kvId);
            assertThat(state).isInstanceOf(InternalValueState.class);
            ((InternalValueState) state).setCurrentNamespace(VoidNamespace.INSTANCE);

            // we will be expecting ExpectedKryoTestException to be thrown,
            // because the ExceptionThrowingTestSerializer should be used
            int numExceptions = 0;

            backend.setCurrentKey(1);

            try {
                // backends that eagerly serializes (such as RocksDB) will fail here
                state.update(new TestPojo("u1", 1));
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            try {
                // backends that lazily serializes (such as memory state backend) will fail here
                runSnapshot(
                        backend.snapshot(
                                682375462378L,
                                2,
                                streamFactory,
                                CheckpointOptions.forCheckpointWithDefaultLocation()),
                        sharedStateRegistry);
            } catch (ExpectedKryoTestException e) {
                numExceptions++;
            } catch (Exception e) {
                if (e.getCause() instanceof ExpectedKryoTestException) {
                    numExceptions++;
                } else {
                    throw e;
                }
            }

            assertThat(numExceptions)
                    .withFailMessage("Didn't see the expected Kryo exception.")
                    .isGreaterThan(0);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * Verify state restore resilience when: - snapshot was taken without any Kryo registrations,
     * specific serializers or default serializers for the state type - restored with the state type
     * registered (no specific serializer)
     *
     * <p>This test should not fail, because de- / serialization of the state should not be
     * performed with Kryo's default {@link com.esotericsoftware.kryo.serializers.FieldSerializer}.
     */
    @TestTemplate
    void testKryoRegisteringRestoreResilienceWithRegisteredType() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

        // make sure that we are in fact using the KryoSerializer
        assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                .isInstanceOf(KryoSerializer.class);

        ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);
        try {
            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // ============== create snapshot - no Kryo registration or specific / default
            // serializers
            // ==============

            // make some more modifications
            backend.setCurrentKey(1);
            state.update(new TestPojo("u1", 1));

            backend.setCurrentKey(2);
            state.update(new TestPojo("u2", 2));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            IOUtils.closeQuietly(backend);
            backend.dispose();

            // ====================================== restore snapshot
            // ======================================

            env.getExecutionConfig().registerKryoType(TestPojo.class);

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

            snapshot.discardState();

            state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            backend.setCurrentKey(1);
            assertThat(state.value()).isEqualTo(new TestPojo("u1", 1));

            backend.setCurrentKey(2);
            assertThat(state.value()).isEqualTo(new TestPojo("u2", 2));
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * Verify state restore resilience when: - snapshot was taken without any Kryo registrations,
     * specific serializers or default serializers for the state type - restored with a default
     * serializer for the state type
     *
     * <p>The default serializer used on restore is {@link CustomKryoTestSerializer}, which
     * deliberately fails only on deserialization. We use the deliberate deserialization failure to
     * acknowledge test success.
     *
     * @throws Exception expects {@link ExpectedKryoTestException} to be thrown.
     */
    @TestTemplate
    @SuppressWarnings("unchecked")
    void testKryoRegisteringRestoreResilienceWithDefaultSerializer() throws Exception {
        assumeThat(supportsMetaInfoVerification()).isTrue();
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);
        try {
            TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

            // make sure that we are in fact using the KryoSerializer
            assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                    .isInstanceOf(KryoSerializer.class);

            ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // ============== create snapshot - no Kryo registration or specific / default
            // serializers ==============

            // make some more modifications
            backend.setCurrentKey(1);
            state.update(new TestPojo("u1", 1));

            backend.setCurrentKey(2);
            state.update(new TestPojo("u2", 2));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();

            // ========== restore snapshot - should use default serializer (ONLY SERIALIZATION)
            // ==========

            // cast because our test serializer is not typed to TestPojo
            env.getExecutionConfig()
                    .addDefaultKryoSerializer(
                            TestPojo.class, (Class) CustomKryoTestSerializer.class);

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

            // re-initialize to ensure that we create the KryoSerializer from scratch, otherwise
            // initializeSerializerUnlessSet would not pick up our new config
            kvId = new ValueStateDescriptor<>("id", pojoType);
            state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);

            // update to test state backends that eagerly serialize, such as RocksDB
            state.update(new TestPojo("u1", 11));

            KeyedStateHandle snapshot2 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            snapshot.discardState();

            backend.dispose();

            // ========= restore snapshot - should use default serializer (FAIL ON DESERIALIZATION)
            // =========

            // cast because our test serializer is not typed to TestPojo
            env.getExecutionConfig()
                    .addDefaultKryoSerializer(
                            TestPojo.class, (Class) CustomKryoTestSerializer.class);

            assertRestoreKeyedBackendFail(snapshot2, kvId);

            snapshot2.discardState();
        } finally {
            // ensure to release native resources even when we exit through exception
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * Verify state restore resilience when: - snapshot was taken without any Kryo registrations,
     * specific serializers or default serializers for the state type - restored with a specific
     * serializer for the state type
     *
     * <p>The specific serializer used on restore is {@link CustomKryoTestSerializer}, which
     * deliberately fails only on deserialization. We use the deliberate deserialization failure to
     * acknowledge test success.
     *
     * @throws Exception expects {@link ExpectedKryoTestException} to be thrown.
     */
    @TestTemplate
    void testKryoRegisteringRestoreResilienceWithRegisteredSerializer() throws Exception {
        assumeThat(supportsMetaInfoVerification()).isTrue();
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        CheckpointableKeyedStateBackend<Integer> backend = null;

        try {
            backend = createKeyedBackend(IntSerializer.INSTANCE, env);

            TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

            // make sure that we are in fact using the KryoSerializer
            assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                    .isInstanceOf(KryoSerializer.class);

            ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);
            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // ============== create snapshot - no Kryo registration or specific / default
            // serializers ==============

            // make some more modifications
            backend.setCurrentKey(1);
            state.update(new TestPojo("u1", 1));

            backend.setCurrentKey(2);
            state.update(new TestPojo("u2", 2));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();

            // ========== restore snapshot - should use specific serializer (ONLY SERIALIZATION)
            // ==========

            env.getExecutionConfig()
                    .registerTypeWithKryoSerializer(TestPojo.class, CustomKryoTestSerializer.class);

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

            // re-initialize to ensure that we create the KryoSerializer from scratch, otherwise
            // initializeSerializerUnlessSet would not pick up our new config
            kvId = new ValueStateDescriptor<>("id", pojoType);
            state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);

            // update to test state backends that eagerly serialize, such as RocksDB
            state.update(new TestPojo("u1", 11));

            KeyedStateHandle snapshot2 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            snapshot.discardState();

            backend.dispose();

            // ========= restore snapshot - should use specific serializer (FAIL ON DESERIALIZATION)
            // =========

            env.getExecutionConfig()
                    .registerTypeWithKryoSerializer(TestPojo.class, CustomKryoTestSerializer.class);

            assertRestoreKeyedBackendFail(snapshot2, kvId);
        } finally {
            // ensure that native resources are also released in case of exception
            if (backend != null) {
                backend.dispose();
            }
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testKryoRestoreResilienceWithDifferentRegistrationOrder() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        // register A first then B
        env.getExecutionConfig().registerKryoType(TestNestedPojoClassA.class);
        env.getExecutionConfig().registerKryoType(TestNestedPojoClassB.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);

        try {

            TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

            // make sure that we are in fact using the KryoSerializer
            assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                    .isInstanceOf(KryoSerializer.class);

            ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);
            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // access the internal state representation to retrieve the original Kryo registration
            // ids;
            // these will be later used to check that on restore, the new Kryo serializer has
            // reconfigured itself to
            // have identical mappings
            InternalKvState internalKvState = (InternalKvState) state;
            KryoSerializer<TestPojo> kryoSerializer =
                    (KryoSerializer<TestPojo>) internalKvState.getValueSerializer();
            int mainPojoClassRegistrationId =
                    kryoSerializer.getKryo().getRegistration(TestPojo.class).getId();
            int nestedPojoClassARegistrationId =
                    kryoSerializer.getKryo().getRegistration(TestNestedPojoClassA.class).getId();
            int nestedPojoClassBRegistrationId =
                    kryoSerializer.getKryo().getRegistration(TestNestedPojoClassB.class).getId();

            // ============== create snapshot of current configuration ==============

            // make some more modifications
            backend.setCurrentKey(1);
            state.update(
                    new TestPojo(
                            "u1",
                            1,
                            new TestNestedPojoClassA(1.0, 2),
                            new TestNestedPojoClassB(2.3, "foo")));

            backend.setCurrentKey(2);
            state.update(
                    new TestPojo(
                            "u2",
                            2,
                            new TestNestedPojoClassA(2.0, 5),
                            new TestNestedPojoClassB(3.1, "bar")));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();

            // ========== restore snapshot, with a different registration order in the configuration
            // ==========

            env.close();
            env = buildMockEnv();

            env.getExecutionConfig()
                    .registerKryoType(TestNestedPojoClassB.class); // this time register B first
            env.getExecutionConfig().registerKryoType(TestNestedPojoClassA.class);

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

            // re-initialize to ensure that we create the KryoSerializer from scratch, otherwise
            // initializeSerializerUnlessSet would not pick up our new config
            kvId = new ValueStateDescriptor<>("id", pojoType);
            state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // verify that on restore, the serializer that the state handle uses has reconfigured
            // itself to have
            // identical Kryo registration ids compared to the previous execution
            internalKvState = (InternalKvState) state;
            kryoSerializer = (KryoSerializer<TestPojo>) internalKvState.getValueSerializer();
            assertThat(kryoSerializer.getKryo().getRegistration(TestPojo.class).getId())
                    .isEqualTo(mainPojoClassRegistrationId);
            assertThat(kryoSerializer.getKryo().getRegistration(TestNestedPojoClassA.class).getId())
                    .isEqualTo(nestedPojoClassARegistrationId);
            assertThat(kryoSerializer.getKryo().getRegistration(TestNestedPojoClassB.class).getId())
                    .isEqualTo(nestedPojoClassBRegistrationId);

            backend.setCurrentKey(1);

            // update to test state backends that eagerly serialize, such as RocksDB
            state.update(
                    new TestPojo(
                            "u1",
                            11,
                            new TestNestedPojoClassA(22.1, 12),
                            new TestNestedPojoClassB(1.23, "foobar")));

            // this tests backends that lazily serialize, such as memory state backend
            runSnapshot(
                    backend.snapshot(
                            682375462378L,
                            2,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation()),
                    sharedStateRegistry);

            snapshot.discardState();
        } finally {
            backend.dispose();
        }
    }

    @TestTemplate
    void testPojoRestoreResilienceWithDifferentRegistrationOrder() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        // register A first then B
        env.getExecutionConfig().registerPojoType(TestNestedPojoClassA.class);
        env.getExecutionConfig().registerPojoType(TestNestedPojoClassB.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);

        try {

            TypeInformation<TestPojo> pojoType = TypeExtractor.getForClass(TestPojo.class);

            // make sure that we are in fact using the PojoSerializer
            assertThat(pojoType.createSerializer(env.getExecutionConfig()))
                    .isInstanceOf(PojoSerializer.class);

            ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);
            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // ============== create snapshot of current configuration ==============

            // make some more modifications
            backend.setCurrentKey(1);
            state.update(
                    new TestPojo(
                            "u1",
                            1,
                            new TestNestedPojoClassA(1.0, 2),
                            new TestNestedPojoClassB(2.3, "foo")));

            backend.setCurrentKey(2);
            state.update(
                    new TestPojo(
                            "u2",
                            2,
                            new TestNestedPojoClassA(2.0, 5),
                            new TestNestedPojoClassB(3.1, "bar")));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();

            // ========== restore snapshot, with a different registration order in the configuration
            // ==========

            env.close();
            env = buildMockEnv();

            env.getExecutionConfig()
                    .registerPojoType(TestNestedPojoClassB.class); // this time register B first
            env.getExecutionConfig().registerPojoType(TestNestedPojoClassA.class);

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

            // re-initialize to ensure that we create the PojoSerializer from scratch, otherwise
            // initializeSerializerUnlessSet would not pick up our new config
            kvId = new ValueStateDescriptor<>("id", pojoType);
            state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);

            // update to test state backends that eagerly serialize, such as RocksDB
            state.update(
                    new TestPojo(
                            "u1",
                            11,
                            new TestNestedPojoClassA(22.1, 12),
                            new TestNestedPojoClassB(1.23, "foobar")));

            // this tests backends that lazily serialize, such as memory state backend
            runSnapshot(
                    backend.snapshot(
                            682375462378L,
                            2,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation()),
                    sharedStateRegistry);

            snapshot.discardState();
        } finally {
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testValueState() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> kvState =
                    (InternalKvState<Integer, VoidNamespace, String>) state;

            // this is only available after the backend initialized the serializer
            TypeSerializer<String> valueSerializer = kvId.getSerializer();

            // some modifications to the state
            backend.setCurrentKey(1);
            assertThat(state.value()).isNull();
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isNull();
            state.update("1");
            backend.setCurrentKey(2);
            assertThat(state.value()).isNull();
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isNull();
            state.update("2");
            backend.setCurrentKey(1);
            assertThat(state.value()).isEqualTo("1");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("1");

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // make some more modifications
            backend.setCurrentKey(1);
            state.update("u1");
            backend.setCurrentKey(2);
            state.update("u2");
            backend.setCurrentKey(3);
            state.update("u3");

            // draw another snapshot
            KeyedStateHandle snapshot2 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462379L,
                                    4,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // validate the original state
            backend.setCurrentKey(1);
            assertThat(state.value()).isEqualTo("u1");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u1");
            backend.setCurrentKey(2);
            assertThat(state.value()).isEqualTo("u2");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u2");
            backend.setCurrentKey(3);
            assertThat(state.value()).isEqualTo("u3");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    3,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u3");

            backend.dispose();
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

            snapshot1.discardState();

            ValueState<String> restored1 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> restoredKvState1 =
                    (InternalKvState<Integer, VoidNamespace, String>) restored1;

            backend.setCurrentKey(1);
            assertThat(restored1.value()).isEqualTo("1");
            assertThat(
                            getSerializedValue(
                                    restoredKvState1,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("1");
            backend.setCurrentKey(2);
            assertThat(restored1.value()).isEqualTo("2");
            assertThat(
                            getSerializedValue(
                                    restoredKvState1,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("2");

            backend.dispose();
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);

            snapshot2.discardState();

            ValueState<String> restored2 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> restoredKvState2 =
                    (InternalKvState<Integer, VoidNamespace, String>) restored2;

            backend.setCurrentKey(1);
            assertThat(restored2.value()).isEqualTo("u1");
            assertThat(
                            getSerializedValue(
                                    restoredKvState2,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u1");
            backend.setCurrentKey(2);
            assertThat(restored2.value()).isEqualTo("u2");
            assertThat(
                            getSerializedValue(
                                    restoredKvState2,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u2");
            backend.setCurrentKey(3);
            assertThat(restored2.value()).isEqualTo("u3");
            assertThat(
                            getSerializedValue(
                                    restoredKvState2,
                                    3,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u3");
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testValueStateWorkWithTtl() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueStateDescriptor<MutableLong> kvId =
                    new ValueStateDescriptor<>("id", MutableLong.class);
            kvId.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(1)).build());

            ValueState<MutableLong> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            backend.setCurrentKey(1);
            state.update(new MutableLong());
            state.value();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * Tests {@link ValueState#value()} and {@link InternalKvState#getSerializedValue(byte[],
     * TypeSerializer, TypeSerializer, TypeSerializer)} accessing the state concurrently. They
     * should not get in the way of each other.
     */
    @TestTemplate
    @SuppressWarnings("unchecked")
    void testValueStateRace() throws Exception {
        final Integer namespace = 1;

        final ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        final TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        final TypeSerializer<Integer> namespaceSerializer = IntSerializer.INSTANCE;

        final CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            final ValueState<String> state =
                    backend.getPartitionedState(namespace, IntSerializer.INSTANCE, kvId);

            // this is only available after the backend initialized the serializer
            final TypeSerializer<String> valueSerializer = kvId.getSerializer();

            @SuppressWarnings("unchecked")
            final InternalKvState<Integer, Integer, String> kvState =
                    (InternalKvState<Integer, Integer, String>) state;

            /**
             * 1) Test that ValueState#value() before and after KvState#getSerializedValue(byte[])
             * return the same value.
             */

            // set some key and namespace
            final int key1 = 1;
            backend.setCurrentKey(key1);
            kvState.setCurrentNamespace(2);
            state.update("2");
            assertThat(state.value()).isEqualTo("2");

            // query another key and namespace
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    3,
                                    keySerializer,
                                    namespace,
                                    IntSerializer.INSTANCE,
                                    valueSerializer))
                    .isNull();

            // the state should not have changed!
            assertThat(state.value()).isEqualTo("2");

            // re-set values
            kvState.setCurrentNamespace(namespace);

            /**
             * 2) Test two threads concurrently using ValueState#value() and
             * KvState#getSerializedValue(byte[]).
             */

            // some modifications to the state
            final int key2 = 10;
            backend.setCurrentKey(key2);
            assertThat(state.value()).isNull();
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    key2,
                                    keySerializer,
                                    namespace,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isNull();
            state.update("1");

            final CheckedThread getter =
                    new CheckedThread("State getter") {
                        @Override
                        public void go() throws Exception {
                            while (!isInterrupted()) {
                                assertThat(state.value()).isEqualTo("1");
                            }
                        }
                    };

            final CheckedThread serializedGetter =
                    new CheckedThread("Serialized state getter") {
                        @Override
                        public void go() throws Exception {
                            while (!isInterrupted() && getter.isAlive()) {
                                final String serializedValue =
                                        getSerializedValue(
                                                kvState,
                                                key2,
                                                keySerializer,
                                                namespace,
                                                namespaceSerializer,
                                                valueSerializer);
                                assertThat(serializedValue).isEqualTo("1");
                            }
                        }
                    };

            getter.start();
            serializedGetter.start();

            // run both threads for max 100ms
            Timer t = new Timer("stopper");
            t.schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            getter.interrupt();
                            serializedGetter.interrupt();
                            this.cancel();
                        }
                    },
                    100);

            // wait for both threads to finish
            // serializedGetter will finish if its assertion fails or if
            // getter is not alive any more
            serializedGetter.sync();
            // if serializedGetter crashed, getter will not know -> interrupt just in case
            getter.interrupt();
            getter.sync();
            t.cancel(); // if not executed yet
        } finally {
            // clean up
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testMultipleValueStates() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        ValueStateDescriptor<String> desc1 =
                new ValueStateDescriptor<>("a-string", StringSerializer.INSTANCE);
        ValueStateDescriptor<Integer> desc2 =
                new ValueStateDescriptor<>("an-integer", IntSerializer.INSTANCE);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, 1, new KeyGroupRange(0, 0), env);
        try {
            ValueState<String> state1 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc1);
            ValueState<Integer> state2 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc2);

            // some modifications to the state
            backend.setCurrentKey(1);
            assertThat(state1.value()).isNull();
            assertThat(state2.value()).isNull();
            state1.update("1");

            // state2 should still have nothing
            assertThat(state1.value()).isEqualTo("1");
            assertThat(state2.value()).isNull();
            state2.update(13);

            // both have some state now
            assertThat(state1.value()).isEqualTo("1");
            assertThat(state2.value()).isEqualTo(13);

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();
            backend =
                    restoreKeyedBackend(
                            IntSerializer.INSTANCE,
                            1,
                            new KeyGroupRange(0, 0),
                            Collections.singletonList(snapshot1),
                            env);

            snapshot1.discardState();

            backend.setCurrentKey(1);

            state1 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc1);
            state2 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc2);

            // verify that they are still the same
            assertThat(state1.value()).isEqualTo("1");
            assertThat(state2.value()).isEqualTo(13);

        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * This test verifies that passing {@code null} to {@link ValueState#update(Object)} acts the
     * same as {@link ValueState#clear()}.
     *
     * @throws Exception
     */
    @TestTemplate
    @SuppressWarnings("unchecked")
    void testValueStateNullUpdate() throws Exception {
        // precondition: LongSerializer must fail on null value. this way the test would fail
        // later if null values where actually stored in the state instead of acting as clear()
        assertThatThrownBy(
                        () ->
                                LongSerializer.INSTANCE.serialize(
                                        null,
                                        new DataOutputViewStreamWrapper(
                                                new ByteArrayOutputStream())))
                .isInstanceOf(NullPointerException.class);

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueStateDescriptor<Long> kvId =
                    new ValueStateDescriptor<>("id", LongSerializer.INSTANCE, 42L);

            ValueState<Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // some modifications to the state
            backend.setCurrentKey(1);

            // verify default value
            assertThat(state.value()).isEqualTo(42L);
            state.update(1L);
            assertThat(state.value()).isOne();

            backend.setCurrentKey(2);
            assertThat(state.value()).isEqualTo(42L);

            backend.setCurrentKey(1);
            state.clear();
            assertThat(state.value()).isEqualTo(42L);

            state.update(17L);
            assertThat(state.value()).isEqualTo(17L);

            state.update(null);
            assertThat(state.value()).isEqualTo(42L);

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

            snapshot1.discardState();

            backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked,rawtypes")
    void testListState() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ListState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> kvState =
                    (InternalKvState<Integer, VoidNamespace, String>) state;

            // this is only available after the backend initialized the serializer
            TypeSerializer<String> valueSerializer = kvId.getElementSerializer();

            Joiner joiner = Joiner.on(",");

            // some modifications to the state
            backend.setCurrentKey(1);
            assertThat(state.get()).isNull();
            assertThat(
                            getSerializedList(
                                    kvState,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isNull();
            state.add("1");

            backend.setCurrentKey(2);
            assertThat(state.get()).isNull();
            assertThat(
                            getSerializedList(
                                    kvState,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isNull();
            state.update(Arrays.asList("2"));

            backend.setCurrentKey(1);
            assertThat(joiner.join(state.get())).isEqualTo("1");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            kvState,
                                            1,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("1");

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // make some more modifications
            backend.setCurrentKey(1);
            state.add("u1");

            backend.setCurrentKey(2);
            state.add("u2");

            backend.setCurrentKey(3);
            state.add("u3");

            // draw another snapshot
            KeyedStateHandle snapshot2 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462379L,
                                    4,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // validate the original state
            backend.setCurrentKey(1);
            assertThat(joiner.join(state.get())).isEqualTo("1,u1");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            kvState,
                                            1,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("1,u1");

            backend.setCurrentKey(2);
            assertThat(joiner.join(state.get())).isEqualTo("2,u2");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            kvState,
                                            2,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("2,u2");

            backend.setCurrentKey(3);
            assertThat(joiner.join(state.get())).isEqualTo("u3");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            kvState,
                                            3,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("u3");

            backend.dispose();
            // restore the first snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
            snapshot1.discardState();

            ListState<String> restored1 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> restoredKvState1 =
                    (InternalKvState<Integer, VoidNamespace, String>) restored1;

            backend.setCurrentKey(1);
            assertThat(joiner.join(restored1.get())).isEqualTo("1");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            restoredKvState1,
                                            1,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("1");

            backend.setCurrentKey(2);
            assertThat(joiner.join(restored1.get())).isEqualTo("2");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            restoredKvState1,
                                            2,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("2");

            backend.dispose();
            // restore the second snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
            snapshot2.discardState();

            ListState<String> restored2 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> restoredKvState2 =
                    (InternalKvState<Integer, VoidNamespace, String>) restored2;

            backend.setCurrentKey(1);
            assertThat(joiner.join(restored2.get())).isEqualTo("1,u1");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            restoredKvState2,
                                            1,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("1,u1");

            backend.setCurrentKey(2);
            assertThat(joiner.join(restored2.get())).isEqualTo("2,u2");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            restoredKvState2,
                                            2,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("2,u2");

            backend.setCurrentKey(3);
            assertThat(joiner.join(restored2.get())).isEqualTo("u3");
            assertThat(
                            joiner.join(
                                    getSerializedList(
                                            restoredKvState2,
                                            3,
                                            keySerializer,
                                            VoidNamespace.INSTANCE,
                                            namespaceSerializer,
                                            valueSerializer)))
                    .isEqualTo("u3");
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing adding
     * {@code null}.
     */
    @TestTemplate
    void testListStateAddNull() throws Exception {
        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            assertThatThrownBy(() -> state.add(null)).isInstanceOf(NullPointerException.class);
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#addAll(List)} to be called with {@code null} entries in the list of entries to add.
     */
    @TestTemplate
    void testListStateAddAllNullEntries() throws Exception {
        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            List<Long> adding = new ArrayList<>();
            adding.add(3L);
            adding.add(null);
            adding.add(5L);

            assertThatThrownBy(() -> state.addAll(adding)).isInstanceOf(NullPointerException.class);
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#addAll(List)} to be called with {@code null}.
     */
    @TestTemplate
    void testListStateAddAllNull() throws Exception {
        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            assertThatThrownBy(() -> state.addAll(null)).isInstanceOf(NullPointerException.class);
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#addAll(List)} to be called with {@code null} entries in the list of entries to add.
     */
    @TestTemplate
    void testListStateUpdateNullEntries() throws Exception {
        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            List<Long> adding = new ArrayList<>();
            adding.add(3L);
            adding.add(null);
            adding.add(5L);

            assertThatThrownBy(() -> state.update(adding)).isInstanceOf(NullPointerException.class);
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#addAll(List)} to be called with {@code null}.
     */
    @TestTemplate
    void testListStateUpdateNull() throws Exception {
        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            assertThatThrownBy(() -> state.update(null)).isInstanceOf(NullPointerException.class);
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    void testListStateAPIs() throws Exception {
        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();
            state.add(17L);
            state.add(11L);
            assertThat(state.get()).containsExactlyInAnyOrder(17L, 11L);
            // update(emptyList) should remain the value null
            state.update(Collections.emptyList());
            assertThat(state.get()).isNull();
            state.update(Arrays.asList(10L, 16L));
            assertThat(state.get()).containsExactlyInAnyOrder(16L, 10L);
            assertThat(state.get()).containsExactlyInAnyOrder(16L, 10L);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isNull();
            assertThat(state.get()).isNull();
            state.addAll(Collections.emptyList());
            assertThat(state.get()).isNull();
            state.addAll(Arrays.asList(3L, 4L));
            assertThat(state.get()).containsExactlyInAnyOrder(3L, 4L);
            assertThat(state.get()).containsExactlyInAnyOrder(3L, 4L);
            state.addAll(new ArrayList<>());
            assertThat(state.get()).containsExactlyInAnyOrder(3L, 4L);
            state.addAll(Arrays.asList(5L, 6L));
            assertThat(state.get()).containsExactlyInAnyOrder(3L, 4L, 5L, 6L);
            state.addAll(new ArrayList<>());
            assertThat(state.get()).containsExactlyInAnyOrder(3L, 4L, 5L, 6L);

            assertThat(state.get()).containsExactlyInAnyOrder(3L, 4L, 5L, 6L);
            state.update(Arrays.asList(1L, 2L));
            assertThat(state.get()).containsExactlyInAnyOrder(1L, 2L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).containsExactlyInAnyOrder(10L, 16L);
            state.clear();
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).containsExactlyInAnyOrder(1L, 2L, 3L, 2L, 1L);
            state.update(Arrays.asList(5L, 6L));
            assertThat(state.get()).containsExactlyInAnyOrder(5L, 6L);
            state.clear();

            // make sure all lists / maps are cleared
            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    void testListStateMerging() throws Exception {
        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            InternalListState<String, Integer, Long> state =
                    (InternalListState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces
            //  - def spreads the values over two namespaces (one empty)
            //  - ghi is empty
            //  - jkl has all elements already in the target namespace
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("abc");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("ghi");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();

            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testReducingState() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        ReducingStateDescriptor<String> kvId =
                new ReducingStateDescriptor<>("id", new AppendingReduce(), String.class);

        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ReducingState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> kvState =
                    (InternalKvState<Integer, VoidNamespace, String>) state;

            // this is only available after the backend initialized the serializer
            TypeSerializer<String> valueSerializer = kvId.getSerializer();

            // some modifications to the state
            backend.setCurrentKey(1);
            assertThat(state.get()).isNull();
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isNull();
            state.add("1");
            backend.setCurrentKey(2);
            assertThat(state.get()).isNull();
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isNull();
            state.add("2");
            backend.setCurrentKey(1);
            assertThat(state.get()).isEqualTo("1");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("1");

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // make some more modifications
            backend.setCurrentKey(1);
            state.add("u1");
            backend.setCurrentKey(2);
            state.add("u2");
            backend.setCurrentKey(3);
            state.add("u3");

            // draw another snapshot
            KeyedStateHandle snapshot2 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462379L,
                                    4,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // validate the original state
            backend.setCurrentKey(1);
            assertThat(state.get()).isEqualTo("1,u1");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("1,u1");
            backend.setCurrentKey(2);
            assertThat(state.get()).isEqualTo("2,u2");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("2,u2");
            backend.setCurrentKey(3);
            assertThat(state.get()).isEqualTo("u3");
            assertThat(
                            getSerializedValue(
                                    kvState,
                                    3,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u3");

            backend.dispose();
            // restore the first snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
            snapshot1.discardState();

            ReducingState<String> restored1 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> restoredKvState1 =
                    (InternalKvState<Integer, VoidNamespace, String>) restored1;

            backend.setCurrentKey(1);
            assertThat(restored1.get()).isEqualTo("1");
            assertThat(
                            getSerializedValue(
                                    restoredKvState1,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("1");
            backend.setCurrentKey(2);
            assertThat(restored1.get()).isEqualTo("2");
            assertThat(
                            getSerializedValue(
                                    restoredKvState1,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("2");

            backend.dispose();
            // restore the second snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
            snapshot2.discardState();

            ReducingState<String> restored2 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> restoredKvState2 =
                    (InternalKvState<Integer, VoidNamespace, String>) restored2;

            backend.setCurrentKey(1);
            assertThat(restored2.get()).isEqualTo("1,u1");
            assertThat(
                            getSerializedValue(
                                    restoredKvState2,
                                    1,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("1,u1");
            backend.setCurrentKey(2);
            assertThat(restored2.get()).isEqualTo("2,u2");
            assertThat(
                            getSerializedValue(
                                    restoredKvState2,
                                    2,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("2,u2");
            backend.setCurrentKey(3);
            assertThat(restored2.get()).isEqualTo("u3");
            assertThat(
                            getSerializedValue(
                                    restoredKvState2,
                                    3,
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    valueSerializer))
                    .isEqualTo("u3");
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testReducingStateAddAndGet() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", (a, b) -> a + b, Long.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        try {
            ReducingState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();
            state.add(17L);
            state.add(11L);
            assertThat(state.get().longValue()).isEqualTo(28L);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isNull();
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get().longValue()).isEqualTo(28L);
            state.clear();
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get().longValue()).isEqualTo(9L);
            state.clear();

            // make sure all lists / maps are cleared
            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    void testReducingStateMerging() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", (a, b) -> a + b, Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        try {
            final InternalReducingState<String, Integer, Long> state =
                    (InternalReducingState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces
            //  - def spreads the values over two namespaces (one empty)
            //  - ghi is empty
            //  - jkl has all elements already in the target namespace
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("abc");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("ghi");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();

            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    void testAggregatingStateAddAndGetWithMutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        try {
            AggregatingState<Long, Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();
            state.add(17L);
            state.add(11L);
            assertThat(state.get().longValue()).isEqualTo(28L);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isNull();
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get().longValue()).isEqualTo(28L);
            state.clear();
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get().longValue()).isEqualTo(9L);
            state.clear();

            // make sure all lists / maps are cleared
            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    void testAggregatingStateMergingWithMutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        try {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces
            //  - def spreads the values over two namespaces (one empty)
            //  - ghi is empty
            //  - jkl has all elements already in the target namespace
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("abc");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("ghi");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();

            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    void testAggregatingStateAddAndGetWithImmutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        try {
            AggregatingState<Long, Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();
            state.add(17L);
            state.add(11L);
            assertThat(state.get().longValue()).isEqualTo(28L);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isNull();
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get().longValue()).isEqualTo(28L);
            state.clear();
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get().longValue()).isEqualTo(9L);
            state.clear();

            // make sure all lists / maps are cleared
            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    void testAggregatingStateMergingWithImmutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        try {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces
            //  - def spreads the values over two namespaces (one empty)
            //  - ghi is empty
            //  - jkl has all elements already in the target namespace
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("abc");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("ghi");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();

            assertThat(((TestableKeyedStateBackend) keyedBackend).numKeyValueStateEntries())
                    .withFailMessage("State backend is not empty.")
                    .isZero();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked,rawtypes")
    void testMapState() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        MapStateDescriptor<Integer, String> kvId =
                new MapStateDescriptor<>("id", Integer.class, String.class);

        TypeSerializer<String> keySerializer = StringSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<String> backend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            MapState<Integer, String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<String, VoidNamespace, Map<Integer, String>> kvState =
                    (InternalKvState<String, VoidNamespace, Map<Integer, String>>) state;

            // these are only available after the backend initialized the serializer
            TypeSerializer<Integer> userKeySerializer = kvId.getKeySerializer();
            TypeSerializer<String> userValueSerializer = kvId.getValueSerializer();

            // some modifications to the state
            backend.setCurrentKey("1");
            assertThat(state.get(1)).isNull();
            assertThat(
                            getSerializedMap(
                                    kvState,
                                    "1",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isNull();
            state.put(1, "1");
            backend.setCurrentKey("2");
            assertThat(state.get(2)).isNull();
            assertThat(
                            getSerializedMap(
                                    kvState,
                                    "2",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isNull();
            state.put(2, "2");

            // put entry with different userKeyOffset
            backend.setCurrentKey("11");
            state.put(11, "11");

            backend.setCurrentKey("1");
            assertThat(state.contains(1)).isTrue();
            assertThat(state.get(1)).isEqualTo("1");
            assertThat(
                            getSerializedMap(
                                    kvState,
                                    "1",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(1, "1");
                                }
                            });

            assertThat(
                            getSerializedMap(
                                    kvState,
                                    "11",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(11, "11");
                                }
                            });

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // make some more modifications
            backend.setCurrentKey("1");
            state.put(1, "101");
            backend.setCurrentKey("2");
            state.put(102, "102");
            backend.setCurrentKey("3");
            state.put(103, "103");
            state.putAll(
                    new HashMap<Integer, String>() {
                        {
                            put(1031, "1031");
                            put(1032, "1032");
                        }
                    });

            // draw another snapshot
            KeyedStateHandle snapshot2 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462379L,
                                    4,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            // validate the original state
            backend.setCurrentKey("1");
            assertThat(state.get(1)).isEqualTo("101");
            assertThat(
                            getSerializedMap(
                                    kvState,
                                    "1",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(1, "101");
                                }
                            });

            backend.setCurrentKey("2");
            assertThat(state.get(102)).isEqualTo("102");
            assertThat(
                            getSerializedMap(
                                    kvState,
                                    "2",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(2, "2");
                                    put(102, "102");
                                }
                            });

            backend.setCurrentKey("3");
            assertThat(state.contains(103)).isTrue();
            assertThat(state.get(103)).isEqualTo("103");

            assertThat(
                            getSerializedMap(
                                    kvState,
                                    "3",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(103, "103");
                                    put(1031, "1031");
                                    put(1032, "1032");
                                }
                            });

            List<Integer> keys = new ArrayList<>();
            for (Integer key : state.keys()) {
                keys.add(key);
            }
            List<Integer> expectedKeys = Arrays.asList(103, 1031, 1032);
            assertThat(keys).hasSameSizeAs(expectedKeys);
            keys.removeAll(expectedKeys);

            List<String> values = new ArrayList<>();
            for (String value : state.values()) {
                values.add(value);
            }
            List<String> expectedValues = Arrays.asList("103", "1031", "1032");
            assertThat(values).hasSameSizeAs(expectedValues);
            values.removeAll(expectedValues);

            // make some more modifications
            backend.setCurrentKey("1");
            state.clear();
            backend.setCurrentKey("2");
            state.remove(102);
            backend.setCurrentKey("3");
            final String updateSuffix = "_updated";
            Iterator<Map.Entry<Integer, String>> iterator = state.iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, String> entry = iterator.next();
                if (entry.getValue().length() != 4) {
                    iterator.remove();
                } else {
                    entry.setValue(entry.getValue() + updateSuffix);
                }
            }

            // validate the state
            backend.setCurrentKey("1");
            backend.setCurrentKey("2");
            assertThat(state.contains(102)).isFalse();
            backend.setCurrentKey("3");
            for (Map.Entry<Integer, String> entry : state.entries()) {
                assertThat(entry.getValue()).hasSize(4 + updateSuffix.length());
                assertThat(entry.getValue().endsWith(updateSuffix)).isTrue();
            }

            backend.dispose();
            // restore the first snapshot and validate it
            backend = restoreKeyedBackend(StringSerializer.INSTANCE, snapshot1);
            snapshot1.discardState();

            MapState<Integer, String> restored1 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<String, VoidNamespace, Map<Integer, String>> restoredKvState1 =
                    (InternalKvState<String, VoidNamespace, Map<Integer, String>>) restored1;

            backend.setCurrentKey("1");
            assertThat(restored1.get(1)).isEqualTo("1");
            assertThat(
                            getSerializedMap(
                                    restoredKvState1,
                                    "1",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(1, "1");
                                }
                            });

            backend.setCurrentKey("2");
            assertThat(restored1.get(2)).isEqualTo("2");
            assertThat(
                            getSerializedMap(
                                    restoredKvState1,
                                    "2",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(2, "2");
                                }
                            });

            backend.dispose();
            // restore the second snapshot and validate it
            backend = restoreKeyedBackend(StringSerializer.INSTANCE, snapshot2);
            snapshot2.discardState();

            @SuppressWarnings("unchecked")
            MapState<Integer, String> restored2 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<String, VoidNamespace, Map<Integer, String>> restoredKvState2 =
                    (InternalKvState<String, VoidNamespace, Map<Integer, String>>) restored2;

            backend.setCurrentKey("1");
            assertThat(restored2.get(1)).isEqualTo("101");
            assertThat(
                            getSerializedMap(
                                    restoredKvState2,
                                    "1",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(1, "101");
                                }
                            });

            backend.setCurrentKey("2");
            assertThat(restored2.get(102)).isEqualTo("102");
            assertThat(
                            getSerializedMap(
                                    restoredKvState2,
                                    "2",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(2, "2");
                                    put(102, "102");
                                }
                            });

            backend.setCurrentKey("3");
            assertThat(restored2.get(103)).isEqualTo("103");
            assertThat(
                            getSerializedMap(
                                    restoredKvState2,
                                    "3",
                                    keySerializer,
                                    VoidNamespace.INSTANCE,
                                    namespaceSerializer,
                                    userKeySerializer,
                                    userValueSerializer))
                    .isEqualTo(
                            new HashMap<Integer, String>() {
                                {
                                    put(103, "103");
                                    put(1031, "1031");
                                    put(1032, "1032");
                                }
                            });
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testMapStateIsEmpty() throws Exception {
        MapStateDescriptor<Integer, Long> kvId =
                new MapStateDescriptor<>("id", Integer.class, Long.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            MapState<Integer, Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            backend.setCurrentKey(1);
            assertThat(state.isEmpty()).isTrue();

            int stateSize = 1024;
            for (int i = 0; i < stateSize; i++) {
                state.put(i, i * 2L);
                assertThat(state.isEmpty()).isFalse();
            }

            for (int i = 0; i < stateSize; i++) {
                assertThat(state.isEmpty()).isFalse();
                state.remove(i);
            }
            assertThat(state.isEmpty()).isTrue();

        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * Verify iterator of {@link MapState} supporting arbitrary access, see [FLINK-10267] to know
     * more details.
     */
    @TestTemplate
    void testMapStateIteratorArbitraryAccess() throws Exception {
        MapStateDescriptor<Integer, Long> kvId =
                new MapStateDescriptor<>("id", Integer.class, Long.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            MapState<Integer, Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            backend.setCurrentKey(1);
            int stateSize = 4096;
            for (int i = 0; i < stateSize; i++) {
                state.put(i, i * 2L);
            }
            Iterator<Map.Entry<Integer, Long>> iterator = state.iterator();
            int iteratorCount = 0;
            while (iterator.hasNext()) {
                Map.Entry<Integer, Long> entry = iterator.next();
                assertThat(entry.getKey()).isEqualTo(iteratorCount);
                switch (ThreadLocalRandom.current().nextInt() % 3) {
                    case 0: // remove twice
                        iterator.remove();
                        assertThatThrownBy(iterator::remove)
                                .isInstanceOf(IllegalStateException.class);
                        break;
                    case 1: // hasNext -> remove
                        iterator.hasNext();
                        iterator.remove();
                        break;
                    case 2: // nothing to do
                        break;
                }
                iteratorCount++;
            }
            assertThat(iteratorCount).isEqualTo(stateSize);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Verify that iterator of {@link MapState} compares on the content. */
    @TestTemplate
    public void testMapStateEntryCompare() throws Exception {
        MapStateDescriptor<Integer, Long> stateDesc1 =
                new MapStateDescriptor<>("map-state-1", Integer.class, Long.class);
        MapStateDescriptor<Integer, Long> stateDesc2 =
                new MapStateDescriptor<>("map-state-2", Integer.class, Long.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            MapState<Integer, Long> state1 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDesc1);
            MapState<Integer, Long> state2 =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDesc2);

            Map.Entry<Integer, Long> expectedEntry = new AbstractMap.SimpleEntry<>(0, 10L);
            backend.setCurrentKey(1);
            state1.put(expectedEntry.getKey(), expectedEntry.getValue());
            state2.put(expectedEntry.getKey(), expectedEntry.getValue());

            assertThat(state1.entries().iterator().next()).isEqualTo(expectedEntry);
            assertThat(state2.entries().iterator().next()).isEqualTo(expectedEntry);

            assertThat(state1.entries().iterator().next())
                    .isEqualTo(state2.entries().iterator().next());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Verify that {@link ValueStateDescriptor} allows {@code null} as default. */
    @TestTemplate
    void testValueStateNullAsDefaultValue() throws Exception {
        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            assertThat(state.value()).isNull();

            state.update("Ciao");
            assertThat(state.value()).isEqualTo("Ciao");

            state.clear();
            assertThat(state.value()).isNull();

        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Verify that an empty {@code ValueState} will yield the default value. */
    @TestTemplate
    void testValueStateDefaultValue() throws Exception {
        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, "Hello");

        KeyedStateHandle keyedStateHandle;
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            assertThat(state.value()).isEqualTo("Hello");

            state.update("Ciao");
            assertThat(state.value()).isEqualTo("Ciao");

            state.clear();
            assertThat(state.value()).isEqualTo("Hello");

            keyedStateHandle =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    1L,
                                    createStreamFactory(),
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            new SharedStateRegistryImpl());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }

        try {
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, keyedStateHandle);
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            assertThat(state.value()).isEqualTo("Hello");
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Verify that an empty {@code ReduceState} yields {@code null}. */
    @TestTemplate
    void testReducingStateDefaultValue() throws Exception {
        ReducingStateDescriptor<String> kvId =
                new ReducingStateDescriptor<>("id", new AppendingReduce(), String.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ReducingState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            assertThat(state.get()).isNull();

            state.add("Ciao");
            assertThat(state.get()).isEqualTo("Ciao");

            state.clear();
            assertThat(state.get()).isNull();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Verify that an empty {@code ListState} yields {@code null}. */
    @TestTemplate
    void testListStateDefaultValue() throws Exception {
        ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ListState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            assertThat(state.get()).isNull();

            state.update(Arrays.asList("Ciao", "Bello"));
            assertThat(state.get()).containsExactlyInAnyOrder("Ciao", "Bello");

            state.clear();
            assertThat(state.get()).isNull();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Verify that an empty {@code MapState} yields {@code null}. */
    @TestTemplate
    void testMapStateDefaultValue() throws Exception {
        MapStateDescriptor<String, String> kvId =
                new MapStateDescriptor<>("id", String.class, String.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            MapState<String, String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            assertThat(state.entries()).isNotNull();
            assertThat(state.entries().iterator()).isExhausted();

            state.put("Ciao", "Hello");
            state.put("Bello", "Nice");

            assertThat(state.entries()).isNotNull();
            assertThat(state.get("Ciao")).isEqualTo("Hello");
            assertThat(state.get("Bello")).isEqualTo("Nice");

            state.clear();
            assertThat(state.entries()).isNotNull();
            assertThat(state.entries().iterator()).isExhausted();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testSnapshotNonAccessedState() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        final String stateName = "test-name";

        CheckpointableKeyedStateBackend<String> backend =
                createKeyedBackend(StringSerializer.INSTANCE);

        try {
            MapStateDescriptor<Integer, String> kvId =
                    new MapStateDescriptor<>(stateName, Integer.class, String.class);
            MapState<Integer, String> mapState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // write some state to be snapshotted
            backend.setCurrentKey("1");
            mapState.put(11, "foo");
            backend.setCurrentKey("2");
            mapState.put(8, "bar");
            backend.setCurrentKey("3");
            mapState.put(91, "hello world");

            // take a snapshot, and then restore backend with snapshot
            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    1L,
                                    2L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            backend.dispose();

            backend = restoreKeyedBackend(StringSerializer.INSTANCE, snapshot);

            // now take a snapshot again without accessing the state
            snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    2L,
                                    3L,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            backend.dispose();

            // we restore again and try to access previous state
            backend = restoreKeyedBackend(StringSerializer.INSTANCE, snapshot);
            mapState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey("1");
            assertThat(mapState.get(11)).isEqualTo("foo");
            backend.setCurrentKey("2");
            assertThat(mapState.get(8)).isEqualTo("bar");
            backend.setCurrentKey("3");
            assertThat(mapState.get(91)).isEqualTo("hello world");

            snapshot.discardState();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * This test verifies that state is correctly assigned to key groups and that restore restores
     * the relevant key groups in the backend.
     *
     * <p>We have 128 key groups. Initially, four backends with different states are responsible for
     * all the key groups equally. Different backends for the same operator may contains different
     * states if we create the state in runtime (such as {@link DeltaTrigger#onElement} Then we
     * snapshot, split up the state and restore into 2 backends where each is responsible for 64 key
     * groups. Then we make sure that the state is only available in the correct backend.
     */
    @TestTemplate
    void testKeyGroupSnapshotRestoreScaleDown() throws Exception {
        testKeyGroupSnapshotRestore(4, 2, 128);
    }

    /**
     * This test verifies that state is correctly assigned to key groups and that restore restores
     * the relevant key groups in the backend.
     *
     * <p>We have 128 key groups. Initially, two backends with different states are responsible for
     * all the key groups equally. Different backends for the same operator may contains different
     * states if we create the state in runtime (such as {@link DeltaTrigger#onElement} Then we
     * snapshot, split up the state and restore into 4 backends where each is responsible for 32 key
     * groups. Then we make sure that the state is only available in the correct backend.
     */
    @TestTemplate
    void testKeyGroupSnapshotRestoreScaleUp() throws Exception {
        testKeyGroupSnapshotRestore(2, 4, 128);
    }

    /**
     * This test verifies that state is correctly assigned to key groups and that restore restores
     * the relevant key groups in the backend.
     *
     * <p>We have 128 key groups. Initially, two backends with different states are responsible for
     * all the key groups equally. Different backends for the same operator may contains different
     * states if we create the state in runtime (such as {@link DeltaTrigger#onElement} Then we
     * snapshot, split up the state and restore into 2 backends where each is responsible for 64 key
     * groups. Then we make sure that the state is only available in the correct backend.
     */
    @TestTemplate
    void testKeyGroupsSnapshotRestoreNoRescale() throws Exception {
        testKeyGroupSnapshotRestore(2, 2, 128);
    }

    /**
     * Similar with testKeyGroupSnapshotRestoreScaleUp, but the KeyGroups were distributed unevenly.
     */
    @TestTemplate
    void testKeyGroupsSnapshotRestoreScaleUpUnEvenDistribute() throws Exception {
        testKeyGroupSnapshotRestore(15, 77, 128);
    }

    /**
     * Similar with testKeyGroupSnapshotRestoreScaleDown, but the KeyGroups were distributed
     * unevenly.
     */
    @TestTemplate
    void testKeyGroupsSnapshotRestoreScaleDownUnEvenDistribute() throws Exception {
        testKeyGroupSnapshotRestore(77, 15, 128);
    }

    private void testKeyGroupSnapshotRestore(
            int sourceParallelism, int targetParallelism, int maxParallelism) throws Exception {
        checkArgument(
                sourceParallelism > 0,
                "parallelism must be positive, current is %s.",
                sourceParallelism);
        checkArgument(
                targetParallelism > 0,
                "parallelism must be positive, current is %s.",
                targetParallelism);
        checkArgument(
                sourceParallelism <= maxParallelism,
                "Maximum parallelism must not be smaller than parallelism.");
        checkArgument(
                targetParallelism <= maxParallelism,
                "Maximum parallelism must not be smaller than parallelism.");

        Random random = new Random();

        List<ValueStateDescriptor<String>> stateDescriptors = new ArrayList<>(maxParallelism);
        List<Integer> keyInKeyGroups = new ArrayList<>(maxParallelism);
        List<String> expectedValue = new ArrayList<>(maxParallelism);
        for (int i = 0; i < maxParallelism; ++i) {
            // all states have different name to mock that all the parallelisms of one operator have
            // different states.
            stateDescriptors.add(new ValueStateDescriptor<>("state" + i, String.class));
        }

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        List<KeyedStateHandle> snapshots = new ArrayList<>(sourceParallelism);
        for (int i = 0; i < sourceParallelism; ++i) {
            KeyGroupRange range =
                    KeyGroupRange.of(
                            maxParallelism * i / sourceParallelism,
                            maxParallelism * (i + 1) / sourceParallelism - 1);
            CheckpointableKeyedStateBackend<Integer> backend =
                    createKeyedBackend(IntSerializer.INSTANCE, maxParallelism, range, env);
            try {
                for (int j = range.getStartKeyGroup(); j <= range.getEndKeyGroup(); ++j) {
                    ValueState<String> state =
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    stateDescriptors.get(j));
                    int keyInKeyGroup =
                            getKeyInKeyGroup(random, maxParallelism, KeyGroupRange.of(j, j));
                    backend.setCurrentKey(keyInKeyGroup);
                    keyInKeyGroups.add(keyInKeyGroup);
                    String updateValue = i + ":" + j;
                    state.update(updateValue);
                    expectedValue.add(updateValue);
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
            CheckpointableKeyedStateBackend<Integer> backend =
                    restoreKeyedBackend(
                            IntSerializer.INSTANCE,
                            maxParallelism,
                            keyGroupRangesRestore.get(i),
                            keyGroupStatesAfterDistribute.get(i),
                            env);
            try {
                KeyGroupRange range = keyGroupRangesRestore.get(i);
                for (int j = range.getStartKeyGroup(); j <= range.getEndKeyGroup(); ++j) {
                    ValueState<String> state =
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    stateDescriptors.get(j));
                    backend.setCurrentKey(keyInKeyGroups.get(j));
                    assertThat(state.value()).isEqualTo(expectedValue.get(j));
                }
            } finally {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }
    }

    @TestTemplate
    void testRestoreWithWrongKeySerializer() throws Exception {
        assumeThat(supportsMetaInfoVerification()).isTrue();
        CheckpointStreamFactory streamFactory = createStreamFactory();

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        KeyedStateHandle snapshot;
        // use an IntSerializer at first
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // write some state
            backend.setCurrentKey(1);
            state.update("1");
            backend.setCurrentKey(2);
            state.update("2");

            // draw a snapshot
            snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }

        // restore with the wrong key serializer
        assertThatThrownBy(() -> restoreKeyedBackend(DoubleSerializer.INSTANCE, snapshot))
                .withFailMessage("should recognize wrong key serializer")
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testValueStateRestoreWithWrongSerializers() throws Exception {
        assumeThat(supportsMetaInfoVerification()).isTrue();
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            state.update("1");
            backend.setCurrentKey(2);
            state.update("2");

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();
            // restore the first snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
            snapshot1.discardState();

            @SuppressWarnings("unchecked")
            TypeSerializer<String> fakeStringSerializer =
                    (TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

            CheckpointableKeyedStateBackend<Integer> restoredBackend = backend;

            assertThatThrownBy(
                            () ->
                                    restoredBackend
                                            .getPartitionedState(
                                                    VoidNamespace.INSTANCE,
                                                    VoidNamespaceSerializer.INSTANCE,
                                                    new ValueStateDescriptor<>(
                                                            "id", fakeStringSerializer))
                                            .value())
                    .withFailMessage("should recognize wrong serializers")
                    .isInstanceOf(StateMigrationException.class);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testListStateRestoreWithWrongSerializers() throws Exception {
        assumeThat(supportsMetaInfoVerification()).isTrue();
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);
            ListState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            state.add("1");
            backend.setCurrentKey(2);
            state.add("2");

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();
            // restore the first snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
            snapshot1.discardState();

            @SuppressWarnings("unchecked")
            TypeSerializer<String> fakeStringSerializer =
                    (TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

            CheckpointableKeyedStateBackend<Integer> restoredBackend = backend;

            assertThatThrownBy(
                            () ->
                                    restoredBackend
                                            .getPartitionedState(
                                                    VoidNamespace.INSTANCE,
                                                    VoidNamespaceSerializer.INSTANCE,
                                                    new ListStateDescriptor<>(
                                                            "id", fakeStringSerializer))
                                            .get())
                    .withFailMessage("should recognize wrong serializers")
                    .isInstanceOf(StateMigrationException.class);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testReducingStateRestoreWithWrongSerializers() throws Exception {
        assumeThat(supportsMetaInfoVerification()).isTrue();
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            ReducingStateDescriptor<String> kvId =
                    new ReducingStateDescriptor<>(
                            "id", new AppendingReduce(), StringSerializer.INSTANCE);
            ReducingState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            state.add("1");
            backend.setCurrentKey(2);
            state.add("2");

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();
            // restore the first snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
            snapshot1.discardState();

            @SuppressWarnings("unchecked")
            TypeSerializer<String> fakeStringSerializer =
                    (TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

            CheckpointableKeyedStateBackend<Integer> restoredBackend = backend;

            assertThatThrownBy(
                            () ->
                                    restoredBackend
                                            .getPartitionedState(
                                                    VoidNamespace.INSTANCE,
                                                    VoidNamespaceSerializer.INSTANCE,
                                                    new ReducingStateDescriptor<>(
                                                            "id",
                                                            new AppendingReduce(),
                                                            fakeStringSerializer))
                                            .get())
                    .withFailMessage("should recognize wrong serializers")
                    .isInstanceOf(StateMigrationException.class);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testMapStateRestoreWithWrongSerializers() throws Exception {
        assumeThat(supportsMetaInfoVerification()).isTrue();
        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            MapStateDescriptor<String, String> kvId =
                    new MapStateDescriptor<>(
                            "id", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
            MapState<String, String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            state.put("1", "First");
            backend.setCurrentKey(2);
            state.put("2", "Second");

            // draw a snapshot
            KeyedStateHandle snapshot1 =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();
            // restore the first snapshot and validate it
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
            snapshot1.discardState();

            @SuppressWarnings("unchecked")
            TypeSerializer<String> fakeStringSerializer =
                    (TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

            CheckpointableKeyedStateBackend<Integer> restoredBackend = backend;
            assertThatThrownBy(
                            () ->
                                    restoredBackend
                                            .getPartitionedState(
                                                    VoidNamespace.INSTANCE,
                                                    VoidNamespaceSerializer.INSTANCE,
                                                    new MapStateDescriptor<>(
                                                            "id",
                                                            fakeStringSerializer,
                                                            StringSerializer.INSTANCE))
                                            .entries())
                    .withFailMessage("should recognize wrong serializers")
                    .isInstanceOf(StateMigrationException.class);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testCopyDefaultValue() throws Exception {
        ValueStateDescriptor<IntValue> kvId =
                new ValueStateDescriptor<>("id", IntValue.class, new IntValue(-1));

        final CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueState<IntValue> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            IntValue default1 = state.value();

            backend.setCurrentKey(2);
            IntValue default2 = state.value();

            assertThat(default1).isNotNull();
            assertThat(default2).isNotNull();
            assertThat(default2).isEqualTo(default1);
            assertThat(default2).isNotSameAs(default1);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * Previously, it was possible to create partitioned state with <code>null</code> namespace.
     * This test makes sure that this is prohibited now.
     */
    @TestTemplate
    void testRequireNonNullNamespace() throws Exception {
        ValueStateDescriptor<IntValue> kvId =
                new ValueStateDescriptor<>("id", IntValue.class, new IntValue(-1));

        final CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            assertThatThrownBy(
                            () ->
                                    backend.getPartitionedState(
                                            null, VoidNamespaceSerializer.INSTANCE, kvId))
                    .isInstanceOf(NullPointerException.class);

            assertThatThrownBy(
                            () -> backend.getPartitionedState(VoidNamespace.INSTANCE, null, kvId))
                    .isInstanceOf(NullPointerException.class);

            assertThatThrownBy(() -> backend.getPartitionedState(null, null, kvId))
                    .isInstanceOf(NullPointerException.class);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /**
     * Tests that {@link AbstractHeapState} instances respect the queryable flag and create
     * concurrent variants for internal state structures.
     */
    @SuppressWarnings("unchecked")
    void testConcurrentMapIfQueryable() throws Exception {
        final int numberOfKeyGroups = 1;
        final CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(
                        IntSerializer.INSTANCE,
                        numberOfKeyGroups,
                        new KeyGroupRange(0, 0),
                        new DummyEnvironment());

        try {
            {
                // ValueState
                ValueStateDescriptor<Integer> desc =
                        new ValueStateDescriptor<>("value-state", Integer.class, -1);
                desc.setQueryable("my-query");

                ValueState<Integer> state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

                InternalKvState<Integer, VoidNamespace, Integer> kvState =
                        (InternalKvState<Integer, VoidNamespace, Integer>) state;
                assertThat(kvState).isInstanceOf(AbstractHeapState.class);

                kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
                backend.setCurrentKey(1);
                state.update(121818273);

                assertThat(((AbstractHeapState<?, ?, ?>) kvState).getStateTable())
                        .withFailMessage("State not set")
                        .isNotNull();
            }

            {
                // ListState
                ListStateDescriptor<Integer> desc =
                        new ListStateDescriptor<>("list-state", Integer.class);
                desc.setQueryable("my-query");

                ListState<Integer> state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

                InternalKvState<Integer, VoidNamespace, Integer> kvState =
                        (InternalKvState<Integer, VoidNamespace, Integer>) state;
                assertThat(kvState).isInstanceOf(AbstractHeapState.class);

                kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
                backend.setCurrentKey(1);
                state.add(121818273);

                assertThat(((AbstractHeapState<?, ?, ?>) kvState).getStateTable())
                        .withFailMessage("State not set")
                        .isNotNull();
            }

            {
                // ReducingState
                ReducingStateDescriptor<Integer> desc =
                        new ReducingStateDescriptor<>(
                                "reducing-state",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2)
                                            throws Exception {
                                        return value1 + value2;
                                    }
                                },
                                Integer.class);
                desc.setQueryable("my-query");

                ReducingState<Integer> state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

                InternalKvState<Integer, VoidNamespace, Integer> kvState =
                        (InternalKvState<Integer, VoidNamespace, Integer>) state;
                assertThat(kvState).isInstanceOf(AbstractHeapState.class);

                kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
                backend.setCurrentKey(1);
                state.add(121818273);

                assertThat(((AbstractHeapState<?, ?, ?>) kvState).getStateTable())
                        .withFailMessage("State not set")
                        .isNotNull();
            }

            {
                // MapState
                MapStateDescriptor<Integer, String> desc =
                        new MapStateDescriptor<>("map-state", Integer.class, String.class);
                desc.setQueryable("my-query");

                MapState<Integer, String> state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

                InternalKvState<Integer, VoidNamespace, Map<Integer, String>> kvState =
                        (InternalKvState<Integer, VoidNamespace, Map<Integer, String>>) state;
                assertThat(kvState).isInstanceOf(AbstractHeapState.class);

                kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
                backend.setCurrentKey(1);
                state.put(121818273, "121818273");

                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(1, numberOfKeyGroups);
                StateTable stateTable = ((AbstractHeapState) kvState).getStateTable();
                assertThat(stateTable.get(keyGroupIndex))
                        .withFailMessage("State not set")
                        .isNotNull();
            }
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Tests registration with the KvStateRegistry. */
    @TestTemplate
    void testQueryableStateRegistration() throws Exception {
        KvStateRegistry registry = env.getKvStateRegistry();

        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);
        try {
            KeyGroupRange expectedKeyGroupRange = backend.getKeyGroupRange();

            KvStateRegistryListener listener = mock(KvStateRegistryListener.class);
            registry.registerListener(HighAvailabilityServices.DEFAULT_JOB_ID, listener);

            ValueStateDescriptor<Integer> desc =
                    new ValueStateDescriptor<>("test", IntSerializer.INSTANCE);
            desc.setQueryable("banana");

            backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            // Verify registered
            verify(listener, times(1))
                    .notifyKvStateRegistered(
                            eq(env.getJobID()),
                            eq(env.getJobVertexId()),
                            eq(expectedKeyGroupRange),
                            eq("banana"),
                            any(KvStateID.class));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462379L,
                                    4,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            backend.dispose();

            verify(listener, times(1))
                    .notifyKvStateUnregistered(
                            eq(env.getJobID()),
                            eq(env.getJobVertexId()),
                            eq(expectedKeyGroupRange),
                            eq("banana"));
            backend.dispose();
            // Initialize again
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);
            if (snapshot != null) {
                snapshot.discardState();
            }

            backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            // Verify registered again
            verify(listener, times(2))
                    .notifyKvStateRegistered(
                            eq(env.getJobID()),
                            eq(env.getJobVertexId()),
                            eq(expectedKeyGroupRange),
                            eq("banana"),
                            any(KvStateID.class));
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testEmptyStateCheckpointing() throws Exception {

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            CheckpointStreamFactory streamFactory = createStreamFactory();
            SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

            // draw a snapshot
            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462379L,
                                    1,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            assertThat(snapshot).isNull();
            backend.dispose();

            backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testNumStateEntries() throws Exception {
        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            assertThat(((TestableKeyedStateBackend) backend).numKeyValueStateEntries()).isZero();

            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(0);
            state.update("hello");
            state.update("ciao");

            assertThat(((TestableKeyedStateBackend) backend).numKeyValueStateEntries()).isOne();

            backend.setCurrentKey(42);
            state.update("foo");

            assertThat(((TestableKeyedStateBackend) backend).numKeyValueStateEntries())
                    .isEqualTo(2);

            backend.setCurrentKey(0);
            state.clear();

            assertThat(((TestableKeyedStateBackend) backend).numKeyValueStateEntries()).isOne();

            backend.setCurrentKey(42);
            state.clear();

            assertThat(((TestableKeyedStateBackend) backend).numKeyValueStateEntries()).isZero();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    private static class AppendingReduce implements ReduceFunction<String> {
        @Override
        public String reduce(String value1, String value2) throws Exception {
            return value1 + "," + value2;
        }
    }

    /**
     * The purpose of this test is to check that parallel snapshots are possible, and work even if a
     * previous snapshot is still running and blocking.
     */
    @TestTemplate
    void testParallelAsyncSnapshots() throws Exception {
        assumeThat(snapshotUsesStreamFactory()).isTrue();
        OneShotLatch blocker = new OneShotLatch();
        OneShotLatch waiter = new OneShotLatch();
        BlockerCheckpointStreamFactory streamFactory =
                new BlockerCheckpointStreamFactory(1024 * 1024);
        streamFactory.setWaiterLatch(waiter);
        streamFactory.setBlockerLatch(blocker);
        streamFactory.setAfterNumberInvocations(10);

        final CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {

            if (!supportsAsynchronousSnapshots()) {
                return;
            }

            // insert some data to the backend.
            InternalValueState<Integer, VoidNamespace, Integer> valueState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("test", IntSerializer.INSTANCE));

            valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

            for (int i = 0; i < 10; ++i) {
                backend.setCurrentKey(i);
                valueState.update(i);
            }

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot1 =
                    backend.snapshot(
                            0L,
                            0L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            Thread runner1 = new Thread(snapshot1, "snapshot-1-runner");
            runner1.start();
            // after this call returns, we have a running snapshot-1 that is blocked in IO.
            waiter.await();

            // do some updates in between the snapshots.
            for (int i = 5; i < 15; ++i) {
                backend.setCurrentKey(i);
                valueState.update(i + 1);
            }

            // we don't want to block the second snapshot.
            streamFactory.setWaiterLatch(null);
            streamFactory.setBlockerLatch(null);

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot2 =
                    backend.snapshot(
                            1L,
                            1L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            Thread runner2 = new Thread(snapshot2, "snapshot-2-runner");
            runner2.start();
            // snapshot-2 should run and succeed, while snapshot-1 is still running and blocked in
            // IO.
            snapshot2.get();

            // we release the blocking IO so that snapshot-1 can also finish and succeed.
            blocker.trigger();
            snapshot1.get();

        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testNonConcurrentSnapshotTransformerAccess() throws Exception {
        BlockerCheckpointStreamFactory streamFactory =
                new BlockerCheckpointStreamFactory(1024 * 1024);
        CheckpointableKeyedStateBackend<Integer> backend = null;
        try {
            backend = createKeyedBackend(IntSerializer.INSTANCE);
            new StateSnapshotTransformerTest(backend, streamFactory)
                    .testNonConcurrentSnapshotTransformerAccess();
        } finally {
            if (backend != null) {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }
    }

    @TestTemplate
    void testAsyncSnapshot() throws Exception {
        assumeThat(snapshotUsesStreamFactory()).isTrue();
        OneShotLatch waiter = new OneShotLatch();
        BlockerCheckpointStreamFactory streamFactory =
                new BlockerCheckpointStreamFactory(1024 * 1024);
        streamFactory.setWaiterLatch(waiter);

        CheckpointableKeyedStateBackend<Integer> backend = null;
        KeyedStateHandle stateHandle = null;

        try {
            backend = createKeyedBackend(IntSerializer.INSTANCE);
            InternalValueState<Integer, VoidNamespace, Integer> valueState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("test", IntSerializer.INSTANCE));

            valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

            for (int i = 0; i < 10; ++i) {
                backend.setCurrentKey(i);
                valueState.update(i);
            }

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    backend.snapshot(
                            0L,
                            0L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            Thread runner = new Thread(snapshot);
            runner.start();
            for (int i = 0; i < 20; ++i) {
                backend.setCurrentKey(i);
                valueState.update(i + 1);
                if (10 == i) {
                    waiter.await();
                }
            }

            runner.join();
            SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
            stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

            // test isolation
            for (int i = 0; i < 20; ++i) {
                backend.setCurrentKey(i);
                assertThat((int) valueState.value()).isEqualTo(i + 1);
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
            backend = restoreKeyedBackend(IntSerializer.INSTANCE, stateHandle);

            InternalValueState<Integer, VoidNamespace, Integer> valueState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("test", IntSerializer.INSTANCE));

            valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

            for (int i = 0; i < 10; ++i) {
                backend.setCurrentKey(i);
                assertThat((int) valueState.value()).isEqualTo(i);
            }

            backend.setCurrentKey(11);
            assertThat(valueState.value()).isNull();
        } finally {
            if (null != backend) {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }
    }

    /**
     * Since {@link AbstractKeyedStateBackend#getKeys(String, Object)} does't support concurrent
     * modification and {@link AbstractKeyedStateBackend#applyToAllKeys(Object, TypeSerializer,
     * StateDescriptor, KeyedStateFunction)} rely on it to get keys from backend. So we need this
     * unit test to verify the concurrent modification with {@link
     * AbstractKeyedStateBackend#applyToAllKeys(Object, TypeSerializer, StateDescriptor,
     * KeyedStateFunction)}.
     */
    @TestTemplate
    void testConcurrentModificationWithApplyToAllKeys() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<>("foo", StringSerializer.INSTANCE);

            ListState<String> listState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            listStateDescriptor);

            for (int i = 0; i < 100; ++i) {
                backend.setCurrentKey(i);
                listState.add("Hello" + i);
            }

            // valid state value via applyToAllKeys().
            backend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    listStateDescriptor,
                    new KeyedStateFunction<Integer, ListState<String>>() {
                        @Override
                        public void process(Integer key, ListState<String> state) throws Exception {
                            assertThat(state.get().iterator().next()).isEqualTo("Hello" + key);
                        }
                    });

            // clear state via applyToAllKeys().
            backend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    listStateDescriptor,
                    new KeyedStateFunction<Integer, ListState<String>>() {
                        @Override
                        public void process(Integer key, ListState<String> state) throws Exception {
                            state.clear();
                        }
                    });

            // valid that state has been cleared.
            backend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    listStateDescriptor,
                    new KeyedStateFunction<Integer, ListState<String>>() {
                        @Override
                        public void process(Integer key, ListState<String> state) throws Exception {
                            assertThat(state.get().iterator()).isExhausted();
                        }
                    });

            // clear() with add() in applyToAllKeys()
            backend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    listStateDescriptor,
                    new KeyedStateFunction<Integer, ListState<String>>() {
                        @Override
                        public void process(Integer key, ListState<String> state) throws Exception {
                            state.add("Hello" + key);
                            state.clear();
                            state.add("Hello_" + key);
                        }
                    });

            // valid state value via applyToAllKeys().
            backend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    listStateDescriptor,
                    new KeyedStateFunction<Integer, ListState<String>>() {
                        @Override
                        public void process(Integer key, ListState<String> state) throws Exception {
                            final Iterator<String> it = state.get().iterator();
                            assertThat(it.next()).isEqualTo("Hello_" + key);
                            assertThat(it).isExhausted(); // finally verify we have no more elements
                        }
                    });
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testApplyToAllKeysLambdaFunction() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<>("foo", StringSerializer.INSTANCE);

            ListState<String> listState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            listStateDescriptor);

            for (int i = 0; i < 100; ++i) {
                backend.setCurrentKey(i);
                listState.add("Hello" + i);
            }

            // valid state value via applyToAllKeys().
            backend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    listStateDescriptor,
                    (Integer key, ListState<String> state) ->
                            assertThat(state.get().iterator().next()).isEqualTo("Hello" + key));
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testAsyncSnapshotCancellation() throws Exception {
        assumeThat(snapshotUsesStreamFactory()).isTrue();
        OneShotLatch blocker = new OneShotLatch();
        OneShotLatch waiter = new OneShotLatch();
        BlockerCheckpointStreamFactory streamFactory =
                new BlockerCheckpointStreamFactory(1024 * 1024);
        streamFactory.setWaiterLatch(waiter);
        streamFactory.setBlockerLatch(blocker);
        streamFactory.setAfterNumberInvocations(10);

        final CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {

            if (!supportsAsynchronousSnapshots()) {
                return;
            }

            InternalValueState<Integer, VoidNamespace, Integer> valueState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("test", IntSerializer.INSTANCE));

            valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

            for (int i = 0; i < 10; ++i) {
                backend.setCurrentKey(i);
                valueState.update(i);
            }

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    backend.snapshot(
                            0L,
                            0L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            Thread runner = new Thread(snapshot);
            runner.start();

            // wait until the code reached some stream read
            waiter.await();

            // close the backend to see if the close is propagated to the stream
            IOUtils.closeQuietly(backend);

            // unblock the stream so that it can run into the IOException
            blocker.trigger();

            runner.join();

            assertThatThrownBy(snapshot::get)
                    .withFailMessage("Close was not propagated.")
                    .isInstanceOf(CancellationException.class);

        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testMapStateGetKeys() throws Exception {
        final int namespace1ElementsNum = 1000;
        final int namespace2ElementsNum = 1000;
        String fieldName = "get-keys-test";
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            final String ns1 = "ns1";
            MapState<String, Integer> keyedState1 =
                    backend.getPartitionedState(
                            ns1,
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    fieldName, StringSerializer.INSTANCE, IntSerializer.INSTANCE));

            for (int key = 0; key < namespace1ElementsNum; key++) {
                backend.setCurrentKey(key);
                keyedState1.put("he", key * 2);
                keyedState1.put("ho", key * 2);
            }

            final String ns2 = "ns2";
            MapState<String, Integer> keyedState2 =
                    backend.getPartitionedState(
                            ns2,
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    fieldName, StringSerializer.INSTANCE, IntSerializer.INSTANCE));

            for (int key = namespace1ElementsNum;
                    key < namespace1ElementsNum + namespace2ElementsNum;
                    key++) {
                backend.setCurrentKey(key);
                keyedState2.put("he", key * 2);
                keyedState2.put("ho", key * 2);
            }

            // valid for namespace1
            try (Stream<Integer> keysStream = backend.getKeys(fieldName, ns1).sorted()) {
                PrimitiveIterator.OfInt actualIterator =
                        keysStream.mapToInt(value -> value.intValue()).iterator();

                for (int expectedKey = 0; expectedKey < namespace1ElementsNum; expectedKey++) {
                    assertThat(actualIterator).hasNext();
                    assertThat(actualIterator.nextInt()).isEqualTo(expectedKey);
                }

                assertThat(actualIterator).isExhausted();
            }

            // valid for namespace2
            try (Stream<Integer> keysStream = backend.getKeys(fieldName, ns2).sorted()) {
                PrimitiveIterator.OfInt actualIterator =
                        keysStream.mapToInt(value -> value.intValue()).iterator();

                for (int expectedKey = namespace1ElementsNum;
                        expectedKey < namespace1ElementsNum + namespace2ElementsNum;
                        expectedKey++) {
                    assertThat(actualIterator).hasNext();
                    assertThat(actualIterator.nextInt()).isEqualTo(expectedKey);
                }

                assertThat(actualIterator).isExhausted();
            }
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testMapStateGetKeysAndNamespaces() throws Exception {
        final int elementsNum = 1000;
        String fieldName = "get-keys-test";
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            InternalMapState<Integer, String, String, Integer> internalState =
                    backend.createOrUpdateInternalState(
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    fieldName, StringSerializer.INSTANCE, IntSerializer.INSTANCE));
            String[] namespaces = new String[] {"ns1", "ns2"};

            for (int key = 0; key < elementsNum; key++) {
                backend.setCurrentKey(key);
                for (String ns : namespaces) {
                    internalState.setCurrentNamespace(ns);
                    internalState.put("hello", key);
                    internalState.put("world", key);
                }
            }

            try (Stream<Tuple2<Integer, String>> stream = backend.getKeysAndNamespaces(fieldName)) {
                final Map<String, Set<Integer>> keysByNamespace = new HashMap<>();
                stream.forEach(
                        entry -> {
                            assertThat(entry.f1)
                                    .withFailMessage("Unexpected namespace")
                                    .isIn(namespaces);
                            assertThat(entry.f0)
                                    .withFailMessage("Unexpected key")
                                    .isGreaterThanOrEqualTo(0)
                                    .isLessThan(elementsNum);

                            Set<Integer> keys =
                                    keysByNamespace.computeIfAbsent(entry.f1, k -> new HashSet<>());
                            assertThat(keys.add(entry.f0))
                                    .withFailMessage("Duplicate key for namespace")
                                    .isTrue();
                        });
            }
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void testCheckConcurrencyProblemWhenPerformingCheckpointAsync() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();
        ExecutorService executorService = Executors.newScheduledThreadPool(1);
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, env);
        try {
            long checkpointID = 0;
            List<Future> futureList = new ArrayList();
            for (int i = 0; i < 10; ++i) {
                ValueStateDescriptor<Integer> kvId =
                        new ValueStateDescriptor<>("id" + i, IntSerializer.INSTANCE);
                ValueState<Integer> state =
                        backend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, kvId);
                ((InternalValueState) state).setCurrentNamespace(VoidNamespace.INSTANCE);
                backend.setCurrentKey(i);
                state.update(i);

                futureList.add(
                        runSnapshotAsync(
                                executorService,
                                backend.snapshot(
                                        checkpointID++,
                                        System.currentTimeMillis(),
                                        streamFactory,
                                        CheckpointOptions.forCheckpointWithDefaultLocation())));
            }

            for (Future future : futureList) {
                future.get(20, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
            executorService.shutdown();
        }
    }

    private void assertRestoreKeyedBackendFail(
            KeyedStateHandle keyedStateHandle, ValueStateDescriptor<TestPojo> kvId) {
        assertThatThrownBy(
                        () -> {
                            // state backends that eagerly deserializes (such as the memory
                            // state backend) will fail here
                            CheckpointableKeyedStateBackend<Integer> restoreBackend = null;
                            try {
                                restoreBackend =
                                        restoreKeyedBackend(
                                                IntSerializer.INSTANCE, keyedStateHandle, env);

                                ValueState<TestPojo> restoreState =
                                        restoreBackend.getPartitionedState(
                                                VoidNamespace.INSTANCE,
                                                VoidNamespaceSerializer.INSTANCE,
                                                kvId);

                                restoreBackend.setCurrentKey(1);
                                // state backends that lazily deserializes (such as RocksDB)
                                // will fail here
                                restoreState.value();

                                restoreBackend.dispose();
                            } finally {
                                if (restoreBackend != null) {
                                    IOUtils.closeQuietly(restoreBackend);
                                    restoreBackend.dispose();
                                }
                            }
                        })
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(ExpectedKryoTestException.class),
                        e -> assertThat(e).hasCauseInstanceOf(ExpectedKryoTestException.class));
    }

    protected Future<SnapshotResult<KeyedStateHandle>> runSnapshotAsync(
            ExecutorService executorService,
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture)
            throws Exception {

        if (!snapshotRunnableFuture.isDone()) {
            CompletableFuture<SnapshotResult<KeyedStateHandle>> completableFuture =
                    new CompletableFuture<>();
            executorService.submit(
                    () -> {
                        try {
                            snapshotRunnableFuture.run();
                            completableFuture.complete(snapshotRunnableFuture.get());
                        } catch (Exception e) {
                            completableFuture.completeExceptionally(e);
                        }
                    });
            return completableFuture;
        }
        return CompletableFuture.completedFuture(snapshotRunnableFuture.get());
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

    /** Returns the value by getting the serialized value and deserializing it if it is not null. */
    protected static <V, K, N> V getSerializedValue(
            InternalKvState<K, N, V> kvState,
            K key,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer)
            throws Exception {

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key, keySerializer, namespace, namespaceSerializer);

        byte[] serializedValue =
                kvState.getSerializedValue(
                        serializedKeyAndNamespace,
                        kvState.getKeySerializer(),
                        kvState.getNamespaceSerializer(),
                        kvState.getValueSerializer());

        if (serializedValue == null) {
            return null;
        } else {
            return KvStateSerializer.deserializeValue(serializedValue, valueSerializer);
        }
    }

    /** Returns the value by getting the serialized value and deserializing it if it is not null. */
    private static <V, K, N> List<V> getSerializedList(
            InternalKvState<K, N, V> kvState,
            K key,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer)
            throws Exception {

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key, keySerializer, namespace, namespaceSerializer);

        byte[] serializedValue =
                kvState.getSerializedValue(
                        serializedKeyAndNamespace,
                        kvState.getKeySerializer(),
                        kvState.getNamespaceSerializer(),
                        kvState.getValueSerializer());

        if (serializedValue == null) {
            return null;
        } else {
            return KvStateSerializer.deserializeList(serializedValue, valueSerializer);
        }
    }

    /** Returns the value by getting the serialized value and deserializing it if it is not null. */
    private static <UK, UV, K, N> Map<UK, UV> getSerializedMap(
            InternalKvState<K, N, Map<UK, UV>> kvState,
            K key,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer)
            throws Exception {

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key, keySerializer, namespace, namespaceSerializer);

        byte[] serializedValue =
                kvState.getSerializedValue(
                        serializedKeyAndNamespace,
                        kvState.getKeySerializer(),
                        kvState.getNamespaceSerializer(),
                        kvState.getValueSerializer());

        if (serializedValue == null) {
            return null;
        } else {
            return KvStateSerializer.deserializeMap(
                    serializedValue, userKeySerializer, userValueSerializer);
        }
    }

    public static KeyedStateHandle runSnapshot(
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

    public static class TestPojo implements Serializable {
        private String strField;
        private Integer intField;

        private TestNestedPojoClassA kryoClassAField;
        private TestNestedPojoClassB kryoClassBField;

        public TestPojo() {}

        public TestPojo(String strField, Integer intField) {
            this.strField = strField;
            this.intField = intField;
            this.kryoClassAField = null;
            this.kryoClassBField = null;
        }

        public TestPojo(
                String strField,
                Integer intField,
                TestNestedPojoClassA classAField,
                TestNestedPojoClassB classBfield) {
            this.strField = strField;
            this.intField = intField;
            this.kryoClassAField = classAField;
            this.kryoClassBField = classBfield;
        }

        public String getStrField() {
            return strField;
        }

        public void setStrField(String strField) {
            this.strField = strField;
        }

        public Integer getIntField() {
            return intField;
        }

        public void setIntField(Integer intField) {
            this.intField = intField;
        }

        public TestNestedPojoClassA getKryoClassAField() {
            return kryoClassAField;
        }

        public void setKryoClassAField(TestNestedPojoClassA kryoClassAField) {
            this.kryoClassAField = kryoClassAField;
        }

        public TestNestedPojoClassB getKryoClassBField() {
            return kryoClassBField;
        }

        public void setKryoClassBField(TestNestedPojoClassB kryoClassBField) {
            this.kryoClassBField = kryoClassBField;
        }

        @Override
        public String toString() {
            return "TestPojo{" + "strField='" + strField + '\'' + ", intField=" + intField + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestPojo testPojo = (TestPojo) o;

            return strField.equals(testPojo.strField)
                    && intField.equals(testPojo.intField)
                    && ((kryoClassAField == null && testPojo.kryoClassAField == null)
                            || kryoClassAField.equals(testPojo.kryoClassAField))
                    && ((kryoClassBField == null && testPojo.kryoClassBField == null)
                            || kryoClassBField.equals(testPojo.kryoClassBField));
        }

        @Override
        public int hashCode() {
            int result = strField.hashCode();
            result = 31 * result + intField.hashCode();

            if (kryoClassAField != null) {
                result = 31 * result + kryoClassAField.hashCode();
            }

            if (kryoClassBField != null) {
                result = 31 * result + kryoClassBField.hashCode();
            }

            return result;
        }
    }

    public static class TestNestedPojoClassA implements Serializable {
        private Double doubleField;
        private Integer intField;

        public TestNestedPojoClassA() {}

        public TestNestedPojoClassA(Double doubleField, Integer intField) {
            this.doubleField = doubleField;
            this.intField = intField;
        }

        public Double getDoubleField() {
            return doubleField;
        }

        public void setDoubleField(Double doubleField) {
            this.doubleField = doubleField;
        }

        public Integer getIntField() {
            return intField;
        }

        public void setIntField(Integer intField) {
            this.intField = intField;
        }

        @Override
        public String toString() {
            return "TestNestedPojoClassA{"
                    + "doubleField='"
                    + doubleField
                    + '\''
                    + ", intField="
                    + intField
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestNestedPojoClassA testNestedPojoClassA = (TestNestedPojoClassA) o;

            if (!doubleField.equals(testNestedPojoClassA.doubleField)) return false;
            return intField.equals(testNestedPojoClassA.intField);
        }

        @Override
        public int hashCode() {
            int result = doubleField.hashCode();
            result = 31 * result + intField.hashCode();
            return result;
        }
    }

    public static class TestNestedPojoClassB implements Serializable {
        private Double doubleField;
        private String strField;

        public TestNestedPojoClassB() {}

        public TestNestedPojoClassB(Double doubleField, String strField) {
            this.doubleField = doubleField;
            this.strField = strField;
        }

        public Double getDoubleField() {
            return doubleField;
        }

        public void setDoubleField(Double doubleField) {
            this.doubleField = doubleField;
        }

        public String getStrField() {
            return strField;
        }

        public void setStrField(String strField) {
            this.strField = strField;
        }

        @Override
        public String toString() {
            return "TestNestedPojoClassB{"
                    + "doubleField='"
                    + doubleField
                    + '\''
                    + ", strField="
                    + strField
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestNestedPojoClassB testNestedPojoClassB = (TestNestedPojoClassB) o;

            if (!doubleField.equals(testNestedPojoClassB.doubleField)) return false;
            return strField.equals(testNestedPojoClassB.strField);
        }

        @Override
        public int hashCode() {
            int result = doubleField.hashCode();
            result = 31 * result + strField.hashCode();
            return result;
        }
    }

    /** We throw this in our {@link ExceptionThrowingTestSerializer}. */
    private static class ExpectedKryoTestException extends RuntimeException {}

    /**
     * Kryo {@code Serializer} that throws an expected exception. We use this to ensure that the
     * state backend correctly uses a specified Kryo serializer.
     */
    public static class ExceptionThrowingTestSerializer extends JavaSerializer {
        @Override
        public void write(Kryo kryo, Output output, Object object) {
            throw new ExpectedKryoTestException();
        }

        @Override
        public Object read(Kryo kryo, Input input, Class type) {
            throw new ExpectedKryoTestException();
        }
    }

    /**
     * Our custom version of {@link JavaSerializer} for checking whether restore with a registered
     * serializer works when no serializer was previously registered.
     *
     * <p>This {@code Serializer} can only be used for writing, not for reading. With this we verify
     * that state that was serialized without a registered {@code Serializer} is in fact not
     * restored with a {@code Serializer} that was later registered.
     */
    public static class CustomKryoTestSerializer extends JavaSerializer {
        @Override
        public void write(Kryo kryo, Output output, Object object) {
            super.write(kryo, output, object);
        }

        @Override
        public Object read(Kryo kryo, Input input, Class type) {
            throw new ExpectedKryoTestException();
        }
    }

    @SuppressWarnings("serial")
    public static class MutableAggregatingAddingFunction
            implements AggregateFunction<Long, MutableLong, Long> {

        @Override
        public MutableLong createAccumulator() {
            return new MutableLong();
        }

        @Override
        public MutableLong add(Long value, MutableLong accumulator) {
            accumulator.value += value;
            return accumulator;
        }

        @Override
        public Long getResult(MutableLong accumulator) {
            return accumulator.value;
        }

        @Override
        public MutableLong merge(MutableLong a, MutableLong b) {
            a.value += b.value;
            return a;
        }
    }

    @SuppressWarnings("serial")
    private static class ImmutableAggregatingAddingFunction
            implements AggregateFunction<Long, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Long value, Long accumulator) {
            return accumulator += value;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static final class MutableLong {
        long value;
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

    /**
     * @return true if {@link org.apache.flink.runtime.state.Snapshotable#snapshot
     *     backend.snapshot()} actually uses passed {@link CheckpointStreamFactory}.
     */
    protected boolean snapshotUsesStreamFactory() {
        return true;
    }

    /**
     * @return true if metadata serialization supports verification. If not, expected exceptions
     *     will likely not be thrown.
     */
    protected boolean supportsMetaInfoVerification() {
        return true;
    }

    /** @return true if state backend is safe to reuse state. */
    protected boolean isSafeToReuseKVState() {
        return false;
    }
}
