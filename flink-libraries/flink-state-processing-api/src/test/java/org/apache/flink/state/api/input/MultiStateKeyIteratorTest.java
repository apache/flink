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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.mock.MockRestoreOperation;
import org.apache.flink.runtime.state.ttl.mock.MockStateBackend;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Test for the multi-state key iterator. */
public class MultiStateKeyIteratorTest {
    private static final List<ValueStateDescriptor<Integer>> descriptors;

    static {
        descriptors = new ArrayList<>(2);
        descriptors.add(new ValueStateDescriptor<>("state-1", Types.INT));
        descriptors.add(new ValueStateDescriptor<>("state-2", Types.INT));
    }

    private static AbstractKeyedStateBackend<Integer> createKeyedStateBackend() {
        MockStateBackend backend = new MockStateBackend();

        Environment env = new DummyEnvironment();
        JobID jobID = new JobID();
        KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 128);
        MetricGroup metricGroup = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        return backend.createKeyedStateBackend(
                new KeyedStateBackendParametersImpl<>(
                        env,
                        jobID,
                        "mock-backend",
                        IntSerializer.INSTANCE,
                        129,
                        keyGroupRange,
                        (TaskKvStateRegistry) null,
                        TtlTimeProvider.DEFAULT,
                        metricGroup,
                        Collections.emptyList(),
                        cancelStreamRegistry));
    }

    private static CountingKeysKeyedStateBackend createCountingKeysKeyedStateBackend(
            Integer numKeys) {
        Environment env = new DummyEnvironment();
        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        int numberOfKeyGroups = 129;
        KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 128);
        TaskKvStateRegistry kvStateRegistry = null;
        TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;
        @Nonnull Collection<KeyedStateHandle> stateHandles = Collections.emptyList();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        Map<String, Map<Integer, Map<Object, Object>>> stateValues = new HashMap<>();
        MockRestoreOperation<Integer> restoreOperation =
                new MockRestoreOperation<>(stateHandles, stateValues);
        restoreOperation.restore();

        StateSerializerProvider<Integer> keySerializerProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(keySerializer);

        return new CountingKeysKeyedStateBackend(
                numKeys,
                kvStateRegistry,
                keySerializerProvider.currentSchemaSerializer(),
                env.getUserCodeClassLoader().asClassLoader(),
                env.getExecutionConfig(),
                ttlTimeProvider,
                LatencyTrackingStateConfig.disabled(),
                cancelStreamRegistry,
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups));
    }

    private static void setKey(
            AbstractKeyedStateBackend<Integer> backend,
            ValueStateDescriptor<Integer> descriptor,
            int key)
            throws Exception {
        backend.setCurrentKey(key);
        backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor)
                .update(0);
    }

    private static void clearKey(
            AbstractKeyedStateBackend<Integer> backend,
            ValueStateDescriptor<Integer> descriptor,
            int key)
            throws Exception {
        backend.setCurrentKey(key);
        backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor)
                .clear();
    }

    @Test
    public void testIteratorPullsKeyFromAllDescriptors() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedStateBackend = createKeyedStateBackend();

        setKey(keyedStateBackend, descriptors.get(0), 1);
        setKey(keyedStateBackend, descriptors.get(1), 2);

        MultiStateKeyIterator<Integer> iterator =
                new MultiStateKeyIterator<>(descriptors, keyedStateBackend);

        List<Integer> keys = new ArrayList<>();

        while (iterator.hasNext()) {
            keys.add(iterator.next());
        }

        Assert.assertEquals("Unexpected number of keys", 2, keys.size());
        Assert.assertEquals("Unexpected keys found", Arrays.asList(1, 2), keys);
    }

    @Test
    public void testIteratorSkipsEmptyDescriptors() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedStateBackend = createKeyedStateBackend();

        List<ValueStateDescriptor<Integer>> threeDescriptors = new ArrayList<>(3);
        threeDescriptors.add(new ValueStateDescriptor<>("state-1", Types.INT));
        threeDescriptors.add(new ValueStateDescriptor<>("state-2", Types.INT));
        threeDescriptors.add(new ValueStateDescriptor<>("state-3", Types.INT));

        setKey(keyedStateBackend, threeDescriptors.get(0), 1);

        // initializes descriptor 1, but empties it immediately after
        setKey(keyedStateBackend, threeDescriptors.get(1), 12);
        clearKey(keyedStateBackend, threeDescriptors.get(1), 12);

        setKey(keyedStateBackend, threeDescriptors.get(2), 2);

        MultiStateKeyIterator<Integer> iterator =
                new MultiStateKeyIterator<>(threeDescriptors, keyedStateBackend);

        List<Integer> keys = new ArrayList<>();

        while (iterator.hasNext()) {
            keys.add(iterator.next());
        }

        Assert.assertEquals("Unexpected number of keys", 2, keys.size());
        Assert.assertEquals("Unexpected keys found", Arrays.asList(1, 2), keys);
    }

    @Test
    public void testIteratorRemovesFromAllDescriptors() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedStateBackend = createKeyedStateBackend();

        setKey(keyedStateBackend, descriptors.get(0), 1);
        setKey(keyedStateBackend, descriptors.get(1), 1);

        MultiStateKeyIterator<Integer> iterator =
                new MultiStateKeyIterator<>(descriptors, keyedStateBackend);

        int key = iterator.next();
        Assert.assertEquals("Unexpected keys pulled from state backend", 1, key);

        iterator.remove();
        Assert.assertFalse(
                "Failed to drop key from all descriptors in state backend", iterator.hasNext());

        for (StateDescriptor<?, ?> descriptor : descriptors) {
            Assert.assertEquals(
                    "Failed to drop key for state descriptor",
                    0,
                    keyedStateBackend
                            .getKeys(descriptor.getName(), VoidNamespace.INSTANCE)
                            .count());
        }
    }

    /** Test for lazy enumeration of inner iterators. */
    @Test
    public void testIteratorPullsSingleKeyFromAllDescriptors() throws AssertionError {
        CountingKeysKeyedStateBackend keyedStateBackend =
                createCountingKeysKeyedStateBackend(100_000_000);
        MultiStateKeyIterator<Integer> testedIterator =
                new MultiStateKeyIterator<>(descriptors, keyedStateBackend);

        testedIterator.hasNext();

        Assert.assertEquals(
                "Unexpected number of keys enumerated",
                1,
                keyedStateBackend.numberOfKeysEnumerated);
    }

    /**
     * Mockup {@link AbstractKeyedStateBackend} that counts how many keys are enumerated.
     *
     * <p>Generates a configured number of integer keys, only method actually implemented is {@link
     * CountingKeysKeyedStateBackend#getKeys(java.lang.String, java.lang.Object)}
     */
    static class CountingKeysKeyedStateBackend extends AbstractKeyedStateBackend<Integer> {
        int numberOfKeysGenerated;
        public long numberOfKeysEnumerated;

        public CountingKeysKeyedStateBackend(
                int numberOfKeysGenerated,
                TaskKvStateRegistry kvStateRegistry,
                TypeSerializer<Integer> keySerializer,
                ClassLoader userCodeClassLoader,
                ExecutionConfig executionConfig,
                TtlTimeProvider ttlTimeProvider,
                LatencyTrackingStateConfig latencyTrackingStateConfig,
                CloseableRegistry cancelStreamRegistry,
                InternalKeyContext<Integer> keyContext) {
            super(
                    kvStateRegistry,
                    keySerializer,
                    userCodeClassLoader,
                    executionConfig,
                    ttlTimeProvider,
                    latencyTrackingStateConfig,
                    cancelStreamRegistry,
                    keyContext);
            this.numberOfKeysGenerated = numberOfKeysGenerated;
            numberOfKeysEnumerated = 0;
        }

        @Override
        public <N> Stream<Integer> getKeys(String state, N namespace) {
            return IntStream.range(0, this.numberOfKeysGenerated)
                    .boxed()
                    .peek(i -> numberOfKeysEnumerated++);
        }

        @Override
        public int numKeyValueStateEntries() {
            return numberOfKeysGenerated;
        }

        @Nonnull
        @Override
        public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @Nonnull String stateName,
                        @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            throw new UnsupportedOperationException(
                    "Operations other than getKeys() are not supported on this testing StateBackend.");
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Nonnull
        @Override
        public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                @Nonnull CheckpointStreamFactory streamFactory,
                @Nonnull CheckpointOptions checkpointOptions)
                throws UnsupportedOperationException {
            throw new UnsupportedOperationException(
                    "Operations other than getKeys() are not supported on this testing StateBackend.");
        }

        @Nonnull
        @Override
        public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
                @Nonnull TypeSerializer<N> namespaceSerializer,
                @Nonnull StateDescriptor<S, SV> stateDesc,
                @Nonnull
                        StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                                snapshotTransformFactory)
                throws UnsupportedOperationException {
            throw new UnsupportedOperationException(
                    "Operations other than getKeys() are not supported on this testing StateBackend.");
        }

        @Override
        public <N> Stream<Tuple2<Integer, N>> getKeysAndNamespaces(String state) {
            throw new UnsupportedOperationException(
                    "Operations other than getKeys() are not supported on this testing StateBackend.");
        }

        @Nonnull
        @Override
        public SavepointResources<Integer> savepoint() throws UnsupportedOperationException {
            throw new UnsupportedOperationException(
                    "Operations other than getKeys() are not supported on this testing StateBackend.");
        }
    }
}
