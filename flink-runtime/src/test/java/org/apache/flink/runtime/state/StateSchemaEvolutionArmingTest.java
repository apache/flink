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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.StateSchemaEvolvingTestSerializer.StateSchemaEvolvingTestTypeInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the backend capability gate on state schema evolution arming: only a keyed backend that
 * performs object-level value migration may hand a state its armed value serializer.
 *
 * <p>Assertions are made on the serializer object the state descriptor ends up holding, so a gate
 * that is bypassed shows up as an armed serializer rather than having to be read out of the code
 * path.
 */
class StateSchemaEvolutionArmingTest {

    /**
     * Mirrors {@code ExecutionConfigOptions.TABLE_EXEC_STATE_SCHEMA_EVOLUTION_ENABLED}, which lives
     * in a module this one cannot depend on.
     */
    private static final ConfigOption<Boolean> STATE_SCHEMA_EVOLUTION_ENABLED =
            ConfigOptions.key("table.exec.state.schema-evolution.enabled")
                    .booleanType()
                    .defaultValue(false);

    @Test
    void valueStateIsArmedOnAnObjectLevelMigratingBackend() {
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("value", new StateSchemaEvolvingTestTypeInfo());

        keyedStateStore(true).getState(descriptor);

        assertThat(armedFlagOf(descriptor.getSerializer())).isTrue();
    }

    @Test
    void valueStateIsNotArmedOnABackendWithoutObjectLevelMigration() {
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("value", new StateSchemaEvolvingTestTypeInfo());

        keyedStateStore(false).getState(descriptor);

        assertThat(armedFlagOf(descriptor.getSerializer())).isFalse();
    }

    @Test
    void v2ValueStateIsNotArmed() {
        org.apache.flink.api.common.state.v2.ValueStateDescriptor<Integer> descriptor =
                new org.apache.flink.api.common.state.v2.ValueStateDescriptor<>(
                        "value", new StateSchemaEvolvingTestTypeInfo());

        asyncKeyedStateStore().getValueState(descriptor);

        assertThat(armedFlagOf(descriptor.getSerializer())).isFalse();
    }

    @Test
    void operatorListStateIsNotArmed() throws Exception {
        ListStateDescriptor<Integer> descriptor =
                new ListStateDescriptor<>("list", new StateSchemaEvolvingTestTypeInfo());

        operatorStateBackend().getListState(descriptor);

        assertThat(armedFlagOf(descriptor.getElementSerializer())).isFalse();
    }

    @Test
    void broadcastStateIsNotArmed() throws Exception {
        MapStateDescriptor<Integer, Integer> descriptor =
                new MapStateDescriptor<>(
                        "broadcast", Types.INT, new StateSchemaEvolvingTestTypeInfo());

        operatorStateBackend().getBroadcastState(descriptor);

        assertThat(armedFlagOf(descriptor.getValueSerializer())).isFalse();
    }

    private static boolean armedFlagOf(TypeSerializer<Integer> serializer) {
        assertThat(serializer).isInstanceOf(StateSchemaEvolvingTestSerializer.class);
        return ((StateSchemaEvolvingTestSerializer) serializer).isArmed();
    }

    private static DefaultKeyedStateStore keyedStateStore(boolean objectLevelValueMigration) {
        return new DefaultKeyedStateStore(
                new TestKeyedStateBackend(objectLevelValueMigration), serializerFactory());
    }

    private static DefaultKeyedStateStore asyncKeyedStateStore() {
        DefaultKeyedStateStore store =
                new DefaultKeyedStateStore(
                        new TestKeyedStateBackend(true),
                        new TestAsyncKeyedStateBackend(),
                        serializerFactory());
        store.setSupportKeyedStateApiSetV2();
        return store;
    }

    private static DefaultOperatorStateBackend operatorStateBackend() throws Exception {
        return new DefaultOperatorStateBackendBuilder(
                        StateSchemaEvolutionArmingTest.class.getClassLoader(),
                        new ExecutionConfig(configurationWithSchemaEvolutionEnabled()),
                        false,
                        Collections.emptyList(),
                        new CloseableRegistry())
                .build();
    }

    private static SerializerFactory serializerFactory() {
        SerializerConfig config =
                new SerializerConfigImpl(configurationWithSchemaEvolutionEnabled());
        return new SerializerFactory() {
            @Override
            public <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInformation) {
                return typeInformation.createSerializer(config);
            }
        };
    }

    private static Configuration configurationWithSchemaEvolutionEnabled() {
        Configuration configuration = new Configuration();
        configuration.set(STATE_SCHEMA_EVOLUTION_ENABLED, true);
        return configuration;
    }

    /**
     * A keyed backend whose object-level value migration capability is fixed per instance. {@link
     * DefaultKeyedStateStore} only reads that capability and forwards the descriptor to {@link
     * #getPartitionedState}, so every other method is left unsupported.
     */
    private static final class TestKeyedStateBackend implements KeyedStateBackend<Integer> {

        private final boolean objectLevelValueMigration;

        private TestKeyedStateBackend(boolean objectLevelValueMigration) {
            this.objectLevelValueMigration = objectLevelValueMigration;
        }

        @Override
        public boolean supportsObjectLevelValueMigration() {
            return objectLevelValueMigration;
        }

        @Override
        public <N, S extends State> S getPartitionedState(
                N namespace,
                TypeSerializer<N> namespaceSerializer,
                StateDescriptor<S, ?> stateDescriptor) {
            return null;
        }

        @Override
        public String getBackendTypeIdentifier() {
            return "test";
        }

        @Override
        public void setCurrentKey(Integer newKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Integer getCurrentKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCurrentKeyAndKeyGroup(Integer newKey, int newKeyGroupIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeSerializer<Integer> getKeySerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <N, S extends State, T> void applyToAllKeys(
                N namespace,
                TypeSerializer<N> namespaceSerializer,
                StateDescriptor<S, T> stateDescriptor,
                KeyedStateFunction<Integer, S> function) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <N> Stream<Integer> getKeys(String state, N namespace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <N> Stream<Integer> getKeys(List<String> states, N namespace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <N> Stream<Tuple2<Integer, N>> getKeysAndNamespaces(String state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <N, S extends State, T> S getOrCreateKeyedState(
                TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dispose() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerKeySelectionListener(KeySelectionListener<Integer> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deregisterKeySelectionListener(KeySelectionListener<Integer> listener) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
                @Nonnull TypeSerializer<N> namespaceSerializer,
                @Nonnull StateDescriptor<S, SV> stateDesc,
                @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @Nonnull String stateName,
                        @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * An async keyed backend that accepts a v2 descriptor and returns no state, so the v2 path
     * through {@link DefaultKeyedStateStore} can be exercised without a real backend.
     */
    private static final class TestAsyncKeyedStateBackend
            implements AsyncKeyedStateBackend<Integer> {

        @Override
        public <N, S extends org.apache.flink.api.common.state.v2.State, SV>
                S getOrCreateKeyedState(
                        N defaultNamespace,
                        TypeSerializer<N> namespaceSerializer,
                        org.apache.flink.api.common.state.v2.StateDescriptor<SV> stateDesc) {
            return null;
        }

        @Override
        public String getBackendTypeIdentifier() {
            return "test";
        }

        @Override
        public void setup(@Nonnull StateRequestHandler stateRequestHandler) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public <N, S extends InternalKeyedState, SV> S createStateInternal(
                @Nonnull N defaultNamespace,
                @Nonnull TypeSerializer<N> namespaceSerializer,
                @Nonnull org.apache.flink.api.common.state.v2.StateDescriptor<SV> stateDesc) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public StateExecutor createStateExecutor() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dispose() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyCheckpointSubsumed(long checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                CheckpointStreamFactory streamFactory,
                CheckpointOptions checkpointOptions) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @Nonnull String stateName,
                        @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            throw new UnsupportedOperationException();
        }
    }
}
