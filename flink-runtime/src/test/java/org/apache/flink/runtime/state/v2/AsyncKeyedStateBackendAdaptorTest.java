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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AsyncKeyedStateBackendAdaptor}. */
public class AsyncKeyedStateBackendAdaptorTest {

    @Test
    public void testAdapt() throws Exception {
        CheckpointableKeyedStateBackend<Integer> keyedStateBackend = new TestKeyedStateBackend();
        AsyncKeyedStateBackendAdaptor<Integer> adaptor =
                new AsyncKeyedStateBackendAdaptor<>(keyedStateBackend);
        StateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("testState", BasicTypeInfo.INT_TYPE_INFO);

        org.apache.flink.api.common.state.v2.ValueState<Integer> valueState =
                adaptor.createState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        // test synchronous interfaces.
        valueState.clear();
        assertThat(valueState.value()).isNull();
        valueState.update(10);
        assertThat(valueState.value()).isEqualTo(10);

        // test asynchronous interfaces.
        valueState
                .asyncClear()
                .thenAccept(
                        clear -> {
                            assertThat(valueState.value()).isNull();
                            valueState
                                    .asyncUpdate(20)
                                    .thenCompose(
                                            empty -> {
                                                assertThat(valueState.value()).isEqualTo(20);
                                                return valueState.asyncValue();
                                            })
                                    .thenAccept(
                                            value -> {
                                                assertThat(value).isEqualTo(20);
                                            });
                        });
    }

    static class TestValueState implements ValueState<Integer> {
        private Integer value;

        TestValueState() {
            this.value = null;
        }

        @Override
        public Integer value() throws IOException {
            return value;
        }

        @Override
        public void update(Integer value) throws IOException {
            this.value = value;
        }

        @Override
        public void clear() {
            this.value = null;
        }
    }

    static class TestKeyedStateBackend implements CheckpointableKeyedStateBackend<Integer> {

        @Override
        public void setCurrentKey(Integer newKey) {}

        @Override
        public Integer getCurrentKey() {
            return 0;
        }

        @Override
        public TypeSerializer<Integer> getKeySerializer() {
            return null;
        }

        @Override
        public <N, S extends State, T> void applyToAllKeys(
                N namespace,
                TypeSerializer<N> namespaceSerializer,
                org.apache.flink.api.common.state.StateDescriptor<S, T> stateDescriptor,
                KeyedStateFunction<Integer, S> function)
                throws Exception {}

        @Override
        public <N> Stream<Integer> getKeys(String state, N namespace) {
            return Stream.empty();
        }

        @Override
        public <N> Stream<Tuple2<Integer, N>> getKeysAndNamespaces(String state) {
            return Stream.empty();
        }

        @Override
        public <N, S extends State, T> S getOrCreateKeyedState(
                TypeSerializer<N> namespaceSerializer,
                org.apache.flink.api.common.state.StateDescriptor<S, T> stateDescriptor)
                throws Exception {
            switch (stateDescriptor.getType()) {
                case VALUE:
                    return (S) new TestValueState();
                default:
                    throw new IllegalArgumentException(
                            "Unsupported state type: " + stateDescriptor.getType());
            }
        }

        @Override
        public <N, S extends State> S getPartitionedState(
                N namespace,
                TypeSerializer<N> namespaceSerializer,
                org.apache.flink.api.common.state.StateDescriptor<S, ?> stateDescriptor)
                throws Exception {
            return null;
        }

        @Override
        public void dispose() {}

        @Override
        public void registerKeySelectionListener(KeySelectionListener<Integer> listener) {}

        @Override
        public boolean deregisterKeySelectionListener(KeySelectionListener<Integer> listener) {
            return false;
        }

        @Nonnull
        @Override
        public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
                @Nonnull TypeSerializer<N> namespaceSerializer,
                @Nonnull org.apache.flink.api.common.state.StateDescriptor<S, SV> stateDesc,
                @Nonnull
                        StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                                snapshotTransformFactory)
                throws Exception {
            return null;
        }

        @Nonnull
        @Override
        public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @Nonnull String stateName,
                        @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            return null;
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public SavepointResources<Integer> savepoint() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            // nothing to do
        }

        @Nonnull
        @Override
        public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                @Nonnull CheckpointStreamFactory streamFactory,
                @Nonnull CheckpointOptions checkpointOptions)
                throws Exception {
            throw new UnsupportedOperationException();
        }
    }
}
