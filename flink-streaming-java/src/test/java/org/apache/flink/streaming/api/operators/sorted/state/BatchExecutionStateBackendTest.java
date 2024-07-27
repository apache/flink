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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests copied over from {@link StateBackendTestBase} and adjusted to make sense for a single key
 * state backend.
 *
 * <p>Some of the tests in {@link StateBackendTestBase} do not make sense for {@link
 * BatchExecutionKeyedStateBackend}, e.g. checkpointing tests, tests verifying methods used by the
 * queryable state etc. Moreover the tests had to be adjusted as the state backend assumes keys are
 * grouped.
 */
class BatchExecutionStateBackendTest {

    private <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer) {
        return new BatchExecutionKeyedStateBackend<>(
                keySerializer, new KeyGroupRange(0, 9), new ExecutionConfig());
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing adding
     * {@code null}.
     */
    @Test
    void testListStateAddNull() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            assertThatThrownBy(() -> state.add(null)).isInstanceOf(NullPointerException.class);
        } finally {
            keyedBackend.close();
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#addAll(List)} to be called with {@code null} entries in the list of entries to add.
     */
    @Test
    void testListStateAddAllNullEntries() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

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
            keyedBackend.close();
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#addAll(List)} to be called with {@code null}.
     */
    @Test
    void testListStateAddAllNull() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            assertThatThrownBy(() -> state.addAll(null)).isInstanceOf(NullPointerException.class);
        } finally {
            keyedBackend.close();
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#update(List)} to be called with {@code null} entries in the list of entries to add.
     */
    @Test
    void testListStateUpdateNullEntries() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

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
            keyedBackend.close();
            keyedBackend.dispose();
        }
    }

    /**
     * This test verifies that all ListState implementations are consistent in not allowing {@link
     * ListState#update(List)} to be called with {@code null}.
     */
    @Test
    void testListStateUpdateNull() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertThat(state.get()).isNull();

            assertThatThrownBy(() -> state.update(null)).isInstanceOf(NullPointerException.class);
        } finally {
            keyedBackend.close();
            keyedBackend.dispose();
        }
    }

    @Test
    void testListStateAPIs() throws Exception {

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

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
        }
    }

    @Test
    void testListStateMergingOverThreeNamespaces() throws Exception {

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalListState<String, Integer, Long> state =
                    (InternalListState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testListStateMergingWithEmptyNamespace() throws Exception {

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalListState<String, Integer, Long> state =
                    (InternalListState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - def spreads the values over two namespaces (one empty)

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testListStateMergingEmpty() throws Exception {

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalListState<String, Integer, Long> state =
                    (InternalListState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - ghi is empty

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("ghi");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testListStateMergingAllInTargetNamespace() throws Exception {

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalListState<String, Integer, Long> state =
                    (InternalListState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - jkl has all elements already in the target namespace

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testListStateMergingInASingleNamespace() throws Exception {

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalListState<String, Integer, Long> state =
                    (InternalListState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).containsExactlyInAnyOrder(11L, 22L, 33L, 44L, 55L);

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testReducingStateAddAndGet() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", Long::sum, Long.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            ReducingState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();
            state.add(17L);
            state.add(11L);
            assertThat(state.get()).isEqualTo(28L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isEqualTo(28L);
            state.clear();
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isNull();
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isEqualTo(9L);
        }
    }

    @Test
    void testReducingStateMergingOverThreeNamespaces() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", Long::sum, Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            final InternalReducingState<String, Integer, Long> state =
                    (InternalReducingState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            keyedBackend.setCurrentKey("abc");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testReducingStateMergingWithEmpty() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", Long::sum, Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            final InternalReducingState<String, Integer, Long> state =
                    (InternalReducingState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - def spreads the values over two namespaces (one empty)

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testReducingStateMergingEmpty() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", Long::sum, Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            final InternalReducingState<String, Integer, Long> state =
                    (InternalReducingState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - ghi is empty

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testReducingStateMergingInTargetNamespace() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", Long::sum, Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            final InternalReducingState<String, Integer, Long> state =
                    (InternalReducingState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - jkl has all elements already in the target namespace

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testReducingStateMergingInASingleNamespace() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", Long::sum, Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            final InternalReducingState<String, Integer, Long> state =
                    (InternalReducingState<String, Integer, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateAddAndGetWithMutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            AggregatingState<Long, Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();
            state.add(17L);
            state.add(11L);
            assertThat(state.get()).isEqualTo(28L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isEqualTo(28L);
            state.clear();
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isNull();
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isEqualTo(9L);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithMutableAccumulatorOverThreeNamespaces() throws Exception {
        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            keyedBackend.setCurrentKey("abc");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithMutableAccumulatorWithEmpty() throws Exception {
        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - def spreads the values over two namespaces (one empty)

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithMutableAccumulatorEmpty() throws Exception {
        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - ghi is empty

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithMutableAccumulatorInTargetNamespace() throws Exception {
        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - jkl has all elements already in the target namespace

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithMutableAccumulatorInASingleNamespace() throws Exception {
        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateAddAndGetWithImmutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            AggregatingState<Long, Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isNull();
            state.add(17L);
            state.add(11L);
            assertThat(state.get()).isEqualTo(28L);

            keyedBackend.setCurrentKey("def");
            assertThat(state.get()).isEqualTo(28L);
            state.clear();
            assertThat(state.get()).isNull();

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isNull();
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("g");
            assertThat(state.get()).isEqualTo(9L);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithImmutableAccumulatorOverThreeNamespaces() throws Exception {
        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - abc spreads the values over three namespaces

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.add(33L);
            state.add(55L);

            state.setCurrentNamespace(namespace2);
            state.add(22L);
            state.add(11L);

            state.setCurrentNamespace(namespace3);
            state.add(44L);

            keyedBackend.setCurrentKey("abc");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithImmutableAccumulatorWithEmpty() throws Exception {
        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - def spreads the values over two namespaces (one empty)

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(44L);

            state.setCurrentNamespace(namespace3);
            state.add(22L);
            state.add(55L);
            state.add(33L);

            keyedBackend.setCurrentKey("def");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithImmutableAccumulatorEmpty() throws Exception {
        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - ghi is empty

            keyedBackend.setCurrentKey("ghi");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithImmutableAccumulatorInTargetNamespace() throws Exception {
        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - jkl has all elements already in the target namespace

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("jkl");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
    void testAggregatingStateMergingWithImmutableAccumulatorInASingleNamespace() throws Exception {
        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        final Long expectedResult = 165L;

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            InternalAggregatingState<String, Integer, Long, Long, Long> state =
                    (InternalAggregatingState<String, Integer, Long, Long, Long>)
                            keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

            // populate the different namespaces
            //  - mno has all elements already in one source namespace

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            state.add(11L);
            state.add(22L);
            state.add(33L);
            state.add(44L);
            state.add(55L);

            keyedBackend.setCurrentKey("mno");
            state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
            state.setCurrentNamespace(namespace1);
            assertThat(state.get()).isEqualTo(expectedResult);

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertThat(state.get()).isNull();
        }
    }

    @Test
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
            backend.dispose();
        }
    }

    /**
     * Verify iterator of {@link MapState} supporting arbitrary access, see [FLINK-10267] to know
     * more details.
     */
    @Test
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
            backend.dispose();
        }
    }

    /** Verify that {@link ValueStateDescriptor} allows {@code null} as default. */
    @Test
    void testValueStateNullAsDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);

        ValueState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertThat(state.value()).isNull();

        state.update("Ciao");
        assertThat(state.value()).isEqualTo("Ciao");

        state.clear();
        assertThat(state.value()).isNull();

        backend.dispose();
    }

    /** Verify that an empty {@code ValueState} will yield the default value. */
    @Test
    void testValueStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, "Hello");

        ValueState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertThat(state.value()).isEqualTo("Hello");

        state.update("Ciao");
        assertThat(state.value()).isEqualTo("Ciao");

        state.clear();
        assertThat(state.value()).isEqualTo("Hello");

        backend.dispose();
    }

    /** Verify that an empty {@code ReduceState} yields {@code null}. */
    @Test
    void testReducingStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ReducingStateDescriptor<String> kvId =
                new ReducingStateDescriptor<>("id", new AppendingReduce(), String.class);

        ReducingState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertThat(state.get()).isNull();

        state.add("Ciao");
        assertThat(state.get()).isEqualTo("Ciao");

        state.clear();
        assertThat(state.get()).isNull();

        backend.dispose();
    }

    /** Verify that an empty {@code ListState} yields {@code null}. */
    @Test
    void testListStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

        ListState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertThat(state.get()).isNull();

        state.update(Arrays.asList("Ciao", "Bello"));
        assertThat(state.get()).containsExactlyInAnyOrder("Ciao", "Bello");

        state.clear();
        assertThat(state.get()).isNull();

        backend.dispose();
    }

    /** Verify that an empty {@code MapState} yields {@code null}. */
    @Test
    void testMapStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        MapStateDescriptor<String, String> kvId =
                new MapStateDescriptor<>("id", String.class, String.class);

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

        backend.dispose();
    }

    private static class AppendingReduce implements ReduceFunction<String> {
        @Override
        public String reduce(String value1, String value2) throws Exception {
            return value1 + "," + value2;
        }
    }

    @SuppressWarnings("serial")
    private static class MutableAggregatingAddingFunction
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

    private static final class MutableLong {
        long value;
    }
}
