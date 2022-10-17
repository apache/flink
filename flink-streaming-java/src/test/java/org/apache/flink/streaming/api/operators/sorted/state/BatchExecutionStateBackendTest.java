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
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests copied over from {@link StateBackendTestBase} and adjusted to make sense for a single key
 * state backend.
 *
 * <p>Some of the tests in {@link StateBackendTestBase} do not make sense for {@link
 * BatchExecutionKeyedStateBackend}, e.g. checkpointing tests, tests verifying methods used by the
 * queryable state etc. Moreover the tests had to be adjusted as the state backend assumes keys are
 * grouped.
 */
public class BatchExecutionStateBackendTest extends TestLogger {

    @Rule public final ExpectedException expectedException = ExpectedException.none();

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
    public void testListStateAddNull() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertNull(state.get());

            expectedException.expect(NullPointerException.class);
            state.add(null);
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
    public void testListStateAddAllNullEntries() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertNull(state.get());

            expectedException.expect(NullPointerException.class);

            List<Long> adding = new ArrayList<>();
            adding.add(3L);
            adding.add(null);
            adding.add(5L);
            state.addAll(adding);
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
    public void testListStateAddAllNull() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertNull(state.get());

            expectedException.expect(NullPointerException.class);
            state.addAll(null);
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
    public void testListStateUpdateNullEntries() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertNull(state.get());

            expectedException.expect(NullPointerException.class);

            List<Long> adding = new ArrayList<>();
            adding.add(3L);
            adding.add(null);
            adding.add(5L);
            state.update(adding);
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
    public void testListStateUpdateNull() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE);

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            assertNull(state.get());

            expectedException.expect(NullPointerException.class);
            state.update(null);
        } finally {
            keyedBackend.close();
            keyedBackend.dispose();
        }
    }

    @Test
    public void testListStateAPIs() throws Exception {

        final ListStateDescriptor<Long> stateDescr =
                new ListStateDescriptor<>("my-state", Long.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            ListState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("g");
            assertNull(state.get());
            assertNull(state.get());
            state.addAll(Collections.emptyList());
            assertNull(state.get());
            state.addAll(Arrays.asList(3L, 4L));
            assertThat(state.get(), containsInAnyOrder(3L, 4L));
            assertThat(state.get(), containsInAnyOrder(3L, 4L));
            state.addAll(new ArrayList<>());
            assertThat(state.get(), containsInAnyOrder(3L, 4L));
            state.addAll(Arrays.asList(5L, 6L));
            assertThat(state.get(), containsInAnyOrder(3L, 4L, 5L, 6L));
            state.addAll(new ArrayList<>());
            assertThat(state.get(), containsInAnyOrder(3L, 4L, 5L, 6L));

            assertThat(state.get(), containsInAnyOrder(3L, 4L, 5L, 6L));
            state.update(Arrays.asList(1L, 2L));
            assertThat(state.get(), containsInAnyOrder(1L, 2L));
        }
    }

    @Test
    public void testListStateMergingOverThreeNamespaces() throws Exception {

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
            assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testListStateMergingWithEmptyNamespace() throws Exception {

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
            assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testListStateMergingEmpty() throws Exception {

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
            assertNull(state.get());

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("ghi");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testListStateMergingAllInTargetNamespace() throws Exception {

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
            assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testListStateMergingInASingleNamespace() throws Exception {

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
            assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

            // make sure all lists / maps are cleared

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testReducingStateAddAndGet() throws Exception {

        final ReducingStateDescriptor<Long> stateDescr =
                new ReducingStateDescriptor<>("my-state", Long::sum, Long.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            ReducingState<Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("def");
            assertNull(state.get());
            state.add(17L);
            state.add(11L);
            assertEquals(28L, state.get().longValue());

            keyedBackend.setCurrentKey("def");
            assertEquals(28L, state.get().longValue());
            state.clear();
            assertNull(state.get());

            keyedBackend.setCurrentKey("g");
            assertNull(state.get());
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("g");
            assertEquals(9L, state.get().longValue());
        }
    }

    @Test
    public void testReducingStateMergingOverThreeNamespaces() throws Exception {

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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testReducingStateMergingWithEmpty() throws Exception {

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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testReducingStateMergingEmpty() throws Exception {

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
            assertNull(state.get());
        }
    }

    @Test
    public void testReducingStateMergingInTargetNamespace() throws Exception {

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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testReducingStateMergingInASingleNamespace() throws Exception {

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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateAddAndGetWithMutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            AggregatingState<Long, Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("def");
            assertNull(state.get());
            state.add(17L);
            state.add(11L);
            assertEquals(28L, state.get().longValue());

            keyedBackend.setCurrentKey("def");
            assertEquals(28L, state.get().longValue());
            state.clear();
            assertNull(state.get());

            keyedBackend.setCurrentKey("def");
            assertNull(state.get());

            keyedBackend.setCurrentKey("g");
            assertNull(state.get());
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("g");
            assertEquals(9L, state.get().longValue());
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithMutableAccumulatorOverThreeNamespaces()
            throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithMutableAccumulatorWithEmpty() throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithMutableAccumulatorEmpty() throws Exception {
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
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithMutableAccumulatorInTargetNamespace()
            throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithMutableAccumulatorInASingleNamespace()
            throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateAddAndGetWithImmutableAccumulator() throws Exception {

        final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
                new AggregatingStateDescriptor<>(
                        "my-state", new ImmutableAggregatingAddingFunction(), Long.class);

        try (CheckpointableKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StringSerializer.INSTANCE)) {
            AggregatingState<Long, Long> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("def");
            assertNull(state.get());
            state.add(17L);
            state.add(11L);
            assertEquals(28L, state.get().longValue());

            keyedBackend.setCurrentKey("def");
            assertEquals(28L, state.get().longValue());
            state.clear();
            assertNull(state.get());

            keyedBackend.setCurrentKey("g");
            assertNull(state.get());
            state.add(1L);
            state.add(2L);

            keyedBackend.setCurrentKey("g");
            state.add(3L);
            state.add(2L);
            state.add(1L);

            keyedBackend.setCurrentKey("g");
            assertEquals(9L, state.get().longValue());
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithImmutableAccumulatorOverThreeNamespaces()
            throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithImmutableAccumulatorWithEmpty() throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithImmutableAccumulatorEmpty() throws Exception {
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
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithImmutableAccumulatorInTargetNamespace()
            throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testAggregatingStateMergingWithImmutableAccumulatorInASingleNamespace()
            throws Exception {
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
            assertEquals(expectedResult, state.get());

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace1);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    public void testMapStateIsEmpty() throws Exception {
        MapStateDescriptor<Integer, Long> kvId =
                new MapStateDescriptor<>("id", Integer.class, Long.class);

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        try {
            MapState<Integer, Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            backend.setCurrentKey(1);
            assertTrue(state.isEmpty());

            int stateSize = 1024;
            for (int i = 0; i < stateSize; i++) {
                state.put(i, i * 2L);
                assertFalse(state.isEmpty());
            }

            for (int i = 0; i < stateSize; i++) {
                assertFalse(state.isEmpty());
                state.remove(i);
            }
            assertTrue(state.isEmpty());

        } finally {
            backend.dispose();
        }
    }

    /**
     * Verify iterator of {@link MapState} supporting arbitrary access, see [FLINK-10267] to know
     * more details.
     */
    @Test
    public void testMapStateIteratorArbitraryAccess() throws Exception {
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
                assertEquals(iteratorCount, (int) entry.getKey());
                switch (ThreadLocalRandom.current().nextInt() % 3) {
                    case 0: // remove twice
                        iterator.remove();
                        try {
                            iterator.remove();
                            fail();
                        } catch (IllegalStateException e) {
                            // ignore expected exception
                        }
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
            assertEquals(stateSize, iteratorCount);
        } finally {
            backend.dispose();
        }
    }

    /** Verify that {@link ValueStateDescriptor} allows {@code null} as default. */
    @Test
    public void testValueStateNullAsDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);

        ValueState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertNull(state.value());

        state.update("Ciao");
        assertEquals("Ciao", state.value());

        state.clear();
        assertNull(state.value());

        backend.dispose();
    }

    /** Verify that an empty {@code ValueState} will yield the default value. */
    @Test
    public void testValueStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, "Hello");

        ValueState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertEquals("Hello", state.value());

        state.update("Ciao");
        assertEquals("Ciao", state.value());

        state.clear();
        assertEquals("Hello", state.value());

        backend.dispose();
    }

    /** Verify that an empty {@code ReduceState} yields {@code null}. */
    @Test
    public void testReducingStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ReducingStateDescriptor<String> kvId =
                new ReducingStateDescriptor<>("id", new AppendingReduce(), String.class);

        ReducingState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertNull(state.get());

        state.add("Ciao");
        assertEquals("Ciao", state.get());

        state.clear();
        assertNull(state.get());

        backend.dispose();
    }

    /** Verify that an empty {@code ListState} yields {@code null}. */
    @Test
    public void testListStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

        ListState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertNull(state.get());

        state.update(Arrays.asList("Ciao", "Bello"));
        assertThat(state.get(), containsInAnyOrder("Ciao", "Bello"));

        state.clear();
        assertNull(state.get());

        backend.dispose();
    }

    /** Verify that an empty {@code MapState} yields {@code null}. */
    @Test
    public void testMapStateDefaultValue() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);

        MapStateDescriptor<String, String> kvId =
                new MapStateDescriptor<>("id", String.class, String.class);

        MapState<String, String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertNotNull(state.entries());
        assertFalse(state.entries().iterator().hasNext());

        state.put("Ciao", "Hello");
        state.put("Bello", "Nice");

        assertNotNull(state.entries());
        assertEquals(state.get("Ciao"), "Hello");
        assertEquals(state.get("Bello"), "Nice");

        state.clear();
        assertNotNull(state.entries());
        assertFalse(state.entries().iterator().hasNext());

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
