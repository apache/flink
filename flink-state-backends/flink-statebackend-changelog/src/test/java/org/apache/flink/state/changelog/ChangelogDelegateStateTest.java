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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.util.IOUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.DummyCheckpointingStorageAccess;
import static org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.createKeyedBackend;
import static org.junit.Assert.assertSame;

/** Tests for {@link ChangelogStateBackend} delegating state accesses. */
@RunWith(Parameterized.class)
public class ChangelogDelegateStateTest {
    private MockEnvironment env;

    @Parameterized.Parameters
    public static List<Supplier<AbstractStateBackend>> delegatedStateBackend() {
        return Arrays.asList(HashMapStateBackend::new, EmbeddedRocksDBStateBackend::new);
    }

    @Parameterized.Parameter public Supplier<AbstractStateBackend> backend;

    @Before
    public void before() {
        env = MockEnvironment.builder().build();
        env.setCheckpointStorageAccess(new DummyCheckpointingStorageAccess());
    }

    @After
    public void after() {
        IOUtils.closeQuietly(env);
    }

    @Test
    public void testDelegatingValueState() throws Exception {
        testDelegatingState(
                new ValueStateDescriptor<>("id", String.class), ChangelogValueState.class);
    }

    @Test
    public void testDelegatingListState() throws Exception {
        testDelegatingState(
                new ListStateDescriptor<>("id", String.class), ChangelogListState.class);
    }

    @Test
    public void testDelegatingMapState() throws Exception {
        testDelegatingState(
                new MapStateDescriptor<>("id", Integer.class, String.class),
                ChangelogMapState.class);
    }

    @Test
    public void testDelegatingReducingState() throws Exception {
        testDelegatingState(
                new ReducingStateDescriptor<>(
                        "id", (value1, value2) -> value1 + "," + value2, String.class),
                ChangelogReducingState.class);
    }

    @Test
    public void testDelegatingAggregatingState() throws Exception {
        testDelegatingState(
                new AggregatingStateDescriptor<>(
                        "my-state",
                        new StateBackendTestBase.MutableAggregatingAddingFunction(),
                        StateBackendTestBase.MutableLong.class),
                ChangelogAggregatingState.class);
    }

    private void testDelegatingState(StateDescriptor descriptor, Class<?> stateClass)
            throws Exception {
        KeyedStateBackend<Integer> delegatedBackend = null;
        KeyedStateBackend<Integer> changelogBackend = null;

        try {
            delegatedBackend = createKeyedBackend(backend.get(), env);
            changelogBackend = createKeyedBackend(new ChangelogStateBackend(backend.get()), env);

            State state =
                    changelogBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

            assertSame(state.getClass(), stateClass);
            assertSame(
                    ((AbstractChangelogState<?, ?, ?, ?>) state).getDelegatedState().getClass(),
                    delegatedBackend
                            .getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    descriptor)
                            .getClass());
        } finally {
            if (delegatedBackend != null) {
                delegatedBackend.dispose();
            }

            if (changelogBackend != null) {
                changelogBackend.dispose();
            }
        }
    }
}
