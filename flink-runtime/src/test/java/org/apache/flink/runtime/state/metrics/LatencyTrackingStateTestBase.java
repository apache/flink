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

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for latency tracking state. */
abstract class LatencyTrackingStateTestBase<K> {
    protected static final int SAMPLE_INTERVAL = 10;

    protected AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer)
            throws Exception {

        Environment env = new DummyEnvironment();
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 127);
        int numberOfKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        Configuration configuration = new Configuration();
        configuration.setBoolean(StateBackendOptions.LATENCY_TRACK_ENABLED, true);
        configuration.setInteger(
                StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL, SAMPLE_INTERVAL);
        // use a very large value to not let metrics data overridden.
        int historySize = 1000_000;
        configuration.setInteger(StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE, historySize);
        HashMapStateBackend stateBackend =
                new HashMapStateBackend()
                        .configure(configuration, Thread.currentThread().getContextClassLoader());
        return stateBackend.createKeyedStateBackend(
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
    }

    @SuppressWarnings("unchecked")
    protected <
                    N,
                    V,
                    S extends InternalKvState<K, N, V>,
                    S2 extends State,
                    LSM extends StateLatencyMetricBase>
            AbstractLatencyTrackState<K, N, V, S, LSM> createLatencyTrackingState(
                    AbstractKeyedStateBackend<K> keyedBackend,
                    StateDescriptor<S2, V> stateDescriptor)
                    throws Exception {

        S2 keyedState =
                keyedBackend.getOrCreateKeyedState(
                        VoidNamespaceSerializer.INSTANCE, stateDescriptor);
        Preconditions.checkState(keyedState instanceof AbstractLatencyTrackState);
        return (AbstractLatencyTrackState<K, N, V, S, LSM>) keyedState;
    }

    abstract <V, S extends State> StateDescriptor<S, V> getStateDescriptor();

    abstract TypeSerializer<K> getKeySerializer();

    abstract void setCurrentKey(AbstractKeyedStateBackend<K> keyedBackend);

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testLatencyTrackingStateClear() throws Exception {
        AbstractKeyedStateBackend<K> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            AbstractLatencyTrackState latencyTrackingState =
                    createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            StateLatencyMetricBase latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertThat(latencyTrackingStateMetric.getClearCount()).isZero();

            setCurrentKey(keyedBackend);
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.clear();
                assertThat(latencyTrackingStateMetric.getClearCount()).isEqualTo(expectedResult);
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
