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

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LatencyTrackingListState}. */
class LatencyTrackingListStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    ListStateDescriptor<Long> getStateDescriptor() {
        return new ListStateDescriptor<>("list", Long.class);
    }

    @Override
    TypeSerializer<Integer> getKeySerializer() {
        return IntSerializer.INSTANCE;
    }

    @Override
    void setCurrentKey(AbstractKeyedStateBackend<Integer> keyedBackend) {
        keyedBackend.setCurrentKey(1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testLatencyTrackingListState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingListState<Integer, VoidNamespace, Long> latencyTrackingState =
                    (LatencyTrackingListState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            LatencyTrackingListState.ListStateLatencyMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertThat(latencyTrackingStateMetric.getAddCount()).isZero();
            assertThat(latencyTrackingStateMetric.getAddAllCount()).isZero();
            assertThat(latencyTrackingStateMetric.getGetCount()).isZero();
            assertThat(latencyTrackingStateMetric.getUpdateCount()).isZero();
            assertThat(latencyTrackingStateMetric.getMergeNamespaceCount()).isZero();

            setCurrentKey(keyedBackend);
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.add(ThreadLocalRandom.current().nextLong());
                assertThat(latencyTrackingStateMetric.getAddCount()).isEqualTo(expectedResult);

                latencyTrackingState.addAll(
                        Collections.singletonList(ThreadLocalRandom.current().nextLong()));
                assertThat(latencyTrackingStateMetric.getAddAllCount()).isEqualTo(expectedResult);

                latencyTrackingState.update(
                        Collections.singletonList(ThreadLocalRandom.current().nextLong()));
                assertThat(latencyTrackingStateMetric.getUpdateCount()).isEqualTo(expectedResult);

                latencyTrackingState.get();
                assertThat(latencyTrackingStateMetric.getGetCount()).isEqualTo(expectedResult);

                latencyTrackingState.mergeNamespaces(
                        VoidNamespace.INSTANCE, Collections.emptyList());
                assertThat(latencyTrackingStateMetric.getMergeNamespaceCount())
                        .isEqualTo(expectedResult);
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
