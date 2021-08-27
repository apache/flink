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

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

/** Tests for {@link LatencyTrackingListState}. */
public class LatencyTrackingListStateTest extends LatencyTrackingStateTestBase<Integer> {
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
    public void testLatencyTrackingListState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingListState<Integer, VoidNamespace, Long> latencyTrackingState =
                    (LatencyTrackingListState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            LatencyTrackingListState.ListStateLatencyMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertEquals(0, latencyTrackingStateMetric.getAddCount());
            assertEquals(0, latencyTrackingStateMetric.getAddAllCount());
            assertEquals(0, latencyTrackingStateMetric.getGetCount());
            assertEquals(0, latencyTrackingStateMetric.getUpdateCount());
            assertEquals(0, latencyTrackingStateMetric.getMergeNamespaceCount());

            setCurrentKey(keyedBackend);
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.add(ThreadLocalRandom.current().nextLong());
                assertEquals(expectedResult, latencyTrackingStateMetric.getAddCount());

                latencyTrackingState.addAll(
                        Collections.singletonList(ThreadLocalRandom.current().nextLong()));
                assertEquals(expectedResult, latencyTrackingStateMetric.getAddAllCount());

                latencyTrackingState.update(
                        Collections.singletonList(ThreadLocalRandom.current().nextLong()));
                assertEquals(expectedResult, latencyTrackingStateMetric.getUpdateCount());

                latencyTrackingState.get();
                assertEquals(expectedResult, latencyTrackingStateMetric.getGetCount());

                latencyTrackingState.mergeNamespaces(
                        VoidNamespace.INSTANCE, Collections.emptyList());
                assertEquals(expectedResult, latencyTrackingStateMetric.getMergeNamespaceCount());
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
