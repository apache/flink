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

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

/** Tests for {@link LatencyTrackingMapState}. */
public class LatencyTrackingMapStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    MapStateDescriptor<Integer, Double> getStateDescriptor() {
        return new MapStateDescriptor<>("map", Integer.class, Double.class);
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
    public void testLatencyTrackingMapState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState =
                    (LatencyTrackingMapState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            LatencyTrackingMapState.MapStateLatencyMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertEquals(0, latencyTrackingStateMetric.getContainsCount());
            assertEquals(0, latencyTrackingStateMetric.getEntriesInitCount());
            assertEquals(0, latencyTrackingStateMetric.getGetCount());
            assertEquals(0, latencyTrackingStateMetric.getIsEmptyCount());
            assertEquals(0, latencyTrackingStateMetric.getIteratorInitCount());
            assertEquals(0, latencyTrackingStateMetric.getIteratorHasNextCount());
            assertEquals(0, latencyTrackingStateMetric.getIteratorNextCount());
            assertEquals(0, latencyTrackingStateMetric.getKeysInitCount());
            assertEquals(0, latencyTrackingStateMetric.getValuesInitCount());
            assertEquals(0, latencyTrackingStateMetric.getIteratorRemoveCount());
            assertEquals(0, latencyTrackingStateMetric.getPutAllCount());
            assertEquals(0, latencyTrackingStateMetric.getPutCount());
            assertEquals(0, latencyTrackingStateMetric.getRemoveCount());

            setCurrentKey(keyedBackend);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.put(random.nextLong(), random.nextDouble());
                assertEquals(expectedResult, latencyTrackingStateMetric.getPutCount());

                latencyTrackingState.putAll(
                        Collections.singletonMap(random.nextLong(), random.nextDouble()));
                assertEquals(expectedResult, latencyTrackingStateMetric.getPutAllCount());

                latencyTrackingState.get(random.nextLong());
                assertEquals(expectedResult, latencyTrackingStateMetric.getGetCount());

                latencyTrackingState.remove(random.nextLong());
                assertEquals(expectedResult, latencyTrackingStateMetric.getRemoveCount());

                latencyTrackingState.contains(random.nextLong());
                assertEquals(expectedResult, latencyTrackingStateMetric.getContainsCount());

                latencyTrackingState.isEmpty();
                assertEquals(expectedResult, latencyTrackingStateMetric.getIsEmptyCount());

                latencyTrackingState.entries();
                assertEquals(expectedResult, latencyTrackingStateMetric.getEntriesInitCount());

                latencyTrackingState.keys();
                assertEquals(expectedResult, latencyTrackingStateMetric.getKeysInitCount());

                latencyTrackingState.values();
                assertEquals(expectedResult, latencyTrackingStateMetric.getValuesInitCount());

                latencyTrackingState.iterator();
                assertEquals(expectedResult, latencyTrackingStateMetric.getIteratorInitCount());
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLatencyTrackingMapStateIterator() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState =
                    (LatencyTrackingMapState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            LatencyTrackingMapState.MapStateLatencyMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            setCurrentKey(keyedBackend);

            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.iterator(),
                    true);
            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.entries().iterator(),
                    true);
            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.keys().iterator(),
                    false);
            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.values().iterator(),
                    false);
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }

    private <E> void verifyIterator(
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState,
            LatencyTrackingMapState.MapStateLatencyMetrics latencyTrackingStateMetric,
            Iterator<E> iterator,
            boolean removeIterator)
            throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
            latencyTrackingState.put((long) index, random.nextDouble());
        }
        int count = 1;
        while (iterator.hasNext()) {
            int expectedResult = count == SAMPLE_INTERVAL ? 0 : count;
            assertEquals(expectedResult, latencyTrackingStateMetric.getIteratorHasNextCount());

            iterator.next();
            assertEquals(expectedResult, latencyTrackingStateMetric.getIteratorNextCount());

            if (removeIterator) {
                iterator.remove();
                assertEquals(expectedResult, latencyTrackingStateMetric.getIteratorRemoveCount());
            }
            count += 1;
        }
        // as we call #hasNext on more time than #next, to avoid complex check, just reset hasNext
        // counter in the end.
        latencyTrackingStateMetric.resetIteratorHasNextCount();
        latencyTrackingState.clear();
    }
}
