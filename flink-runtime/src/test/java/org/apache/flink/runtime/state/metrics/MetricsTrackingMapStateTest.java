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
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MetricsTrackingMapState}. */
class MetricsTrackingMapStateTest extends MetricsTrackingStateTestBase<Integer> {
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
    void testLatencyTrackingMapState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            MetricsTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState =
                    (MetricsTrackingMapState)
                            createMetricsTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            MetricsTrackingMapState.MapStateMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertThat(latencyTrackingStateMetric.getContainsCount()).isZero();
            assertThat(latencyTrackingStateMetric.getEntriesInitCount()).isZero();
            assertThat(latencyTrackingStateMetric.getGetCount()).isZero();
            assertThat(latencyTrackingStateMetric.getIsEmptyCount()).isZero();
            assertThat(latencyTrackingStateMetric.getIteratorInitCount()).isZero();
            assertThat(latencyTrackingStateMetric.getIteratorHasNextCount()).isZero();
            assertThat(latencyTrackingStateMetric.getIteratorNextCount()).isZero();
            assertThat(latencyTrackingStateMetric.getKeysInitCount()).isZero();
            assertThat(latencyTrackingStateMetric.getValuesInitCount()).isZero();
            assertThat(latencyTrackingStateMetric.getIteratorRemoveCount()).isZero();
            assertThat(latencyTrackingStateMetric.getPutAllCount()).isZero();
            assertThat(latencyTrackingStateMetric.getPutCount()).isZero();
            assertThat(latencyTrackingStateMetric.getRemoveCount()).isZero();

            setCurrentKey(keyedBackend);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.put(random.nextLong(), random.nextDouble());
                assertThat(latencyTrackingStateMetric.getPutCount()).isEqualTo(expectedResult);

                latencyTrackingState.putAll(
                        Collections.singletonMap(random.nextLong(), random.nextDouble()));
                assertThat(latencyTrackingStateMetric.getPutAllCount()).isEqualTo(expectedResult);

                latencyTrackingState.get(random.nextLong());
                assertThat(latencyTrackingStateMetric.getGetCount()).isEqualTo(expectedResult);

                latencyTrackingState.remove(random.nextLong());
                assertThat(latencyTrackingStateMetric.getRemoveCount()).isEqualTo(expectedResult);

                latencyTrackingState.contains(random.nextLong());
                assertThat(latencyTrackingStateMetric.getContainsCount()).isEqualTo(expectedResult);

                latencyTrackingState.isEmpty();
                assertThat(latencyTrackingStateMetric.getIsEmptyCount()).isEqualTo(expectedResult);

                latencyTrackingState.entries();
                assertThat(latencyTrackingStateMetric.getEntriesInitCount())
                        .isEqualTo(expectedResult);

                latencyTrackingState.keys();
                assertThat(latencyTrackingStateMetric.getKeysInitCount()).isEqualTo(expectedResult);

                latencyTrackingState.values();
                assertThat(latencyTrackingStateMetric.getValuesInitCount())
                        .isEqualTo(expectedResult);

                latencyTrackingState.iterator();
                assertThat(latencyTrackingStateMetric.getIteratorInitCount())
                        .isEqualTo(expectedResult);
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
    void testSizeTrackingMapState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            MetricsTrackingMapState<Integer, VoidNamespace, Long, Double> sizeTrackingState =
                    (MetricsTrackingMapState)
                            createMetricsTrackingState(keyedBackend, getStateDescriptor());
            sizeTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            MetricsTrackingMapState.MapStateMetrics sizeTrackingStateMetric =
                    sizeTrackingState.getSizeTrackingStateMetric();

            assertThat(sizeTrackingStateMetric.getContainsCount()).isZero();
            assertThat(sizeTrackingStateMetric.getEntriesInitCount()).isZero();
            assertThat(sizeTrackingStateMetric.getGetCount()).isZero();
            assertThat(sizeTrackingStateMetric.getIsEmptyCount()).isZero();
            assertThat(sizeTrackingStateMetric.getIteratorInitCount()).isZero();
            assertThat(sizeTrackingStateMetric.getIteratorHasNextCount()).isZero();
            assertThat(sizeTrackingStateMetric.getIteratorNextCount()).isZero();
            assertThat(sizeTrackingStateMetric.getKeysInitCount()).isZero();
            assertThat(sizeTrackingStateMetric.getValuesInitCount()).isZero();
            assertThat(sizeTrackingStateMetric.getIteratorRemoveCount()).isZero();
            assertThat(sizeTrackingStateMetric.getPutAllCount()).isZero();
            assertThat(sizeTrackingStateMetric.getPutCount()).isZero();
            assertThat(sizeTrackingStateMetric.getRemoveCount()).isZero();

            setCurrentKey(keyedBackend);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                sizeTrackingState.put(random.nextLong(), random.nextDouble());
                assertThat(sizeTrackingStateMetric.getPutCount()).isEqualTo(expectedResult);

                sizeTrackingState.putAll(
                        Collections.singletonMap(random.nextLong(), random.nextDouble()));
                assertThat(sizeTrackingStateMetric.getPutAllCount()).isEqualTo(expectedResult);

                sizeTrackingState.get(random.nextLong());
                assertThat(sizeTrackingStateMetric.getGetCount()).isEqualTo(expectedResult);

                sizeTrackingState.contains(random.nextLong());
                assertThat(sizeTrackingStateMetric.getContainsCount()).isEqualTo(expectedResult);

                sizeTrackingState.remove(random.nextLong());
                assertThat(sizeTrackingStateMetric.getRemoveCount()).isEqualTo(expectedResult);

                sizeTrackingState.isEmpty();
                assertThat(sizeTrackingStateMetric.getIsEmptyCount()).isEqualTo(expectedResult);
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
    void testLatencyTrackingMapStateIterator() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            MetricsTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState =
                    (MetricsTrackingMapState)
                            createMetricsTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            MetricsTrackingMapState.MapStateMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            setCurrentKey(keyedBackend);

            verifyLatencyTrackingIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState::iterator,
                    true);
            verifyLatencyTrackingIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    () -> latencyTrackingState.entries().iterator(),
                    true);
            verifyLatencyTrackingIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    () -> latencyTrackingState.keys().iterator(),
                    false);
            verifyLatencyTrackingIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    () -> latencyTrackingState.values().iterator(),
                    false);
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSizeTrackingMapStateIterator() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            MetricsTrackingMapState<Integer, VoidNamespace, Long, Double> sizeTrackingState =
                    (MetricsTrackingMapState)
                            createMetricsTrackingState(keyedBackend, getStateDescriptor());
            sizeTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            MetricsTrackingMapState.MapStateMetrics sizeTrackingStateMetric =
                    sizeTrackingState.getSizeTrackingStateMetric();

            setCurrentKey(keyedBackend);

            verifySizeTrackingIterator(
                    sizeTrackingState, sizeTrackingStateMetric, sizeTrackingState::iterator, true);
            verifySizeTrackingIterator(
                    sizeTrackingState,
                    sizeTrackingStateMetric,
                    () -> sizeTrackingState.entries().iterator(),
                    true);
            verifySizeTrackingIterator(
                    sizeTrackingState,
                    sizeTrackingStateMetric,
                    () -> sizeTrackingState.keys().iterator(),
                    false);
            verifySizeTrackingIterator(
                    sizeTrackingState,
                    sizeTrackingStateMetric,
                    () -> sizeTrackingState.values().iterator(),
                    false);
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }

    private <E> void verifyLatencyTrackingIterator(
            MetricsTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState,
            MetricsTrackingMapState.MapStateMetrics latencyTrackingStateMetric,
            SupplierWithException<Iterator<E>, Exception> iteratorSupplier,
            boolean removeIterator)
            throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
            latencyTrackingState.put((long) index, random.nextDouble());
        }
        int count = 1;
        Iterator<E> iterator = iteratorSupplier.get();
        while (iterator.hasNext()) {
            int expectedResult = count == SAMPLE_INTERVAL ? 0 : count;
            assertThat(latencyTrackingStateMetric.getIteratorHasNextCount())
                    .isEqualTo(expectedResult);

            iterator.next();
            assertThat(latencyTrackingStateMetric.getIteratorNextCount()).isEqualTo(expectedResult);

            if (removeIterator) {
                iterator.remove();
                assertThat(latencyTrackingStateMetric.getIteratorRemoveCount())
                        .isEqualTo(expectedResult);
            }
            count += 1;
        }
        // as we call #hasNext on more time than #next, to avoid complex check, just reset hasNext
        // counter in the end.
        latencyTrackingStateMetric.resetIteratorHasNextCount();
        latencyTrackingState.clear();
    }

    private <E> void verifySizeTrackingIterator(
            MetricsTrackingMapState<Integer, VoidNamespace, Long, Double> sizeTrackingState,
            MetricsTrackingMapState.MapStateMetrics sizeTrackingStateMetric,
            SupplierWithException<Iterator<E>, Exception> iteratorSupplier,
            boolean removeIterator)
            throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
            sizeTrackingState.put((long) index, random.nextDouble());
        }
        int count = 1;
        Iterator<E> iterator = iteratorSupplier.get();
        while (iterator.hasNext()) {
            int expectedResult = count == SAMPLE_INTERVAL ? 0 : count;

            iterator.next();
            assertThat(sizeTrackingStateMetric.getIteratorNextCount()).isEqualTo(expectedResult);

            if (removeIterator) {
                iterator.remove();
            }
            count += 1;
        }
        // as we call #hasNext on more time than #next, to avoid complex check, just reset hasNext
        // counter in the end.
        sizeTrackingStateMetric.resetIteratorHasNextCount();
        sizeTrackingState.clear();
    }
}
