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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;

/**
 * This class wraps map state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <UK> Type of the user entry key of state
 * @param <UV> Type of the user entry value of state
 */
class LatencyTrackingMapState<K, N, UK, UV>
        extends AbstractLatencyTrackState<
                K,
                N,
                Map<UK, UV>,
                InternalMapState<K, N, UK, UV>,
                LatencyTrackingMapState.MapStateLatencyMetrics>
        implements InternalMapState<K, N, UK, UV> {
    LatencyTrackingMapState(
            String stateName,
            InternalMapState<K, N, UK, UV> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new MapStateLatencyMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getHistorySize(),
                        latencyTrackingStateConfig.isStateNameAsVariable()));
    }

    @Override
    public UV get(UK key) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnGet()) {
            return trackLatencyWithException(
                    () -> original.get(key), MapStateLatencyMetrics.MAP_STATE_GET_LATENCY);
        } else {
            return original.get(key);
        }
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnPut()) {
            trackLatencyWithException(
                    () -> original.put(key, value), MapStateLatencyMetrics.MAP_STATE_PUT_LATENCY);
        } else {
            original.put(key, value);
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnPutAll()) {
            trackLatencyWithException(
                    () -> original.putAll(map), MapStateLatencyMetrics.MAP_STATE_PUT_ALL_LATENCY);
        } else {
            original.putAll(map);
        }
    }

    @Override
    public void remove(UK key) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnRemove()) {
            trackLatencyWithException(
                    () -> original.remove(key), MapStateLatencyMetrics.MAP_STATE_REMOVE_LATENCY);
        } else {
            original.remove(key);
        }
    }

    @Override
    public boolean contains(UK key) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnContains()) {
            return trackLatencyWithException(
                    () -> original.contains(key),
                    MapStateLatencyMetrics.MAP_STATE_CONTAINS_LATENCY);
        } else {
            return original.contains(key);
        }
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnEntriesInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.entries()),
                    MapStateLatencyMetrics.MAP_STATE_ENTRIES_INIT_LATENCY);
        } else {
            return new IterableWrapper<>(original.entries());
        }
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnKeysInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.keys()),
                    MapStateLatencyMetrics.MAP_STATE_KEYS_INIT_LATENCY);
        } else {
            return new IterableWrapper<>(original.keys());
        }
    }

    @Override
    public Iterable<UV> values() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnValuesInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.values()),
                    MapStateLatencyMetrics.MAP_STATE_VALUES_INIT_LATENCY);
        } else {
            return new IterableWrapper<>(original.values());
        }
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnIteratorInit()) {
            return trackLatencyWithException(
                    () -> new IteratorWrapper<>(original.iterator()),
                    MapStateLatencyMetrics.MAP_STATE_ITERATOR_INIT_LATENCY);
        } else {
            return new IteratorWrapper<>(original.iterator());
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnIsEmpty()) {
            return trackLatencyWithException(
                    () -> original.isEmpty(), MapStateLatencyMetrics.MAP_STATE_IS_EMPTY_LATENCY);
        } else {
            return original.isEmpty();
        }
    }

    private class IterableWrapper<E> implements Iterable<E> {
        private final Iterable<E> iterable;

        IterableWrapper(Iterable<E> iterable) {
            this.iterable = iterable;
        }

        @Override
        public Iterator<E> iterator() {
            return new IteratorWrapper<>(iterable.iterator());
        }
    }

    private class IteratorWrapper<E> implements Iterator<E> {
        private final Iterator<E> iterator;

        IteratorWrapper(Iterator<E> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            if (latencyTrackingStateMetric.trackLatencyOnIteratorHasNext()) {
                return trackLatency(
                        iterator::hasNext,
                        MapStateLatencyMetrics.MAP_STATE_ITERATOR_HAS_NEXT_LATENCY);
            } else {
                return iterator.hasNext();
            }
        }

        @Override
        public E next() {
            if (latencyTrackingStateMetric.trackLatencyOnIteratorNext()) {
                return trackLatency(
                        iterator::next, MapStateLatencyMetrics.MAP_STATE_ITERATOR_NEXT_LATENCY);
            } else {
                return iterator.next();
            }
        }

        @Override
        public void remove() {
            if (latencyTrackingStateMetric.trackLatencyOnIteratorRemove()) {
                trackLatency(
                        iterator::remove, MapStateLatencyMetrics.MAP_STATE_ITERATOR_REMOVE_LATENCY);
            } else {
                iterator.remove();
            }
        }
    }

    static class MapStateLatencyMetrics extends StateLatencyMetricBase {
        private static final String MAP_STATE_GET_LATENCY = "mapStateGetLatency";
        private static final String MAP_STATE_PUT_LATENCY = "mapStatePutLatency";
        private static final String MAP_STATE_PUT_ALL_LATENCY = "mapStatePutAllLatency";
        private static final String MAP_STATE_REMOVE_LATENCY = "mapStateRemoveLatency";
        private static final String MAP_STATE_CONTAINS_LATENCY = "mapStateContainsLatency";
        private static final String MAP_STATE_ENTRIES_INIT_LATENCY = "mapStateEntriesInitLatency";
        private static final String MAP_STATE_KEYS_INIT_LATENCY = "mapStateKeysInitLatency";
        private static final String MAP_STATE_VALUES_INIT_LATENCY = "mapStateValuesInitLatency";
        private static final String MAP_STATE_ITERATOR_INIT_LATENCY = "mapStateIteratorInitLatency";
        private static final String MAP_STATE_IS_EMPTY_LATENCY = "mapStateIsEmptyLatency";
        private static final String MAP_STATE_ITERATOR_HAS_NEXT_LATENCY =
                "mapStateIteratorHasNextLatency";
        private static final String MAP_STATE_ITERATOR_NEXT_LATENCY = "mapStateIteratorNextLatency";
        private static final String MAP_STATE_ITERATOR_REMOVE_LATENCY =
                "mapStateIteratorRemoveLatency";

        private int getCount = 0;
        private int iteratorRemoveCount = 0;
        private int putCount = 0;
        private int putAllCount = 0;
        private int removeCount = 0;
        private int containsCount = 0;
        private int entriesInitCount = 0;
        private int keysInitCount = 0;
        private int valuesInitCount = 0;
        private int isEmptyCount = 0;
        private int iteratorInitCount = 0;
        private int iteratorHasNextCount = 0;
        private int iteratorNextCount = 0;

        private MapStateLatencyMetrics(
                String stateName,
                MetricGroup metricGroup,
                int sampleInterval,
                int historySize,
                boolean stateNameAsVariable) {
            super(stateName, metricGroup, sampleInterval, historySize, stateNameAsVariable);
        }

        int getGetCount() {
            return getCount;
        }

        int getIteratorRemoveCount() {
            return iteratorRemoveCount;
        }

        int getPutCount() {
            return putCount;
        }

        int getPutAllCount() {
            return putAllCount;
        }

        int getRemoveCount() {
            return removeCount;
        }

        int getContainsCount() {
            return containsCount;
        }

        int getEntriesInitCount() {
            return entriesInitCount;
        }

        int getKeysInitCount() {
            return keysInitCount;
        }

        int getValuesInitCount() {
            return valuesInitCount;
        }

        int getIsEmptyCount() {
            return isEmptyCount;
        }

        int getIteratorInitCount() {
            return iteratorInitCount;
        }

        int getIteratorHasNextCount() {
            return iteratorHasNextCount;
        }

        @VisibleForTesting
        void resetIteratorHasNextCount() {
            iteratorHasNextCount = 0;
        }

        int getIteratorNextCount() {
            return iteratorNextCount;
        }

        private boolean trackLatencyOnGet() {
            getCount = loopUpdateCounter(getCount);
            return getCount == 1;
        }

        private boolean trackLatencyOnPut() {
            putCount = loopUpdateCounter(putCount);
            return putCount == 1;
        }

        private boolean trackLatencyOnPutAll() {
            putAllCount = loopUpdateCounter(putAllCount);
            return putAllCount == 1;
        }

        private boolean trackLatencyOnRemove() {
            removeCount = loopUpdateCounter(removeCount);
            return removeCount == 1;
        }

        private boolean trackLatencyOnContains() {
            containsCount = loopUpdateCounter(containsCount);
            return containsCount == 1;
        }

        private boolean trackLatencyOnEntriesInit() {
            entriesInitCount = loopUpdateCounter(entriesInitCount);
            return entriesInitCount == 1;
        }

        private boolean trackLatencyOnKeysInit() {
            keysInitCount = loopUpdateCounter(keysInitCount);
            return keysInitCount == 1;
        }

        private boolean trackLatencyOnValuesInit() {
            valuesInitCount = loopUpdateCounter(valuesInitCount);
            return valuesInitCount == 1;
        }

        private boolean trackLatencyOnIteratorInit() {
            iteratorInitCount = loopUpdateCounter(iteratorInitCount);
            return iteratorInitCount == 1;
        }

        private boolean trackLatencyOnIsEmpty() {
            isEmptyCount = loopUpdateCounter(isEmptyCount);
            return isEmptyCount == 1;
        }

        private boolean trackLatencyOnIteratorHasNext() {
            iteratorHasNextCount = loopUpdateCounter(iteratorHasNextCount);
            return iteratorHasNextCount == 1;
        }

        private boolean trackLatencyOnIteratorNext() {
            iteratorNextCount = loopUpdateCounter(iteratorNextCount);
            return iteratorNextCount == 1;
        }

        private boolean trackLatencyOnIteratorRemove() {
            iteratorRemoveCount = loopUpdateCounter(iteratorRemoveCount);
            return iteratorRemoveCount == 1;
        }
    }
}
