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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
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
class MetricsTrackingMapState<K, N, UK, UV>
        extends AbstractMetricsTrackState<
                K,
                N,
                Map<UK, UV>,
                InternalMapState<K, N, UK, UV>,
                MetricsTrackingMapState.MapStateMetrics>
        implements InternalMapState<K, N, UK, UV> {

    private TypeSerializer<UK> userKeySerializer;
    private TypeSerializer<UV> userValueSerializer;
    private ThrowingConsumer<UK, IOException> trackIteratorKeySizeConsumer;
    private ThrowingConsumer<UV, IOException> trackIteratorValueSizeConsumer;
    private ThrowingConsumer<Map.Entry<UK, UV>, IOException> trackIteratorKeyAndValueSizeConsumer;

    MetricsTrackingMapState(
            String stateName,
            InternalMapState<K, N, UK, UV> original,
            KeyedStateBackend<K> keyedStateBackend,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            SizeTrackingStateConfig sizeTrackingStateConfig) {
        super(
                original,
                keyedStateBackend,
                latencyTrackingStateConfig.isEnabled()
                        ? new MapStateMetrics(
                                stateName,
                                latencyTrackingStateConfig.getMetricGroup(),
                                latencyTrackingStateConfig.getSampleInterval(),
                                latencyTrackingStateConfig.getHistorySize(),
                                latencyTrackingStateConfig.isStateNameAsVariable())
                        : null,
                sizeTrackingStateConfig.isEnabled()
                        ? new MapStateMetrics(
                                stateName,
                                sizeTrackingStateConfig.getMetricGroup(),
                                sizeTrackingStateConfig.getSampleInterval(),
                                sizeTrackingStateConfig.getHistorySize(),
                                sizeTrackingStateConfig.isStateNameAsVariable())
                        : null);
        if (valueSerializer != null) {
            MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
            userKeySerializer = castedMapSerializer.getKeySerializer();
            userValueSerializer = castedMapSerializer.getValueSerializer();
        }
        if (sizeTrackingStateConfig.isEnabled()) {
            trackIteratorKeySizeConsumer =
                    uk -> {
                        sizeTrackingStateMetric.updateMetrics(
                                MapStateMetrics.MAP_STATE_ITERATOR_KEY_SIZE,
                                sizeOfKeyAndUserKey(uk));
                    };
            trackIteratorValueSizeConsumer =
                    uv -> {
                        sizeTrackingStateMetric.updateMetrics(
                                MapStateMetrics.MAP_STATE_ITERATOR_VALUE_SIZE, sizeOfUserValue(uv));
                    };
            trackIteratorKeyAndValueSizeConsumer =
                    entry -> {
                        sizeTrackingStateMetric.updateMetrics(
                                MapStateMetrics.MAP_STATE_ITERATOR_KEY_SIZE,
                                sizeOfKeyAndUserKey(entry.getKey()));
                        sizeTrackingStateMetric.updateMetrics(
                                MapStateMetrics.MAP_STATE_ITERATOR_VALUE_SIZE,
                                sizeOfUserValue(entry.getValue()));
                    };
        }
    }

    @Override
    public UV get(UK key) throws Exception {
        UV result;
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnGet()) {
            result =
                    trackLatencyWithException(
                            () -> original.get(key), MapStateMetrics.MAP_STATE_GET_LATENCY);
        } else {
            result = original.get(key);
        }
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnGet()) {
            sizeTrackingStateMetric.updateMetrics(
                    MapStateMetrics.MAP_STATE_GET_KEY_SIZE, sizeOfKeyAndUserKey(key));
            sizeTrackingStateMetric.updateMetrics(
                    MapStateMetrics.MAP_STATE_GET_VALUE_SIZE, sizeOfUserValue(result));
        }
        return result;
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnPut()) {
            sizeTrackingStateMetric.updateMetrics(
                    MapStateMetrics.MAP_STATE_PUT_KEY_SIZE, sizeOfKeyAndUserKey(key));
            sizeTrackingStateMetric.updateMetrics(
                    MapStateMetrics.MAP_STATE_PUT_VALUE_SIZE, sizeOfUserValue(value));
        }
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnPut()) {
            trackLatencyWithException(
                    () -> original.put(key, value), MapStateMetrics.MAP_STATE_PUT_LATENCY);
        } else {
            original.put(key, value);
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnPutAll()) {
            for (Map.Entry<UK, UV> entry : map.entrySet()) {
                sizeTrackingStateMetric.updateMetrics(
                        MapStateMetrics.MAP_STATE_PUT_KEY_SIZE,
                        sizeOfKeyAndUserKey(entry.getKey()));
                sizeTrackingStateMetric.updateMetrics(
                        MapStateMetrics.MAP_STATE_PUT_VALUE_SIZE,
                        sizeOfUserValue(entry.getValue()));
            }
        }

        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnPutAll()) {
            trackLatencyWithException(
                    () -> original.putAll(map), MapStateMetrics.MAP_STATE_PUT_ALL_LATENCY);
        } else {
            original.putAll(map);
        }
    }

    @Override
    public void remove(UK key) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnRemove()) {
            sizeTrackingStateMetric.updateMetrics(
                    MapStateMetrics.MAP_STATE_REMOVE_KEY_SIZE, sizeOfKeyAndUserKey(key));
        }
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnRemove()) {
            trackLatencyWithException(
                    () -> original.remove(key), MapStateMetrics.MAP_STATE_REMOVE_LATENCY);
        } else {
            original.remove(key);
        }
    }

    @Override
    public boolean contains(UK key) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnContains()) {
            sizeTrackingStateMetric.updateMetrics(
                    MapStateMetrics.MAP_STATE_CONTAINS_KEY_SIZE, sizeOfKeyAndUserKey(key));
        }
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnContains()) {
            return trackLatencyWithException(
                    () -> original.contains(key), MapStateMetrics.MAP_STATE_CONTAINS_LATENCY);
        } else {
            return original.contains(key);
        }
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnEntriesInit()) {
            return trackLatencyWithException(
                    () ->
                            new IterableWrapper<>(
                                    original.entries(), trackIteratorKeyAndValueSizeConsumer),
                    MapStateMetrics.MAP_STATE_ENTRIES_INIT_LATENCY);
        } else {
            return new IterableWrapper<>(original.entries(), trackIteratorKeyAndValueSizeConsumer);
        }
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnKeysInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.keys(), trackIteratorKeySizeConsumer),
                    MapStateMetrics.MAP_STATE_KEYS_INIT_LATENCY);
        } else {
            return new IterableWrapper<>(original.keys(), trackIteratorKeySizeConsumer);
        }
    }

    @Override
    public Iterable<UV> values() throws Exception {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnValuesInit()) {
            return trackLatencyWithException(
                    () -> new IterableWrapper<>(original.values(), trackIteratorValueSizeConsumer),
                    MapStateMetrics.MAP_STATE_VALUES_INIT_LATENCY);
        } else {
            return new IterableWrapper<>(original.values(), trackIteratorValueSizeConsumer);
        }
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnIteratorInit()) {
            return trackLatencyWithException(
                    () ->
                            new IteratorWrapper<>(
                                    original.iterator(), trackIteratorKeyAndValueSizeConsumer),
                    MapStateMetrics.MAP_STATE_ITERATOR_INIT_LATENCY);
        } else {
            return new IteratorWrapper<>(original.iterator(), trackIteratorKeyAndValueSizeConsumer);
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnIsEmpty()) {
            sizeTrackingStateMetric.updateMetrics(
                    MapStateMetrics.MAP_STATE_IS_EMPTY_KEY_SIZE, super.sizeOfKey());
        }
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnIsEmpty()) {
            return trackLatencyWithException(
                    () -> original.isEmpty(), MapStateMetrics.MAP_STATE_IS_EMPTY_LATENCY);
        } else {
            return original.isEmpty();
        }
    }

    protected long sizeOfKeyAndUserKey(UK userKey) throws IOException {

        if (userKeySerializer == null || userKey == null) {
            return super.sizeOfKey();
        }
        long userKeySize;
        if (userKeySerializer.getLength() == -1) {
            try {
                userKeySerializer.serialize(userKey, outputSerializer);
                userKeySize = outputSerializer.length();
            } finally {
                outputSerializer.clear();
            }
        } else {
            userKeySize = userKeySerializer.getLength();
        }

        return super.sizeOfKey() + userKeySize;
    }

    protected long sizeOfUserValue(UV userValue) throws IOException {
        if (userValueSerializer == null || userValue == null) {
            return 0;
        }
        long userValueSize;
        if (userValueSerializer.getLength() == -1) {
            try {
                userValueSerializer.serialize(userValue, outputSerializer);
                userValueSize = outputSerializer.length();
            } finally {
                outputSerializer.clear();
            }
        } else {
            userValueSize = userValueSerializer.getLength();
        }
        return userValueSize;
    }

    private class IterableWrapper<E> implements Iterable<E> {
        private final Iterable<E> iterable;
        private final ThrowingConsumer<E, IOException> trackElementSizeConsumer;

        IterableWrapper(
                Iterable<E> iterable, ThrowingConsumer<E, IOException> trackElementSizeConsumer) {
            this.iterable = iterable;
            this.trackElementSizeConsumer = trackElementSizeConsumer;
        }

        @Override
        public Iterator<E> iterator() {
            return new IteratorWrapper<>(iterable.iterator(), trackElementSizeConsumer);
        }
    }

    private class IteratorWrapper<E> implements Iterator<E> {
        private final Iterator<E> iterator;
        private final ThrowingConsumer<E, IOException> trackElementSizeConsumer;

        IteratorWrapper(Iterator<E> iterator, ThrowingConsumer<E, IOException> trackElementSize) {
            this.iterator = iterator;
            this.trackElementSizeConsumer = trackElementSize;
        }

        @Override
        public boolean hasNext() {
            if (latencyTrackingStateMetric != null
                    && latencyTrackingStateMetric.trackMetricsOnIteratorHasNext()) {
                return trackLatency(
                        iterator::hasNext, MapStateMetrics.MAP_STATE_ITERATOR_HAS_NEXT_LATENCY);
            } else {
                return iterator.hasNext();
            }
        }

        @Override
        public E next() {
            E result;
            if (latencyTrackingStateMetric != null
                    && latencyTrackingStateMetric.trackMetricsOnIteratorNext()) {
                result =
                        trackLatency(
                                iterator::next, MapStateMetrics.MAP_STATE_ITERATOR_NEXT_LATENCY);
            } else {
                result = iterator.next();
            }
            if (sizeTrackingStateMetric != null
                    && sizeTrackingStateMetric.trackMetricsOnIteratorNext()) {
                try {
                    trackElementSizeConsumer.accept(result);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return result;
        }

        @Override
        public void remove() {
            if (latencyTrackingStateMetric != null
                    && latencyTrackingStateMetric.trackMetricsOnIteratorRemove()) {
                trackLatency(iterator::remove, MapStateMetrics.MAP_STATE_ITERATOR_REMOVE_LATENCY);
            } else {
                iterator.remove();
            }
        }
    }

    static class MapStateMetrics extends StateMetricBase {
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
        private static final String MAP_STATE_GET_KEY_SIZE = "mapStateGetKeySize";
        private static final String MAP_STATE_GET_VALUE_SIZE = "mapStateGetValueSize";
        private static final String MAP_STATE_PUT_KEY_SIZE = "mapStatePutKeySize";
        private static final String MAP_STATE_PUT_VALUE_SIZE = "mapStatePutValueSize";
        private static final String MAP_STATE_ITERATOR_KEY_SIZE = "mapStateIteratorKeySize";
        private static final String MAP_STATE_ITERATOR_VALUE_SIZE = "mapStateIteratorValueSize";
        private static final String MAP_STATE_REMOVE_KEY_SIZE = "mapStateRemoveKeySize";
        private static final String MAP_STATE_CONTAINS_KEY_SIZE = "mapStateContainsKeySize";
        private static final String MAP_STATE_IS_EMPTY_KEY_SIZE = "mapStateIsEmptyKeySize";

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

        private MapStateMetrics(
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

        private boolean trackMetricsOnGet() {
            getCount = loopUpdateCounter(getCount);
            return getCount == 1;
        }

        private boolean trackMetricsOnPut() {
            putCount = loopUpdateCounter(putCount);
            return putCount == 1;
        }

        private boolean trackMetricsOnPutAll() {
            putAllCount = loopUpdateCounter(putAllCount);
            return putAllCount == 1;
        }

        private boolean trackMetricsOnRemove() {
            removeCount = loopUpdateCounter(removeCount);
            return removeCount == 1;
        }

        private boolean trackMetricsOnContains() {
            containsCount = loopUpdateCounter(containsCount);
            return containsCount == 1;
        }

        private boolean trackMetricsOnEntriesInit() {
            entriesInitCount = loopUpdateCounter(entriesInitCount);
            return entriesInitCount == 1;
        }

        private boolean trackMetricsOnKeysInit() {
            keysInitCount = loopUpdateCounter(keysInitCount);
            return keysInitCount == 1;
        }

        private boolean trackMetricsOnValuesInit() {
            valuesInitCount = loopUpdateCounter(valuesInitCount);
            return valuesInitCount == 1;
        }

        private boolean trackMetricsOnIteratorInit() {
            iteratorInitCount = loopUpdateCounter(iteratorInitCount);
            return iteratorInitCount == 1;
        }

        private boolean trackMetricsOnIsEmpty() {
            isEmptyCount = loopUpdateCounter(isEmptyCount);
            return isEmptyCount == 1;
        }

        private boolean trackMetricsOnIteratorHasNext() {
            iteratorHasNextCount = loopUpdateCounter(iteratorHasNextCount);
            return iteratorHasNextCount == 1;
        }

        private boolean trackMetricsOnIteratorNext() {
            iteratorNextCount = loopUpdateCounter(iteratorNextCount);
            return iteratorNextCount == 1;
        }

        private boolean trackMetricsOnIteratorRemove() {
            iteratorRemoveCount = loopUpdateCounter(iteratorRemoveCount);
            return iteratorRemoveCount == 1;
        }
    }
}
