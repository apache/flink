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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.conversion.DataStructureConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Converter for MapView state.
 *
 * <p>Internal storage is always a {@link TtlAwareMapView} with keys and values in converted form.
 */
@Internal
class MapViewStateConverter implements StateConverter {

    private final DataStructureConverter<Object, Object> keyConverter;
    private final DataStructureConverter<Object, Object> valueConverter;
    private final LongSupplier clock;

    MapViewStateConverter(
            DataStructureConverter<Object, Object> keyConverter,
            DataStructureConverter<Object, Object> valueConverter,
            LongSupplier clock) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.clock = clock;
    }

    @Override
    public Object toInternal(Object external) {
        return convertView(
                (TtlAwareMapView<?, ?>) external,
                keyConverter::toInternal,
                valueConverter::toInternal);
    }

    @Override
    public Object toExternal(Object internal) {
        return convertView(
                (TtlAwareMapView<?, ?>) internal,
                keyConverter::toExternal,
                valueConverter::toExternal);
    }

    @Override
    public Object createNewInternalState() {
        return new TtlAwareMapView<>(clock);
    }

    @Override
    public void evictExpired(Object internalState, long now, long ttlMillis) {
        ((TtlAwareMapView<?, ?>) internalState).evictExpired(now, ttlMillis);
    }

    private TtlAwareMapView<Object, Object> convertView(
            TtlAwareMapView<?, ?> source,
            Function<Object, Object> convertKey,
            Function<Object, Object> convertValue) {
        Map<Object, Object> entries = new HashMap<>();
        source.getMap().forEach((k, v) -> entries.put(convertKey.apply(k), convertValue.apply(v)));
        Map<Object, Long> timestamps =
                source.getTimestamps().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> convertKey.apply(e.getKey()), Map.Entry::getValue));
        TtlAwareMapView<Object, Object> newView = new TtlAwareMapView<>(clock);
        newView.setMapWithTimestamps(entries, timestamps);
        return newView;
    }
}
