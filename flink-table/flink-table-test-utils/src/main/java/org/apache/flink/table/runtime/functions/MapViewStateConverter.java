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
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.types.logical.MapType;

import java.util.HashMap;
import java.util.Map;

/**
 * Converter for MapView state.
 *
 * <p>Converts between external MapView objects and internal MapData representation.
 */
@Internal
class MapViewStateConverter implements StateConverter {

    private final DataStructureConverter<Object, Object> keyConverter;
    private final DataStructureConverter<Object, Object> valueConverter;
    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    MapViewStateConverter(
            MapType mapType,
            DataStructureConverter<Object, Object> keyConverter,
            DataStructureConverter<Object, Object> valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.keyGetter = ArrayData.createElementGetter(mapType.getKeyType());
        this.valueGetter = ArrayData.createElementGetter(mapType.getValueType());
    }

    @Override
    public Object toInternal(Object external) {
        MapView<?, ?> mapView = (MapView<?, ?>) external;
        Map<?, ?> entries = mapView.getMap();

        Map<Object, Object> internalMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            Object internalKey = keyConverter.toInternal(entry.getKey());
            Object internalValue = valueConverter.toInternal(entry.getValue());
            internalMap.put(internalKey, internalValue);
        }
        return new GenericMapData(internalMap);
    }

    @Override
    public Object toExternal(Object internal) {
        MapData mapData = (MapData) internal;
        MapView<Object, Object> mapView = new MapView<>();

        Map<Object, Object> entries = new HashMap<>();
        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();
        for (int i = 0; i < keyArray.size(); i++) {
            Object internalKey = keyGetter.getElementOrNull(keyArray, i);
            Object internalValue = valueGetter.getElementOrNull(valueArray, i);
            Object externalKey = keyConverter.toExternal(internalKey);
            Object externalValue = valueConverter.toExternal(internalValue);
            entries.put(externalKey, externalValue);
        }
        mapView.setMap(entries);
        return mapView;
    }

    @Override
    public Object createNewInternalState() {
        return new GenericMapData(new HashMap<>());
    }
}
