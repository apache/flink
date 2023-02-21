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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;

import java.util.HashMap;
import java.util.Map;

/** Converter for {@link MapType}/{@link MultisetType} of {@link Map} external type. */
@Internal
public class MapMapConverter<K, V> implements DataStructureConverter<MapData, Map<K, V>> {

    private static final long serialVersionUID = 1L;

    private final ArrayObjectArrayConverter<K> keyConverter;

    private final ArrayObjectArrayConverter<V> valueConverter;

    private final boolean hasInternalEntries;

    private MapMapConverter(
            ArrayObjectArrayConverter<K> keyConverter,
            ArrayObjectArrayConverter<V> valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.hasInternalEntries =
                keyConverter.hasInternalElements && valueConverter.hasInternalElements;
    }

    @Override
    public void open(ClassLoader classLoader) {
        keyConverter.open(classLoader);
        valueConverter.open(classLoader);
    }

    @Override
    public MapData toInternal(Map<K, V> external) {
        if (hasInternalEntries) {
            return new GenericMapData(external);
        }
        return toBinaryMapData(external);
    }

    @Override
    public Map<K, V> toExternal(MapData internal) {
        final ArrayData keyArray = internal.keyArray();
        final ArrayData valueArray = internal.valueArray();

        final int length = internal.size();
        final Map<K, V> map = new HashMap<>();
        for (int pos = 0; pos < length; pos++) {
            final Object keyValue = keyConverter.elementGetter.getElementOrNull(keyArray, pos);
            final Object valueValue =
                    valueConverter.elementGetter.getElementOrNull(valueArray, pos);
            map.put(
                    keyConverter.elementConverter.toExternalOrNull(keyValue),
                    valueConverter.elementConverter.toExternalOrNull(valueValue));
        }
        return map;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime helper methods
    // --------------------------------------------------------------------------------------------

    private MapData toBinaryMapData(Map<K, V> external) {
        final int length = external.size();
        keyConverter.allocateWriter(length);
        valueConverter.allocateWriter(length);
        int pos = 0;
        for (Map.Entry<K, V> entry : external.entrySet()) {
            keyConverter.writeElement(pos, entry.getKey());
            valueConverter.writeElement(pos, entry.getValue());
            pos++;
        }
        return BinaryMapData.valueOf(
                keyConverter.completeWriter(), valueConverter.completeWriter());
    }

    // --------------------------------------------------------------------------------------------
    // Factory method
    // --------------------------------------------------------------------------------------------

    public static MapMapConverter<?, ?> createForMapType(DataType dataType) {
        final DataType keyDataType = dataType.getChildren().get(0);
        final DataType valueDataType = dataType.getChildren().get(1);
        return new MapMapConverter<>(
                ArrayObjectArrayConverter.createForElement(keyDataType),
                ArrayObjectArrayConverter.createForElement(valueDataType));
    }

    public static MapMapConverter<?, ?> createForMultisetType(DataType dataType) {
        final DataType keyDataType = dataType.getChildren().get(0);
        final DataType valueDataType = DataTypes.INT().notNull();
        return new MapMapConverter<>(
                ArrayObjectArrayConverter.createForElement(keyDataType),
                ArrayObjectArrayConverter.createForElement(valueDataType));
    }
}
