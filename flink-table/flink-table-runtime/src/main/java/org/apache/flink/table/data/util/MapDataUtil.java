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

package org.apache.flink.table.data.util;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashMap;
import java.util.Map;

/** Utilities for {@link MapData}. */
public final class MapDataUtil {

    /**
     * Converts a {@link MapData} into Java {@link Map}, the keys and values of the Java map still
     * holds objects of internal data structures.
     */
    public static Map<Object, Object> convertToJavaMap(
            MapData map, LogicalType keyType, LogicalType valueType) {
        ArrayData keyArray = map.keyArray();
        ArrayData valueArray = map.valueArray();
        Map<Object, Object> javaMap = new HashMap<>();
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        for (int i = 0; i < map.size(); i++) {
            Object key = keyGetter.getElementOrNull(keyArray, i);
            Object value = valueGetter.getElementOrNull(valueArray, i);
            javaMap.put(key, value);
        }
        return javaMap;
    }
}
