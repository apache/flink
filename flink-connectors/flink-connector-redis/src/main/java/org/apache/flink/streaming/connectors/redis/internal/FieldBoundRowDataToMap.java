/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.internal;

import org.apache.flink.streaming.connectors.redis.DataConverter;
import org.apache.flink.streaming.connectors.redis.MapConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.LinkedHashMap;
import java.util.Map;

/** */
public class FieldBoundRowDataToMap implements MapConverter<RowData>, DataConverter<RowData> {

    private final String[] fieldNames;

    public FieldBoundRowDataToMap(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public Map<String, String> toMap(RowData row) {
        int arity = row.getArity();
        if (arity != fieldNames.length) {
            throw new RuntimeException();
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < arity; i++) {
            String key = fieldNames[i];
            String value = row.getString(i).toString();
            map.put(key, value);
        }
        return map;
    }

    @Override
    public RowData toData(Map<String, String> map) {
        int size = map.size();
        if (size != fieldNames.length) {
            throw new RuntimeException();
        }
        Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = map.get(fieldNames[i]);
        }
        return GenericRowData.of(array);
    }
}
