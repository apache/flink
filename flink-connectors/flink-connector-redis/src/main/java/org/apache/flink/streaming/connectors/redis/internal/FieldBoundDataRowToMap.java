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
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 */
public class FieldBoundDataRowToMap implements MapConverter<Row>, DataConverter<Row> {

    private final String[] fieldNames;

    public FieldBoundDataRowToMap(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public Map<String, String> toMap(Row row) {
        int arity = row.getArity();
        if (arity != fieldNames.length) {
            throw new RuntimeException();
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < arity; i++) {
            String key = fieldNames[i];
            String value = (String) row.getField(i);
            map.put(key, value);
        }
        return map;
    }

    @Override
    public Row toData(Map<String, String> map) {
        int size = map.size();
        if (size != fieldNames.length) {
            throw new RuntimeException();
        }
        Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = map.get(fieldNames[i]);
        }
        return Row.of(array);
    }
}
