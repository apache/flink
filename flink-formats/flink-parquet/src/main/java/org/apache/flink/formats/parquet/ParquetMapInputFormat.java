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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link ParquetInputFormat} to read {@link Map} records from Parquet files.
 */
public class ParquetMapInputFormat extends ParquetInputFormat<Map> {

    public ParquetMapInputFormat(Path path, MessageType messageType) {
        super(path, messageType);
    }

    @Override
    protected Map convert(Row row) {
        Map<String, Object> map = new HashMap<>();
        convert(map, row, getFieldTypes(), getFieldNames());
        return map;
    }

    @SuppressWarnings("unchecked")
    private void convert(
            Map<String, Object> map,
            Row row,
            TypeInformation<?>[] fieldTypes,
            String[] fieldNames) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (row.getField(i) != null) {
                if (fieldTypes[i] instanceof BasicTypeInfo
                        || fieldTypes[i] instanceof PrimitiveArrayTypeInfo
                        || fieldTypes[i] instanceof BasicArrayTypeInfo) {
                    map.put(fieldNames[i], row.getField(i));
                } else if (fieldTypes[i] instanceof RowTypeInfo) {
                    Map<String, Object> nestedRow = new HashMap<>();
                    RowTypeInfo nestedRowTypeInfo = (RowTypeInfo) fieldTypes[i];
                    convert(
                            nestedRow,
                            (Row) row.getField(i),
                            nestedRowTypeInfo.getFieldTypes(),
                            nestedRowTypeInfo.getFieldNames());
                    map.put(fieldNames[i], nestedRow);
                } else if (fieldTypes[i] instanceof MapTypeInfo) {
                    Map<String, Object> nestedMap = new HashMap<>();
                    MapTypeInfo mapTypeInfo = (MapTypeInfo) fieldTypes[i];
                    convert(nestedMap, (Map<String, Object>) row.getField(i), mapTypeInfo);
                    map.put(fieldNames[i], nestedMap);
                } else if (fieldTypes[i] instanceof ObjectArrayTypeInfo) {
                    List<Object> nestedObjectList = new ArrayList<>();
                    ObjectArrayTypeInfo objectArrayTypeInfo = (ObjectArrayTypeInfo) fieldTypes[i];
                    convert(nestedObjectList, (Row[]) row.getField(i), objectArrayTypeInfo);
                    map.put(fieldNames[i], nestedObjectList);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void convert(
            Map<String, Object> target, Map<String, Object> source, MapTypeInfo mapTypeInfo) {
        TypeInformation valueTypeInfp = mapTypeInfo.getValueTypeInfo();

        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (valueTypeInfp instanceof RowTypeInfo) {
                Map<String, Object> nestedRow = new HashMap<>();
                convert(
                        nestedRow,
                        (Row) value,
                        ((RowTypeInfo) valueTypeInfp).getFieldTypes(),
                        ((RowTypeInfo) valueTypeInfp).getFieldNames());
                target.put(key, nestedRow);
            } else if (valueTypeInfp instanceof MapTypeInfo) {
                Map<String, Object> nestedMap = new HashMap<>();
                convert(nestedMap, (Map<String, Object>) value, (MapTypeInfo) valueTypeInfp);
                target.put(key, nestedMap);
            } else if (valueTypeInfp instanceof ObjectArrayTypeInfo) {
                List<Object> nestedObjectList = new ArrayList<>();
                convert(nestedObjectList, (Object[]) value, (ObjectArrayTypeInfo) valueTypeInfp);
                target.put(key, nestedObjectList);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void convert(
            List<Object> target, Object[] source, ObjectArrayTypeInfo objectArrayTypeInfo) {
        TypeInformation<?> itemType = objectArrayTypeInfo.getComponentInfo();
        for (Object field : source) {
            if (itemType instanceof RowTypeInfo) {
                Map<String, Object> nestedRow = new HashMap<>();
                convert(
                        nestedRow,
                        (Row) field,
                        ((RowTypeInfo) itemType).getFieldTypes(),
                        ((RowTypeInfo) itemType).getFieldNames());
                target.add(nestedRow);
            } else if (itemType instanceof MapTypeInfo) {
                Map<String, Object> nestedMap = new HashMap<>();
                MapTypeInfo mapTypeInfo = (MapTypeInfo) itemType;
                convert(nestedMap, (Map<String, Object>) field, mapTypeInfo);
                target.add(nestedMap);
            } else if (itemType instanceof ObjectArrayTypeInfo) {
                List<Object> nestedObjectList = new ArrayList<>();
                convert(nestedObjectList, (Row[]) field, (ObjectArrayTypeInfo) itemType);
                target.add(nestedObjectList);
            }
        }
    }
}
