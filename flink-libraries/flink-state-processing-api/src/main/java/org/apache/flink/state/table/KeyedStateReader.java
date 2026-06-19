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

package org.apache.flink.state.table;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

/** Keyed state reader function for value, list and map state types. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class KeyedStateReader extends KeyedStateReaderFunction<Object, RowData> {
    private static final long CACHE_MAX_SIZE = 64000L;

    private final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueProjections;
    private final RowType rowType;
    private final Map<Integer, State> states = new HashMap<>();
    private final Cache<Tuple2<Class, String>, Field> classFieldCache;
    private final Cache<Tuple2<Class, String>, Method> classMethodCache;

    public KeyedStateReader(
            final RowType rowType,
            final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueProjections) {
        this.keyValueProjections = keyValueProjections;
        this.rowType = rowType;
        this.classMethodCache = CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE).build();
        this.classFieldCache = CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE).build();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        for (StateValueColumnConfiguration columnConfig : keyValueProjections.f1) {
            switch (columnConfig.getStateType()) {
                case VALUE:
                    states.put(
                            columnConfig.getColumnIndex(),
                            getRuntimeContext()
                                    .getState(
                                            (ValueStateDescriptor)
                                                    columnConfig.getStateDescriptor()));
                    break;

                case LIST:
                    states.put(
                            columnConfig.getColumnIndex(),
                            getRuntimeContext()
                                    .getListState(
                                            (ListStateDescriptor)
                                                    columnConfig.getStateDescriptor()));
                    break;

                case MAP:
                    states.put(
                            columnConfig.getColumnIndex(),
                            getRuntimeContext()
                                    .getMapState(
                                            (MapStateDescriptor)
                                                    columnConfig.getStateDescriptor()));
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported state type: " + columnConfig.getStateType());
            }
        }
    }

    @Override
    public void close() {
        states.clear();
    }

    @Override
    public void readKey(Object key, Context context, Collector<RowData> out) throws Exception {
        GenericRowData row = new GenericRowData(RowKind.INSERT, 1 + keyValueProjections.f1.size());

        List<RowType.RowField> fields = rowType.getFields();

        // Fill column from key
        int columnIndex = keyValueProjections.f0;
        LogicalType keyLogicalType = fields.get(columnIndex).getType();
        row.setField(columnIndex, getValue(keyLogicalType, key));

        // Fill columns from values
        for (StateValueColumnConfiguration columnConfig : keyValueProjections.f1) {
            LogicalType valueLogicalType = fields.get(columnConfig.getColumnIndex()).getType();
            switch (columnConfig.getStateType()) {
                case VALUE:
                    row.setField(
                            columnConfig.getColumnIndex(),
                            getValue(
                                    valueLogicalType,
                                    ((ValueState) states.get(columnConfig.getColumnIndex()))
                                            .value()));
                    break;

                case LIST:
                    row.setField(
                            columnConfig.getColumnIndex(),
                            getValue(
                                    valueLogicalType,
                                    ((ListState) states.get(columnConfig.getColumnIndex())).get()));
                    break;

                case MAP:
                    row.setField(
                            columnConfig.getColumnIndex(),
                            getValue(
                                    valueLogicalType,
                                    ((MapState) states.get(columnConfig.getColumnIndex()))
                                            .entries()));
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported state type: " + columnConfig.getStateType());
            }
        }

        out.collect(row);
    }

    private Object getValue(LogicalType logicalType, Object object) {
        if (object == null) {
            return null;
        }
        switch (logicalType.getTypeRoot()) {
            case CHAR: // String
            case VARCHAR: // String
                return StringData.fromString(object.toString());

            case BOOLEAN: // Boolean
                return object;

            case BINARY: // byte[]
            case VARBINARY: // ByteBuffer, byte[]
                return convertToBytes(object);

            case DECIMAL: // BigDecimal, ByteBuffer, byte[]
                return convertToDecimal(object, logicalType);

            case TINYINT: // Byte
            case SMALLINT: // Short
            case INTEGER: // Integer
            case BIGINT: // Long
            case FLOAT: // Float
            case DOUBLE: // Double
            case DATE: // Integer
                return object;

            case INTERVAL_YEAR_MONTH: // Long
            case INTERVAL_DAY_TIME: // Long
                return object;

            case ARRAY:
                return convertToArray(object, logicalType);

            case MAP:
                return convertToMap(object, logicalType);

            case ROW:
                return convertToRow(object, logicalType);

            case NULL:
                return null;

            case MULTISET:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            case DESCRIPTOR:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }

    private Object getObjectField(Object object, RowType.RowField rowField) {
        String rowFieldName = rowField.getName();

        Class objectClass = object.getClass();
        Object objectField;
        try {
            Field field =
                    classFieldCache.get(
                            Tuple2.of(objectClass, rowFieldName),
                            () -> objectClass.getField(rowFieldName));
            objectField = field.get(object);
        } catch (ExecutionException e1) {
            Method method = getMethod(objectClass, rowFieldName);
            try {
                objectField = method.invoke(object);
            } catch (IllegalAccessException | InvocationTargetException e2) {
                throw new RuntimeException(e2);
            }
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException(
                    "Cannot access field by either public member or getter function: "
                            + rowField.getName());
        }

        return objectField;
    }

    private Method getMethod(Class objectClass, String rowFieldName) {
        String upperRowFieldName =
                rowFieldName.substring(0, 1).toUpperCase() + rowFieldName.substring(1);
        try {
            String methodName = "get" + upperRowFieldName;
            return classMethodCache.get(
                    Tuple2.of(objectClass, methodName), () -> objectClass.getMethod(methodName));
        } catch (ExecutionException e1) {
            try {
                String methodName = "is" + upperRowFieldName;
                return classMethodCache.get(
                        Tuple2.of(objectClass, methodName),
                        () -> objectClass.getMethod(methodName));
            } catch (ExecutionException e2) {
                throw new RuntimeException(e2);
            }
        }
    }

    private static DecimalData convertToDecimal(Object object, LogicalType logicalType) {
        DecimalType decimalType = (DecimalType) logicalType;

        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        if (object instanceof BigDecimal) {
            return DecimalData.fromBigDecimal((BigDecimal) object, precision, scale);
        } else if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            final byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        } else if (object instanceof byte[]) {
            final byte[] bytes = (byte[]) object;
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        } else {
            throw new UnsupportedOperationException(
                    "Decimal conversion supports only BigDecimal, ByteBuffer and byte[] but received: "
                            + object.getClass().getName());
        }
    }

    private byte[] convertToBytes(Object object) {
        if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else if (object instanceof byte[]) {
            return (byte[]) object;
        } else {
            throw new UnsupportedOperationException(
                    "Byte array conversion supports only ByteBuffer and byte[] but received: "
                            + object.getClass().getName());
        }
    }

    private GenericArrayData convertToArray(Object object, LogicalType logicalType) {
        LogicalType elementLogicalType = ((ArrayType) logicalType).getElementType();

        if (object instanceof Iterable) {
            Iterable iterable = (Iterable) object;
            return new GenericArrayData(
                    StreamSupport.stream(iterable.spliterator(), false)
                            .map(v -> getValue(elementLogicalType, v))
                            .toArray());
        } else {
            throw new UnsupportedOperationException(
                    "Array conversion supports only Iterable but received: "
                            + object.getClass().getName());
        }
    }

    private GenericMapData convertToMap(Object object, LogicalType logicalType) {
        MapType mapType = (MapType) logicalType;
        LogicalType keyLogicalType = mapType.getKeyType();
        LogicalType valueLogicalType = mapType.getValueType();

        if (object instanceof Iterable) {
            Iterable iterable = (Iterable) object;
            Iterator iterator = iterable.iterator();
            Map<Object, Object> result = new HashMap<>();
            boolean typeChecked = false;
            while (iterator.hasNext()) {
                Object e = iterator.next();
                // The boolean check here is for performance tuning because instanceof is slow, and
                // it's enough to check the type only once.
                if (!typeChecked && !(e instanceof Map.Entry)) {
                    throw new UnsupportedOperationException(
                            "Map conversion supports only Iterable<Map.Entry> but received: "
                                    + object.getClass().getName());
                } else {
                    typeChecked = true;
                }
                Map.Entry entry = (Map.Entry) e;
                result.put(
                        getValue(keyLogicalType, entry.getKey()),
                        getValue(valueLogicalType, entry.getValue()));
            }
            return new GenericMapData(result);
        } else {
            throw new UnsupportedOperationException(
                    "Map conversion supports only Iterable<Map.Entry> but received: "
                            + object.getClass().getName());
        }
    }

    private GenericRowData convertToRow(Object object, LogicalType logicalType) {
        RowType rowType = (RowType) logicalType;
        GenericRowData result = new GenericRowData(RowKind.INSERT, rowType.getFieldCount());
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            RowType.RowField subRowField = fields.get(i);
            Object subObject = getObjectField(object, subRowField);
            result.setField(i, getValue(subRowField.getType(), subObject));
        }
        return result;
    }
}
