/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.python;

import org.apache.flink.api.common.python.pickle.ArrayConstructor;
import org.apache.flink.api.common.python.pickle.ByteArrayConstructor;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.Row;

import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.DATE;
import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIME;

/**
 * Utility class that contains helper methods to create a TableSource from a file which contains
 * Python objects.
 */
public final class PythonBridgeUtils {

    private static Object[] getObjectArrayFromUnpickledData(Object input) {
        if (input.getClass().isArray()) {
            return (Object[]) input;
        } else {
            return ((ArrayList<Object>) input).toArray(new Object[0]);
        }
    }

    public static List<Object[]> readPythonObjects(String fileName, boolean batched)
            throws IOException {
        List<byte[]> data = readPickledBytes(fileName);
        Unpickler unpickle = new Unpickler();
        initialize();
        List<Object[]> unpickledData = new ArrayList<>();
        for (byte[] pickledData : data) {
            Object obj = unpickle.loads(pickledData);
            if (batched) {
                if (obj instanceof Object[]) {
                    Object[] arrayObj = (Object[]) obj;
                    for (Object o : arrayObj) {
                        unpickledData.add(getObjectArrayFromUnpickledData(o));
                    }
                } else {
                    for (Object o : (ArrayList<Object>) obj) {
                        unpickledData.add(getObjectArrayFromUnpickledData(o));
                    }
                }
            } else {
                unpickledData.add(getObjectArrayFromUnpickledData(obj));
            }
        }
        return unpickledData;
    }

    public static List<?> readPythonObjects(String fileName) throws IOException {
        List<byte[]> data = readPickledBytes(fileName);
        Unpickler unpickle = new Unpickler();
        initialize();
        return data.stream()
                .map(
                        pickledData -> {
                            try {
                                Object obj = unpickle.loads(pickledData);
                                return obj.getClass().isArray() || obj instanceof List
                                        ? getObjectArrayFromUnpickledData(obj)
                                        : obj;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(Collectors.toList());
    }

    public static byte[] convertLiteralToPython(RexLiteral o, SqlTypeName typeName) {
        byte type;
        Object value;
        Pickler pickler = new Pickler();
        if (o.getValue3() == null) {
            type = 0;
            value = null;
        } else {
            switch (typeName) {
                case TINYINT:
                    type = 0;
                    value = ((BigDecimal) o.getValue3()).byteValueExact();
                    break;
                case SMALLINT:
                    type = 0;
                    value = ((BigDecimal) o.getValue3()).shortValueExact();
                    break;
                case INTEGER:
                    type = 0;
                    value = ((BigDecimal) o.getValue3()).intValueExact();
                    break;
                case BIGINT:
                    type = 0;
                    value = ((BigDecimal) o.getValue3()).longValueExact();
                    break;
                case FLOAT:
                    type = 0;
                    value = ((BigDecimal) o.getValue3()).floatValue();
                    break;
                case DOUBLE:
                    type = 0;
                    value = ((BigDecimal) o.getValue3()).doubleValue();
                    break;
                case DECIMAL:
                case BOOLEAN:
                    type = 0;
                    value = o.getValue3();
                    break;
                case CHAR:
                case VARCHAR:
                    type = 0;
                    value = o.getValue3().toString();
                    break;
                case DATE:
                    type = 1;
                    value = o.getValue3();
                    break;
                case TIME:
                    type = 2;
                    value = o.getValue3();
                    break;
                case TIMESTAMP:
                    type = 3;
                    value = o.getValue3();
                    break;
                default:
                    throw new RuntimeException("Unsupported type " + typeName);
            }
        }
        byte[] pickledData;
        try {
            pickledData = pickler.dumps(value);
        } catch (IOException e) {
            throw new RuntimeException("Pickle Java object failed", e);
        }
        byte[] typePickledData = new byte[pickledData.length + 1];
        typePickledData[0] = type;
        System.arraycopy(pickledData, 0, typePickledData, 1, pickledData.length);
        return typePickledData;
    }

    public static List<byte[]> readPickledBytes(final String fileName) throws IOException {
        List<byte[]> objs = new LinkedList<>();
        try (DataInputStream din = new DataInputStream(new FileInputStream(fileName))) {
            try {
                while (true) {
                    final int length = din.readInt();
                    byte[] obj = new byte[length];
                    din.readFully(obj);
                    objs.add(obj);
                }
            } catch (EOFException eof) {
                // expected
            }
        }
        return objs;
    }

    private static Object getPickledBytesFromJavaObject(Object obj, LogicalType dataType)
            throws IOException {
        Pickler pickler = new Pickler();
        initialize();
        if (obj == null) {
            return new byte[0];
        } else {
            if (dataType instanceof DateType) {
                long time;
                if (obj instanceof LocalDate) {
                    time = ((LocalDate) (obj)).toEpochDay();
                } else {
                    time = ((Date) obj).toLocalDate().toEpochDay();
                }
                return pickler.dumps(time);
            } else if (dataType instanceof TimeType) {
                long time;
                if (obj instanceof LocalTime) {
                    time = ((LocalTime) obj).toNanoOfDay();
                } else {
                    time = ((Time) obj).toLocalTime().toNanoOfDay();
                }
                time = time / 1000;
                return pickler.dumps(time);
            } else if (dataType instanceof TimestampType) {
                if (obj instanceof LocalDateTime) {
                    return pickler.dumps(Timestamp.valueOf((LocalDateTime) obj));
                } else {
                    return pickler.dumps(obj);
                }
            } else if (dataType instanceof RowType) {
                Row tmpRow = (Row) obj;
                LogicalType[] tmpRowFieldTypes =
                        ((RowType) dataType).getChildren().toArray(new LogicalType[0]);
                List<Object> rowFieldBytes = new ArrayList<>(tmpRow.getArity() + 1);
                rowFieldBytes.add(new byte[] {tmpRow.getKind().toByteValue()});
                for (int i = 0; i < tmpRow.getArity(); i++) {
                    rowFieldBytes.add(
                            getPickledBytesFromJavaObject(tmpRow.getField(i), tmpRowFieldTypes[i]));
                }
                return rowFieldBytes;
            } else if (dataType instanceof MapType) {
                List<List<Object>> serializedMapKV = new ArrayList<>(2);
                MapType mapType = (MapType) dataType;
                Map<Object, Object> mapObj = (Map) obj;
                List<Object> keyBytesList = new ArrayList<>(mapObj.size());
                List<Object> valueBytesList = new ArrayList<>(mapObj.size());
                for (Map.Entry entry : mapObj.entrySet()) {
                    keyBytesList.add(
                            getPickledBytesFromJavaObject(entry.getKey(), mapType.getKeyType()));
                    valueBytesList.add(
                            getPickledBytesFromJavaObject(
                                    entry.getValue(), mapType.getValueType()));
                }
                serializedMapKV.add(keyBytesList);
                serializedMapKV.add(valueBytesList);
                return pickler.dumps(serializedMapKV);
            } else if (dataType instanceof ArrayType) {
                Object[] objects = (Object[]) obj;
                List<Object> serializedElements = new ArrayList<>(objects.length);
                ArrayType arrayType = (ArrayType) dataType;
                LogicalType elementType = arrayType.getElementType();
                for (Object object : objects) {
                    serializedElements.add(getPickledBytesFromJavaObject(object, elementType));
                }
                return pickler.dumps(serializedElements);
            }
            if (dataType instanceof FloatType) {
                return pickler.dumps(String.valueOf(obj));
            } else {
                return pickler.dumps(obj);
            }
        }
    }

    public static Object getPickledBytesFromJavaObject(Object obj, TypeInformation<?> dataType)
            throws IOException {
        Pickler pickler = new Pickler();
        initialize();
        if (obj == null) {
            return new byte[0];
        } else {
            if (dataType instanceof SqlTimeTypeInfo) {
                SqlTimeTypeInfo<?> sqlTimeTypeInfo =
                        SqlTimeTypeInfo.getInfoFor(dataType.getTypeClass());
                if (sqlTimeTypeInfo == DATE) {
                    return pickler.dumps(((Date) obj).toLocalDate().toEpochDay());
                } else if (sqlTimeTypeInfo == TIME) {
                    return pickler.dumps(((Time) obj).toLocalTime().toNanoOfDay() / 1000);
                }
            } else if (dataType instanceof RowTypeInfo || dataType instanceof TupleTypeInfo) {
                TypeInformation<?>[] fieldTypes = ((TupleTypeInfoBase<?>) dataType).getFieldTypes();
                int arity =
                        dataType instanceof RowTypeInfo
                                ? ((Row) obj).getArity()
                                : ((Tuple) obj).getArity();
                List<Object> fieldBytes = new ArrayList<>(arity + 1);
                if (dataType instanceof RowTypeInfo) {
                    fieldBytes.add(new byte[] {((Row) obj).getKind().toByteValue()});
                }
                for (int i = 0; i < arity; i++) {
                    Object field =
                            dataType instanceof RowTypeInfo
                                    ? ((Row) obj).getField(i)
                                    : ((Tuple) obj).getField(i);
                    fieldBytes.add(getPickledBytesFromJavaObject(field, fieldTypes[i]));
                }
                return fieldBytes;
            } else if (dataType instanceof BasicArrayTypeInfo
                    || dataType instanceof PrimitiveArrayTypeInfo) {
                Object[] objects = (Object[]) obj;
                List<Object> serializedElements = new ArrayList<>(objects.length);
                TypeInformation<?> elementType =
                        dataType instanceof BasicArrayTypeInfo
                                ? ((BasicArrayTypeInfo<?, ?>) dataType).getComponentInfo()
                                : ((PrimitiveArrayTypeInfo<?>) dataType).getComponentType();
                for (Object object : objects) {
                    serializedElements.add(getPickledBytesFromJavaObject(object, elementType));
                }
                return pickler.dumps(serializedElements);
            } else if (dataType instanceof MapTypeInfo) {
                List<List<Object>> serializedMapKV = new ArrayList<>(2);
                Map<Object, Object> mapObj = (Map) obj;
                List<Object> keyBytesList = new ArrayList<>(mapObj.size());
                List<Object> valueBytesList = new ArrayList<>(mapObj.size());
                for (Map.Entry entry : mapObj.entrySet()) {
                    keyBytesList.add(
                            getPickledBytesFromJavaObject(
                                    entry.getKey(), ((MapTypeInfo) dataType).getKeyTypeInfo()));
                    valueBytesList.add(
                            getPickledBytesFromJavaObject(
                                    entry.getValue(), ((MapTypeInfo) dataType).getValueTypeInfo()));
                }
                serializedMapKV.add(keyBytesList);
                serializedMapKV.add(valueBytesList);
                return pickler.dumps(serializedMapKV);
            } else if (dataType instanceof ListTypeInfo) {
                List objects = (List) obj;
                List<Object> serializedElements = new ArrayList<>(objects.size());
                TypeInformation elementType = ((ListTypeInfo) dataType).getElementTypeInfo();
                for (Object object : objects) {
                    serializedElements.add(getPickledBytesFromJavaObject(object, elementType));
                }
                return pickler.dumps(serializedElements);
            }
            if (dataType instanceof BasicTypeInfo
                    && BasicTypeInfo.getInfoFor(dataType.getTypeClass()) == FLOAT_TYPE_INFO) {
                // Serialization of float type with pickler loses precision.
                return pickler.dumps(String.valueOf(obj));
            } else {
                return pickler.dumps(obj);
            }
        }
    }

    public static Object getPickledBytesFromRow(Row row, DataType[] dataTypes) throws IOException {
        LogicalType[] logicalTypes =
                Arrays.stream(dataTypes).map(f -> f.getLogicalType()).toArray(LogicalType[]::new);

        return getPickledBytesFromJavaObject(row, RowType.of(logicalTypes));
    }

    private static boolean initialized = false;

    private static void initialize() {
        synchronized (PythonBridgeUtils.class) {
            if (!initialized) {
                Unpickler.registerConstructor("array", "array", new ArrayConstructor());
                Unpickler.registerConstructor(
                        "__builtin__", "bytearray", new ByteArrayConstructor());
                Unpickler.registerConstructor("builtins", "bytearray", new ByteArrayConstructor());
                Unpickler.registerConstructor("__builtin__", "bytes", new ByteArrayConstructor());
                Unpickler.registerConstructor("_codecs", "encode", new ByteArrayConstructor());
                initialized = true;
            }
        }
    }
}
