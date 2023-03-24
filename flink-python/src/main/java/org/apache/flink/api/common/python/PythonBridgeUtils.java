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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.typeinfo.python.PickledByteArrayTypeInfo;
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
import org.apache.flink.util.Preconditions;

import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
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

    // This method is reflected from planner
    public static byte[] pickleValue(Object value, byte type) {
        Pickler pickler = new Pickler();
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
            return pickler.dumps(null);
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
                    || dataType instanceof PrimitiveArrayTypeInfo
                    || dataType instanceof ObjectArrayTypeInfo) {
                Object[] objects;
                TypeInformation<?> elementType;
                if (dataType instanceof BasicArrayTypeInfo) {
                    objects = (Object[]) obj;
                    elementType = ((BasicArrayTypeInfo<?, ?>) dataType).getComponentInfo();
                } else if (dataType instanceof PrimitiveArrayTypeInfo) {
                    objects = primitiveArrayConverter(obj, dataType);
                    elementType = ((PrimitiveArrayTypeInfo<?>) dataType).getComponentType();
                } else {
                    objects = (Object[]) obj;
                    elementType = ((ObjectArrayTypeInfo<?, ?>) dataType).getComponentInfo();
                }
                List<Object> serializedElements = new ArrayList<>(objects.length);
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
            } else if (dataType instanceof PickledByteArrayTypeInfo
                    || dataType instanceof BasicTypeInfo) {
                return pickler.dumps(obj);
            } else {
                // other typeinfos will use the corresponding serializer to serialize data.
                TypeSerializer serializer = dataType.createSerializer(null);
                ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
                DataOutputViewStreamWrapper baosWrapper = new DataOutputViewStreamWrapper(baos);
                serializer.serialize(obj, baosWrapper);
                return pickler.dumps(baos.toByteArray());
            }
        }
    }

    private static Object[] primitiveArrayConverter(
            Object array, TypeInformation<?> arrayTypeInfo) {
        Preconditions.checkArgument(arrayTypeInfo instanceof PrimitiveArrayTypeInfo);
        Preconditions.checkArgument(array.getClass().isArray());
        Object[] objects;
        if (PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            boolean[] booleans = (boolean[]) array;
            objects = new Object[booleans.length];
            for (int i = 0; i < booleans.length; i++) {
                objects[i] = booleans[i];
            }
        } else if (PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            byte[] bytes = (byte[]) array;
            objects = new Object[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                objects[i] = bytes[i];
            }
        } else if (PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            short[] shorts = (short[]) array;
            objects = new Object[shorts.length];
            for (int i = 0; i < shorts.length; i++) {
                objects[i] = shorts[i];
            }
        } else if (PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            int[] ints = (int[]) array;
            objects = new Object[ints.length];
            for (int i = 0; i < ints.length; i++) {
                objects[i] = ints[i];
            }
        } else if (PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            long[] longs = (long[]) array;
            objects = new Object[longs.length];
            for (int i = 0; i < longs.length; i++) {
                objects[i] = longs[i];
            }
        } else if (PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            float[] floats = (float[]) array;
            objects = new Object[floats.length];
            for (int i = 0; i < floats.length; i++) {
                objects[i] = floats[i];
            }
        } else if (PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            double[] doubles = (double[]) array;
            objects = new Object[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                objects[i] = doubles[i];
            }
        } else if (PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO.equals(arrayTypeInfo)) {
            char[] chars = (char[]) array;
            objects = new Object[chars.length];
            for (int i = 0; i < chars.length; i++) {
                objects[i] = chars[i];
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Primitive array of %s is not supported in PyFlink yet",
                            ((PrimitiveArrayTypeInfo<?>) arrayTypeInfo)
                                    .getComponentType()
                                    .getTypeClass()
                                    .getSimpleName()));
        }
        return objects;
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
