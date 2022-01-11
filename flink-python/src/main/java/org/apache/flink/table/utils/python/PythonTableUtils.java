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

package org.apache.flink.table.utils.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Python utilities. */
@Internal
public final class PythonTableUtils {

    private PythonTableUtils() {}

    /**
     * Wrap the unpickled python data with an InputFormat. It will be passed to
     * PythonInputFormatTableSource later.
     *
     * @param data The unpickled python data.
     * @param dataType The python data type.
     * @param config The execution config used to create serializer.
     * @return An InputFormat containing the python data.
     */
    public static InputFormat<Row, ?> getInputFormat(
            final List<Object[]> data,
            final TypeInformation<Row> dataType,
            final ExecutionConfig config) {
        Function<Object, Object> converter = converter(dataType);
        return new CollectionInputFormat<>(
                data.stream()
                        .map(objects -> (Row) converter.apply(objects))
                        .collect(Collectors.toList()),
                dataType.createSerializer(config));
    }

    /**
     * Wrap the unpickled python data with an InputFormat. It will be passed to
     * StreamExecutionEnvironment.creatInput() to create an InputFormat later.
     *
     * @param data The unpickled python data.
     * @param dataType The python data type.
     * @param config The execution config used to create serializer.
     * @return An InputFormat containing the python data.
     */
    @SuppressWarnings("unchecked")
    public static <T> InputFormat<T, ?> getCollectionInputFormat(
            final List<T> data, final TypeInformation<T> dataType, final ExecutionConfig config) {
        Function<Object, Object> converter = converter(dataType);
        return new CollectionInputFormat<>(
                data.stream()
                        .map(objects -> (T) converter.apply(objects))
                        .collect(Collectors.toList()),
                dataType.createSerializer(config));
    }

    private static BiFunction<Integer, Function<Integer, Object>, Object> arrayConstructor(
            final TypeInformation<?> elementType, final boolean primitiveArray) {
        if (elementType.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    boolean[] array = new boolean[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (boolean) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Boolean[] array = new Boolean[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Boolean) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    byte[] array = new byte[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (byte) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Byte[] array = new Byte[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Byte) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    short[] array = new short[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (short) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Short[] array = new Short[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Short) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.INT_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    int[] array = new int[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (int) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Integer[] array = new Integer[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Integer) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    long[] array = new long[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (long) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Long[] array = new Long[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Long) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    float[] array = new float[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (float) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Float[] array = new Float[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Float) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    double[] array = new double[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (double) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Double[] array = new Double[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Double) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
            return (length, elementGetter) -> {
                String[] array = new String[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (String) elementGetter.apply(i);
                }
                return array;
            };
        }
        return (length, elementGetter) -> {
            Object[] array = new Object[length];
            for (int i = 0; i < length; i++) {
                array[i] = elementGetter.apply(i);
            }
            return array;
        };
    }

    private static Function<Object, Object> converter(final TypeInformation<?> dataType) {
        if (dataType.equals(Types.BOOLEAN())) {
            return b -> b instanceof Boolean ? b : null;
        }
        if (dataType.equals(Types.BYTE())) {
            return c -> {
                if (c instanceof Byte) {
                    return c;
                }
                if (c instanceof Short) {
                    return ((Short) c).byteValue();
                }
                if (c instanceof Integer) {
                    return ((Integer) c).byteValue();
                }
                if (c instanceof Long) {
                    return ((Long) c).byteValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.SHORT())) {
            return c -> {
                if (c instanceof Byte) {
                    return ((Byte) c).shortValue();
                }
                if (c instanceof Short) {
                    return c;
                }
                if (c instanceof Integer) {
                    return ((Integer) c).shortValue();
                }
                if (c instanceof Long) {
                    return ((Long) c).shortValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.INT())) {
            return c -> {
                if (c instanceof Byte) {
                    return ((Byte) c).intValue();
                }
                if (c instanceof Short) {
                    return ((Short) c).intValue();
                }
                if (c instanceof Integer) {
                    return c;
                }
                if (c instanceof Long) {
                    return ((Long) c).intValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.LONG())) {
            return c -> {
                if (c instanceof Byte) {
                    return ((Byte) c).longValue();
                }
                if (c instanceof Short) {
                    return ((Short) c).longValue();
                }
                if (c instanceof Integer) {
                    return ((Integer) c).longValue();
                }
                if (c instanceof Long) {
                    return c;
                }
                return null;
            };
        }
        if (dataType.equals(Types.FLOAT())) {
            return c -> {
                if (c instanceof Float) {
                    return c;
                }
                if (c instanceof Double) {
                    return ((Double) c).floatValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.DOUBLE())) {
            return c -> {
                if (c instanceof Float) {
                    return ((Float) c).doubleValue();
                }
                if (c instanceof Double) {
                    return c;
                }
                return null;
            };
        }
        if (dataType.equals(Types.DECIMAL())) {
            return c -> c instanceof BigDecimal ? c : null;
        }
        if (dataType.equals(Types.SQL_DATE())) {
            return c -> {
                if (c instanceof Integer) {
                    long millisLocal = ((Integer) c).longValue() * 86400000;
                    long millisUtc =
                            millisLocal - PythonTableUtils.getOffsetFromLocalMillis(millisLocal);
                    return new Date(millisUtc);
                }
                return null;
            };
        }
        if (dataType.equals(Types.SQL_TIME())) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? new Time(((Number) c).longValue() / 1000)
                            : null;
        }
        if (dataType.equals(Types.SQL_TIMESTAMP())) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? new Timestamp(((Number) c).longValue() / 1000)
                            : null;
        }
        if (dataType.equals(org.apache.flink.api.common.typeinfo.Types.INSTANT)) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? Instant.ofEpochMilli(((Number) c).longValue() / 1000)
                            : null;
        }
        if (dataType.equals(Types.INTERVAL_MILLIS())) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? ((Number) c).longValue() / 1000
                            : null;
        }
        if (dataType.equals(Types.STRING())) {
            return c -> c != null ? c.toString() : null;
        }
        if (dataType.equals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
            return c -> {
                if (c instanceof String) {
                    return ((String) c).getBytes(StandardCharsets.UTF_8);
                }
                if (c instanceof byte[]) {
                    return c;
                }
                return null;
            };
        }
        if (dataType instanceof PrimitiveArrayTypeInfo
                || dataType instanceof BasicArrayTypeInfo
                || dataType instanceof ObjectArrayTypeInfo) {
            TypeInformation<?> elementType =
                    dataType instanceof PrimitiveArrayTypeInfo
                            ? ((PrimitiveArrayTypeInfo<?>) dataType).getComponentType()
                            : dataType instanceof BasicArrayTypeInfo
                                    ? ((BasicArrayTypeInfo<?, ?>) dataType).getComponentInfo()
                                    : ((ObjectArrayTypeInfo<?, ?>) dataType).getComponentInfo();
            boolean primitive = dataType instanceof PrimitiveArrayTypeInfo;
            Function<Object, Object> elementConverter = converter(elementType);
            BiFunction<Integer, Function<Integer, Object>, Object> arrayConstructor =
                    arrayConstructor(elementType, primitive);
            return c -> {
                int length = -1;
                Function<Integer, Object> elementGetter = null;
                if (c instanceof List) {
                    length = ((List<?>) c).size();
                    elementGetter = i -> elementConverter.apply(((List<?>) c).get(i));
                }
                if (c != null && c.getClass().isArray()) {
                    length = Array.getLength(c);
                    elementGetter = i -> elementConverter.apply(Array.get(c, i));
                }
                if (elementGetter != null) {
                    return arrayConstructor.apply(length, elementGetter);
                }
                return null;
            };
        }
        if (dataType instanceof MapTypeInfo) {
            Function<Object, Object> keyConverter =
                    converter(((MapTypeInfo<?, ?>) dataType).getKeyTypeInfo());
            Function<Object, Object> valueConverter =
                    converter(((MapTypeInfo<?, ?>) dataType).getValueTypeInfo());
            return c ->
                    c instanceof Map
                            ? ((Map<?, ?>) c)
                                    .entrySet().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            e -> keyConverter.apply(e.getKey()),
                                                            e ->
                                                                    valueConverter.apply(
                                                                            e.getValue())))
                            : null;
        }
        if (dataType instanceof RowTypeInfo) {
            TypeInformation<?>[] fieldTypes = ((RowTypeInfo) dataType).getFieldTypes();
            List<Function<Object, Object>> fieldConverters =
                    Arrays.stream(fieldTypes)
                            .map(PythonTableUtils::converter)
                            .collect(Collectors.toList());
            return c -> {
                if (c != null && c.getClass().isArray()) {
                    int length = Array.getLength(c);
                    if (length - 1 != fieldTypes.length) {
                        throw new IllegalStateException(
                                "Input row doesn't have expected number of values required by the schema. "
                                        + fieldTypes.length
                                        + " fields are required while "
                                        + (length - 1)
                                        + " values are provided.");
                    }

                    Row row = new Row(length - 1);
                    row.setKind(RowKind.fromByteValue(((Number) Array.get(c, 0)).byteValue()));

                    for (int i = 0; i < row.getArity(); i++) {
                        row.setField(i, fieldConverters.get(i).apply(Array.get(c, i + 1)));
                    }

                    return row;
                }
                return null;
            };
        }
        if (dataType instanceof TupleTypeInfo) {
            TypeInformation<?>[] fieldTypes = ((TupleTypeInfo<?>) dataType).getFieldTypes();
            List<Function<Object, Object>> fieldConverters =
                    Arrays.stream(fieldTypes)
                            .map(PythonTableUtils::converter)
                            .collect(Collectors.toList());
            return c -> {
                if (c != null && c.getClass().isArray()) {
                    int length = Array.getLength(c);
                    if (length != fieldTypes.length) {
                        throw new IllegalStateException(
                                "Input tuple doesn't have expected number of values required by the schema. "
                                        + fieldTypes.length
                                        + " fields are required while "
                                        + length
                                        + " values are provided.");
                    }

                    Tuple tuple = Tuple.newInstance(length);
                    for (int i = 0; i < tuple.getArity(); i++) {
                        tuple.setField(fieldConverters.get(i).apply(Array.get(c, i)), i);
                    }

                    return tuple;
                }
                return null;
            };
        }

        return Function.identity();
    }

    private static int getOffsetFromLocalMillis(final long millisLocal) {
        TimeZone localZone = TimeZone.getDefault();
        int result = localZone.getRawOffset();
        // the actual offset should be calculated based on milliseconds in UTC
        int offset = localZone.getOffset(millisLocal - (long) result);
        if (offset != result) {
            // DayLight Saving Time
            result = localZone.getOffset(millisLocal - (long) offset);
            if (result != offset) {
                // fallback to do the reverse lookup using java.time.LocalDateTime
                // this should only happen near the start or end of DST
                LocalDate localDate = LocalDate.ofEpochDay(millisLocal / 86400000L);
                LocalTime localTime =
                        LocalTime.ofNanoOfDay(
                                Math.floorMod(millisLocal, 86400000L) * 1000L * 1000L);
                LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
                long millisEpoch =
                        localDateTime.atZone(localZone.toZoneId()).toInstant().toEpochMilli();
                result = (int) (millisLocal - millisEpoch);
            }
        }

        return result;
    }
}
