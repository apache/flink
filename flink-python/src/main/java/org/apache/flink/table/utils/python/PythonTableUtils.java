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
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.RowKind;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Python utilities. */
@Internal
public final class PythonTableUtils {

    private PythonTableUtils() {}

    /**
     * Create a table from {@link PythonDynamicTableSource} that read data from input file with
     * specific {@link DataType}.
     *
     * @param tEnv The TableEnvironment to create table.
     * @param filePath the file path of the input data.
     * @param schema The python data type.
     * @param batched Whether to read data in a batch
     * @return Table with InputFormat.
     */
    public static Table createTableFromElement(
            TableEnvironment tEnv, String filePath, DataType schema, boolean batched) {
        TableDescriptor.Builder builder =
                TableDescriptor.forConnector(PythonDynamicTableFactory.IDENTIFIER)
                        .option(PythonDynamicTableOptions.INPUT_FILE_PATH, filePath)
                        .option(PythonDynamicTableOptions.BATCH_MODE, batched)
                        .schema(Schema.newBuilder().fromRowDataType(schema).build());
        return tEnv.from(builder.build());
    }

    /**
     * Wrap the unpickled python data with an InputFormat. It will be passed to
     * PythonDynamicTableSource later.
     *
     * @param data The unpickled python data.
     * @param dataType The python data type.
     * @return An InputFormat containing the python data.
     */
    public static InputFormat<RowData, ?> getInputFormat(
            final List<Object[]> data, final DataType dataType) {
        Function<Object, Object> converter = converter(dataType.getLogicalType());
        Collection<RowData> dataCollection =
                data.stream()
                        .map(objects -> (RowData) converter.apply(objects))
                        .collect(Collectors.toList());
        return new CollectionInputFormat<>(
                dataCollection, InternalSerializers.create(dataType.getLogicalType()));
    }

    private static BiFunction<Integer, Function<Integer, Object>, Object> arrayConstructor(
            final LogicalType elementType) {
        if (elementType instanceof BooleanType) {
            return (length, elementGetter) -> {
                Boolean[] array = new Boolean[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (Boolean) elementGetter.apply(i);
                }
                return new GenericArrayData(array);
            };
        }
        if (elementType instanceof TinyIntType) {
            return (length, elementGetter) -> {
                Byte[] array = new Byte[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (Byte) elementGetter.apply(i);
                }
                return new GenericArrayData(array);
            };
        }

        if (elementType instanceof IntType) {
            return (length, elementGetter) -> {
                Integer[] array = new Integer[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (Integer) elementGetter.apply(i);
                }
                return new GenericArrayData(array);
            };
        }
        if (elementType instanceof BigIntType) {
            return (length, elementGetter) -> {
                Long[] array = new Long[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (Long) elementGetter.apply(i);
                }
                return new GenericArrayData(array);
            };
        }
        if (elementType instanceof FloatType) {
            return (length, elementGetter) -> {
                Float[] array = new Float[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (Float) elementGetter.apply(i);
                }
                return new GenericArrayData(array);
            };
        }
        if (elementType instanceof DoubleType) {
            return (length, elementGetter) -> {
                Double[] array = new Double[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (Double) elementGetter.apply(i);
                }
                return new GenericArrayData(array);
            };
        }
        if (elementType instanceof CharType || elementType instanceof VarCharType) {
            return (length, elementGetter) -> {
                StringData[] array = new StringData[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (StringData) elementGetter.apply(i);
                }
                return new GenericArrayData(array);
            };
        }

        return (length, elementGetter) -> {
            Object[] array = new Object[length];
            for (int i = 0; i < length; i++) {
                array[i] = elementGetter.apply(i);
            }
            return new GenericArrayData(array);
        };
    }

    private static Function<Object, Object> converter(LogicalType logicalType) {

        if (logicalType instanceof NullType) {
            return n -> null;
        }

        if (logicalType instanceof BooleanType) {
            return b -> b instanceof Boolean ? b : null;
        }

        if (logicalType instanceof TinyIntType) {
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

        if (logicalType instanceof SmallIntType) {
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

        if (logicalType instanceof IntType) {
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

        if (logicalType instanceof BigIntType) {
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

        if (logicalType instanceof FloatType) {
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
        if (logicalType instanceof DoubleType) {
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

        if (logicalType instanceof DecimalType) {
            int precision = ((DecimalType) logicalType).getPrecision();
            int scale = ((DecimalType) logicalType).getScale();
            return c ->
                    c instanceof BigDecimal
                            ? DecimalData.fromBigDecimal((BigDecimal) c, precision, scale)
                            : null;
        }

        if (logicalType instanceof DateType) {
            return c -> {
                if (c instanceof Integer) {
                    return (Integer) c;
                }
                return null;
            };
        }

        if (logicalType instanceof TimeType) {
            return c -> {
                if (c instanceof Integer || c instanceof Long) {
                    long millisLocal = ((Number) c).longValue() / 1000;
                    long millisUtc = millisLocal + getOffsetFromLocalMillis(millisLocal);
                    return (int) millisUtc;
                }
                return null;
            };
        }

        if (logicalType instanceof TimestampType) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? TimestampData.fromLocalDateTime(
                                    Instant.ofEpochMilli(((Number) c).longValue() / 1000)
                                            .atZone(ZoneId.systemDefault())
                                            .toLocalDateTime())
                            : null;
        }

        if (logicalType instanceof ZonedTimestampType) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? TimestampData.fromInstant(
                                    Instant.ofEpochMilli(((Number) c).longValue() / 1000))
                            : null;
        }

        if (logicalType instanceof LocalZonedTimestampType) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? TimestampData.fromInstant(
                                    Instant.ofEpochMilli(((Number) c).longValue() / 1000))
                            : null;
        }

        if (logicalType instanceof DayTimeIntervalType) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? ((Number) c).longValue() / 1000
                            : null;
        }

        if (logicalType instanceof YearMonthIntervalType) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? ((Number) c).longValue() / 1000
                            : null;
        }

        if (logicalType instanceof CharType || logicalType instanceof VarCharType) {
            return c -> c != null ? StringData.fromString(c.toString()) : null;
        }

        if (logicalType instanceof BinaryType || logicalType instanceof VarBinaryType) {
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

        if (logicalType instanceof ArrayType) {
            LogicalType elementType = ((ArrayType) logicalType).getElementType();
            Function<Object, Object> elementConverter = converter(elementType);
            BiFunction<Integer, Function<Integer, Object>, Object> arrayConstructor =
                    arrayConstructor(elementType);
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

        if (logicalType instanceof MultisetType) {
            return c -> c;
        }

        if (logicalType instanceof MapType) {
            Function<Object, Object> keyConverter = converter(((MapType) logicalType).getKeyType());
            Function<Object, Object> valueConverter =
                    converter(((MapType) logicalType).getValueType());

            return c -> {
                if (c instanceof Map) {
                    Map<?, ?> mapData =
                            ((Map<?, ?>) c)
                                    .entrySet().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            e -> keyConverter.apply(e.getKey()),
                                                            e ->
                                                                    valueConverter.apply(
                                                                            e.getValue())));
                    return new GenericMapData(mapData);
                } else {
                    return null;
                }
            };
        }

        if (logicalType instanceof RowType) {
            LogicalType[] fieldTypes = logicalType.getChildren().toArray(new LogicalType[0]);
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

                    GenericRowData row = new GenericRowData(length - 1);
                    row.setRowKind(RowKind.fromByteValue(((Number) Array.get(c, 0)).byteValue()));

                    for (int i = 0; i < row.getArity(); i++) {
                        row.setField(i, fieldConverters.get(i).apply(Array.get(c, i + 1)));
                    }

                    return row;
                }
                return null;
            };
        } else if (logicalType instanceof StructuredType) {
            Optional<Class<?>> implClass = ((StructuredType) logicalType).getImplementationClass();
            if (implClass.isPresent()
                    && (implClass.get() == ListView.class || implClass.get() == MapView.class)) {
                return converter(logicalType.getChildren().get(0));
            }
            throw new IllegalStateException(
                    "Failed to get the data converter for StructuredType with implementation "
                            + "class: "
                            + implClass.orElse(null));
        }

        throw new IllegalStateException("Failed to get converter for LogicalType: " + logicalType);
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
