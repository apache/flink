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

package org.apache.flink.table.utils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utilities to convert values to SQL String representations. */
public class StringUtils {

    /** Like {@link #toSQLString(Object, LogicalType, ZoneId)} using the UTC timezone. */
    public static String toSQLString(Object value, LogicalType fieldType) {
        return toSQLString(value, fieldType, DateTimeUtils.UTC_ZONE.toZoneId());
    }

    /**
     * Returns the SQL String representation of the provided value. This method supports both
     * internal and external types.
     *
     * @param value the value to convert
     * @param fieldType the field logical type
     * @param sessionTimeZone the session time zone, used for time and date types printing
     * @return the string SQL representation of the provided value
     */
    public static String toSQLString(Object value, LogicalType fieldType, ZoneId sessionTimeZone) {
        if (value == null) {
            return "NULL";
        }
        return fieldType.accept(new ToStringLogicalTypeVisitor(value, sessionTimeZone));
    }

    private static class ToStringLogicalTypeVisitor extends LogicalTypeDefaultVisitor<String> {

        private final Object value;
        private final ZoneId sessionTimeZone;

        public ToStringLogicalTypeVisitor(Object value, ZoneId sessionTimeZone) {
            this.value = value;
            this.sessionTimeZone = sessionTimeZone;
        }

        // --- Date and time types

        @Override
        public String visit(DateType dateType) {
            return toInternalDate(value)
                    .map(DateTimeUtils::unixDateToString)
                    .orElseGet(() -> Objects.toString(value));
        }

        private static Optional<Integer> toInternalDate(Object field) {
            if (field instanceof Integer) {
                return Optional.of((int) field);
            } else if (field instanceof java.sql.Date) {
                return Optional.of(DateTimeUtils.dateToInternal((Date) field));
            } else if (field instanceof java.time.LocalDate) {
                return Optional.of(DateTimeUtils.localDateToUnixDate((LocalDate) field));
            }

            return Optional.empty();
        }

        @Override
        public String visit(TimeType timeType) {
            return toInternalTime(value)
                    .map(time -> DateTimeUtils.unixTimeToString(time, timeType.getPrecision()))
                    .orElseGet(() -> Objects.toString(value));
        }

        private static Optional<Integer> toInternalTime(Object field) {
            if (field instanceof Integer) {
                return Optional.of((int) field);
            } else if (field instanceof Long) {
                return Optional.of(((Long) field).intValue());
            } else if (field instanceof Time) {
                return Optional.of(DateTimeUtils.timeToInternal((Time) field));
            } else if (field instanceof LocalTime) {
                return Optional.of(DateTimeUtils.localTimeToUnixDate((LocalTime) field));
            }

            return Optional.empty();
        }

        @Override
        public String visit(TimestampType timestampType) {
            return toInternalTimestamp(value)
                    .map(
                            timestampData ->
                                    DateTimeUtils.timestampToString(
                                            timestampData,
                                            TimeZone.getTimeZone(sessionTimeZone),
                                            timestampType.getPrecision()))
                    .orElseGet(() -> Objects.toString(value));
        }

        private static Optional<TimestampData> toInternalTimestamp(Object field) {
            if (field instanceof Timestamp) {
                return Optional.of(TimestampData.fromTimestamp((Timestamp) field));
            } else if (field instanceof LocalDateTime) {
                return Optional.of(TimestampData.fromLocalDateTime((LocalDateTime) field));
            } else if (field instanceof TimestampData) {
                return Optional.of((TimestampData) field);
            }

            return Optional.empty();
        }

        @Override
        public String visit(LocalZonedTimestampType localZonedTimestampType) {
            return toInternalLocalZonedTimestampType(value)
                    .map(
                            timestampData ->
                                    DateTimeUtils.timestampToString(
                                            timestampData,
                                            TimeZone.getTimeZone(sessionTimeZone),
                                            localZonedTimestampType.getPrecision()))
                    .orElseGet(() -> Objects.toString(value));
        }

        private static Optional<TimestampData> toInternalLocalZonedTimestampType(Object field) {
            if (field instanceof Instant) {
                return Optional.of(TimestampData.fromInstant((Instant) field));
            } else if (field instanceof Timestamp) {
                Timestamp timestamp = (Timestamp) field;
                // conversion between java.sql.Timestamp and TIMESTAMP_WITH_LOCAL_TIME_ZONE
                return Optional.of(
                        TimestampData.fromEpochMillis(
                                timestamp.getTime(), timestamp.getNanos() % 1000_000));
            } else if (field instanceof TimestampData) {
                return Optional.of((TimestampData) field);
            } else if (field instanceof Integer) {
                return Optional.of(
                        TimestampData.fromEpochMillis(((Integer) field).longValue() * 1000));
            } else if (field instanceof Long) {
                return Optional.of(TimestampData.fromEpochMillis((Long) field));
            }

            return Optional.empty();
        }

        @Override
        public String visit(YearMonthIntervalType yearMonthIntervalType) {
            return toInternalYeahMonthInterval(value)
                    .map(DateTimeUtils::intervalYearMonthToString)
                    .orElseGet(() -> Objects.toString(value));
        }

        private static Optional<Integer> toInternalYeahMonthInterval(Object field) {
            if (field instanceof Period) {
                return Optional.of((int) ((Period) field).toTotalMonths());
            } else if (field instanceof Integer) {
                return Optional.of((int) field);
            }

            return Optional.empty();
        }

        @Override
        public String visit(DayTimeIntervalType dayTimeIntervalType) {
            return toInternalDayTimeInterval(value)
                    .map(DateTimeUtils::intervalDayTimeToString)
                    .orElseGet(() -> Objects.toString(value));
        }

        private static Optional<Long> toInternalDayTimeInterval(Object field) {
            if (field instanceof Duration) {
                return Optional.of(((Duration) field).toMillis());
            } else if (field instanceof Long) {
                return Optional.of((long) field);
            } else if (field instanceof Integer) {
                return Optional.of(((Integer) field).longValue());
            }

            return Optional.empty();
        }

        // --- Composite types

        @Override
        public String visit(ArrayType arrayType) {
            LogicalType elementType = arrayType.getElementType();
            if (value instanceof List) {
                List<?> array = (List<?>) value;
                String[] formattedArray = new String[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    formattedArray[i] = toSQLString(array.get(i), elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == byte[].class) {
                byte[] array = (byte[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == short[].class) {
                short[] array = (short[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == int[].class) {
                int[] array = (int[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == long[].class) {
                long[] array = (long[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == float[].class) {
                float[] array = (float[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == double[].class) {
                double[] array = (double[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == boolean[].class) {
                boolean[] array = (boolean[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass() == char[].class) {
                char[] array = (char[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            } else if (value.getClass().isArray()) {
                // non-primitive type
                Object[] array = (Object[]) value;
                String[] formattedArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    formattedArray[i] = toSQLString(array[i], elementType, sessionTimeZone);
                }
                return Arrays.toString(formattedArray);
            }
            return Objects.toString(value);
        }

        @Override
        public String visit(RowType rowType) {
            if (value instanceof Row) {
                Row row = (Row) value;
                String[] formattedArray = new String[row.getArity()];
                for (int i = 0; i < rowType.getFields().size(); i++) {
                    LogicalType type = rowType.getFields().get(i).getType();
                    formattedArray[i] = toSQLString(row.getField(i), type, sessionTimeZone);
                }
                return "(" + String.join(", ", formattedArray) + ")";
            } else if (value instanceof RowData) {
                RowData rowData = (RowData) value;
                String[] formattedArray = new String[rowData.getArity()];
                for (int i = 0; i < rowType.getFields().size(); i++) {
                    LogicalType type = rowType.getFields().get(i).getType();
                    formattedArray[i] =
                            toSQLString(
                                    RowData.createFieldGetter(type, i).getFieldOrNull(rowData),
                                    type,
                                    sessionTimeZone);
                }
                return "(" + String.join(", ", formattedArray) + ")";
            }

            return Objects.toString(value);
        }

        @Override
        public String visit(MapType mapType) {
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();
            if (value instanceof Map) {
                return "{"
                        + ((Map<?, ?>) value)
                                .entrySet().stream()
                                        .map(
                                                entry ->
                                                        toSQLString(
                                                                        entry.getKey(),
                                                                        keyType,
                                                                        sessionTimeZone)
                                                                + "="
                                                                + toSQLString(
                                                                        entry.getValue(),
                                                                        valueType,
                                                                        sessionTimeZone))
                                        .collect(Collectors.joining(", "))
                        + "}";
            } else if (value instanceof MapData) {
                MapData mapData = (MapData) value;
                return "{"
                        + IntStream.range(0, mapData.size())
                                .mapToObj(
                                        i ->
                                                toSQLString(
                                                                ArrayData.createElementGetter(
                                                                                keyType)
                                                                        .getElementOrNull(
                                                                                mapData.keyArray(),
                                                                                i),
                                                                keyType,
                                                                sessionTimeZone)
                                                        + "="
                                                        + toSQLString(
                                                                ArrayData.createElementGetter(
                                                                                valueType)
                                                                        .getElementOrNull(
                                                                                mapData
                                                                                        .valueArray(),
                                                                                i),
                                                                valueType,
                                                                sessionTimeZone))
                                .collect(Collectors.joining(", "))
                        + "}";
            }

            return Objects.toString(value);
        }

        // --- Atomic types

        @Override
        public String visit(BooleanType booleanType) {
            if (value instanceof Boolean) {
                if (((boolean) value)) {
                    return "TRUE";
                }
                return "FALSE";
            }
            return Objects.toString(value);
        }

        @Override
        public String visit(DecimalType decimalType) {
            if (value instanceof Double) {
                return DecimalDataUtils.castFrom(
                                (Double) value, decimalType.getPrecision(), decimalType.getScale())
                        .toString();
            } else if (value instanceof DecimalData) {
                return ((DecimalData) value).toString();
            } else if (value instanceof BigDecimal) {
                return DecimalData.fromBigDecimal(
                                (BigDecimal) value,
                                decimalType.getPrecision(),
                                decimalType.getScale())
                        .toString();
            }
            return Objects.toString(value);
        }

        @Override
        public String visit(BinaryType binaryType) {
            if (value.getClass() == byte[].class) {
                return BinaryStringData.fromString(
                                new String(
                                        Arrays.copyOf((byte[]) value, binaryType.getLength()),
                                        StandardCharsets.UTF_8))
                        .toString();
            }
            return Objects.toString(value);
        }

        @Override
        public String visit(VarBinaryType varBinaryType) {
            if (value.getClass() == byte[].class) {
                return BinaryStringData.fromString(
                                new String((byte[]) value, StandardCharsets.UTF_8))
                        .toString();
            }
            return Objects.toString(value);
        }

        @Override
        protected String defaultMethod(LogicalType logicalType) {
            return Objects.toString(value);
        }
    }

    private StringUtils() {}
}
