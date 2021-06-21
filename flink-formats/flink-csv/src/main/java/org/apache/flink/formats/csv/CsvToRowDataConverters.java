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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;

/** Tool class used to convert from CSV-format {@link JsonNode} to {@link RowData}. * */
@Internal
public class CsvToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    public CsvToRowDataConverters(boolean ignoreParseErrors) {
        this.ignoreParseErrors = ignoreParseErrors;
    }

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL internal
     * data structures.
     */
    @FunctionalInterface
    public interface CsvToRowDataConverter extends Serializable {
        Object convert(JsonNode jsonNode);
    }

    public CsvToRowDataConverter createRowConverter(RowType rowType, boolean isTopLevel) {
        final CsvToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createNullableConverter)
                        .toArray(CsvToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        final int arity = fieldNames.length;

        return jsonNode -> {
            int nodeSize = jsonNode.size();

            if (nodeSize != 0) {
                validateArity(arity, nodeSize, ignoreParseErrors);
            } else {
                return null;
            }

            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                JsonNode field;
                // Jackson only supports mapping by name in the first level
                if (isTopLevel) {
                    field = jsonNode.get(fieldNames[i]);
                } else {
                    field = jsonNode.get(i);
                }
                if (field == null) {
                    row.setField(i, null);
                } else {
                    row.setField(i, fieldConverters[i].convert(field));
                }
            }
            return row;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private CsvToRowDataConverter createNullableConverter(LogicalType type) {
        final CsvToRowDataConverter converter = createConverter(type);
        return jsonNode -> {
            if (jsonNode == null || jsonNode.isNull()) {
                return null;
            }
            try {
                return converter.convert(jsonNode);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private CsvToRowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return jsonNode -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
            case SMALLINT:
                return jsonNode -> Short.parseShort(jsonNode.asText().trim());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return convertToTime((TimeType) type);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return jsonNode -> convertToTimestamp(jsonNode, SQL_TIMESTAMP_FORMAT);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return jsonNode ->
                        convertToTimestamp(jsonNode, SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT);
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case ROW:
                return createRowConverter((RowType) type, false);
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private boolean convertToBoolean(JsonNode jsonNode) {
        if (jsonNode.isBoolean()) {
            // avoid redundant toString and parseBoolean, for better performance
            return jsonNode.asBoolean();
        } else {
            return Boolean.parseBoolean(jsonNode.asText().trim());
        }
    }

    private int convertToInt(JsonNode jsonNode) {
        if (jsonNode.canConvertToInt()) {
            // avoid redundant toString and parseInt, for better performance
            return jsonNode.asInt();
        } else {
            return Integer.parseInt(jsonNode.asText().trim());
        }
    }

    private long convertToLong(JsonNode jsonNode) {
        if (jsonNode.canConvertToLong()) {
            // avoid redundant toString and parseLong, for better performance
            return jsonNode.asLong();
        } else {
            return Long.parseLong(jsonNode.asText().trim());
        }
    }

    private double convertToDouble(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return jsonNode.asDouble();
        } else {
            return Double.parseDouble(jsonNode.asText().trim());
        }
    }

    private float convertToFloat(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return (float) jsonNode.asDouble();
        } else {
            return Float.parseFloat(jsonNode.asText().trim());
        }
    }

    private int convertToDate(JsonNode jsonNode) {
        // csv currently is using Date.valueOf() to parse date string
        return (int) Date.valueOf(jsonNode.asText()).toLocalDate().toEpochDay();
    }

    private CsvToRowDataConverter convertToTime(TimeType timeType) {
        final int precision = timeType.getPrecision();
        // csv currently is using Time.valueOf() to parse time string
        // TODO: FLINK-17525 support millisecond and nanosecond
        // get number of milliseconds of the day
        if (precision > 3) {
            throw new IllegalArgumentException(
                    "Csv does not support TIME type "
                            + "with precision: "
                            + precision
                            + ", it only supports precision 0 ~ 3.");
        }
        return jsonNode -> {
            LocalTime localTime = LocalTime.parse(jsonNode.asText());
            int mills = (int) (localTime.toNanoOfDay() / 1000_000L);
            // this is for rounding off values out of precision
            if (precision == 2) {
                mills = mills / 10 * 10;
            } else if (precision == 1) {
                mills = mills / 100 * 100;
            } else if (precision == 0) {
                mills = mills / 1000 * 1000;
            }
            return mills;
        };
    }

    private TimestampData convertToTimestamp(
            JsonNode jsonNode, DateTimeFormatter dateTimeFormatter) {
        return TimestampData.fromLocalDateTime(
                LocalDateTime.parse(jsonNode.asText().trim(), dateTimeFormatter));
    }

    private StringData convertToString(JsonNode jsonNode) {
        return StringData.fromString(jsonNode.asText());
    }

    private byte[] convertToBytes(JsonNode jsonNode) {
        try {
            return jsonNode.binaryValue();
        } catch (IOException e) {
            throw new CsvParseException("Unable to deserialize byte array.", e);
        }
    }

    private CsvToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return jsonNode -> {
            BigDecimal bigDecimal;
            if (jsonNode.isBigDecimal()) {
                bigDecimal = jsonNode.decimalValue();
            } else {
                bigDecimal = new BigDecimal(jsonNode.asText());
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private CsvToRowDataConverter createArrayConverter(ArrayType arrayType) {
        final CsvToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return jsonNode -> {
            final ArrayNode node = (ArrayNode) jsonNode;
            final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
            for (int i = 0; i < node.size(); i++) {
                final JsonNode innerNode = node.get(i);
                array[i] = elementConverter.convert(innerNode);
            }
            return new GenericArrayData(array);
        };
    }

    private static void validateArity(int expected, int actual, boolean ignoreParseErrors) {
        if (expected != actual && !ignoreParseErrors) {
            throw new RuntimeException(
                    "Row length mismatch. "
                            + expected
                            + " fields expected but was "
                            + actual
                            + ".");
        }
    }

    /** Exception which refers to parse errors in converters. */
    private static final class CsvParseException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public CsvParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
