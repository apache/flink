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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIME_FORMAT;

/** Tool class used to convert fields from {@link JsonParser} to {@link RowData}. */
@Internal
public class JsonParserToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    public JsonParserToRowDataConverters(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    /**
     * Runtime converter that converts {@link JsonParser}s into objects of Flink Table & SQL
     * internal data structures. Unlike {@link JsonToRowDataConverters.JsonToRowDataConverter}, this
     * interface also supports projection pushdown of nested fields.
     */
    @FunctionalInterface
    public interface JsonParserToRowDataConverter extends Serializable {
        Object convert(JsonParser jsonParser) throws IOException;
    }

    /** Creates a runtime nested converter which is null safe. */
    public JsonParserToRowDataConverter createConverter(
            String[][] projectedFields, RowType rowType) {
        // If projectedFields is null or doesn't contain nested fields, fallback to origin way
        if (projectedFields == null
                || Arrays.stream(projectedFields).allMatch(arr -> arr.length == 1)) {
            return createConverter(rowType);
        }

        RowNestedConverter rowConverter = new RowNestedConverter();
        for (int i = 0; i < projectedFields.length; i++) {
            addFieldConverter(
                    rowConverter.fieldConverters, projectedFields[i], 0, i, rowType.getTypeAt(i));
        }

        // DO NOT USE Lambda,it has shade problem.
        return new JsonParserToRowDataConverter() {

            @Override
            public Object convert(JsonParser jp) throws IOException {
                GenericRowData row = new GenericRowData(rowType.getFieldCount());
                rowConverter.convert(jp, row);
                return row;
            }
        };
    }

    /** Creates a runtime converter which is null safe. */
    private JsonParserToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private JsonParserToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return jsonNode -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return this::convertToByte;
            case SMALLINT:
                return this::convertToShort;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToTimestampWithLocalZone;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return JsonParser::getBinaryValue;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(
                        mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return createMapConverter(
                        multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private boolean convertToBoolean(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.VALUE_TRUE) {
            return true;
        } else if (jp.currentToken() == JsonToken.VALUE_FALSE) {
            return false;
        } else {
            return Boolean.parseBoolean(jp.getText().trim());
        }
    }

    private byte convertToByte(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.VALUE_NUMBER_INT) {
            // DON'T use jp.getByteValue() whose value is from -128 to 255 because of the unsigned
            // value.
            int value = jp.getIntValue();
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw new JsonParseException(
                        String.format("Numeric value (%s) out of range of Java byte.", value));
            }
            return (byte) value;
        } else {
            return Byte.parseByte(jp.getText().trim());
        }
    }

    private short convertToShort(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.VALUE_NUMBER_INT) {
            return jp.getShortValue();
        } else {
            return Short.parseShort(jp.getText().trim());
        }
    }

    private int convertToInt(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.VALUE_NUMBER_INT
                || jp.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
            return jp.getIntValue();
        } else {
            return Integer.parseInt(jp.getText().trim());
        }
    }

    private long convertToLong(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.VALUE_NUMBER_INT
                || jp.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
            return jp.getLongValue();
        } else {
            return Long.parseLong(jp.getText().trim());
        }
    }

    private double convertToDouble(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
            return jp.getDoubleValue();
        } else {
            return Double.parseDouble(jp.getText().trim());
        }
    }

    private float convertToFloat(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
            return jp.getFloatValue();
        } else {
            return Float.parseFloat(jp.getText().trim());
        }
    }

    private int convertToDate(JsonParser jp) throws IOException {
        LocalDate date = ISO_LOCAL_DATE.parse(jp.getText()).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private int convertToTime(JsonParser jsonNode) throws IOException {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(jsonNode.getText());
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        // get number of milliseconds of the day
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(JsonParser jp) throws IOException {
        TemporalAccessor parsedTimestamp;
        switch (timestampFormat) {
            case SQL:
                parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(jp.getText());
                break;
            case ISO_8601:
                parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(jp.getText());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private TimestampData convertToTimestampWithLocalZone(JsonParser jp) throws IOException {
        TemporalAccessor parsedTimestampWithLocalZone;
        switch (timestampFormat) {
            case SQL:
                parsedTimestampWithLocalZone =
                        SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jp.getText());
                break;
            case ISO_8601:
                parsedTimestampWithLocalZone =
                        ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jp.getText());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

        return TimestampData.fromInstant(
                LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
    }

    private StringData convertToString(JsonParser jp) throws IOException {
        if (jp.currentToken() == JsonToken.START_OBJECT
                || jp.currentToken() == JsonToken.START_ARRAY) {
            return StringData.fromString(jp.readValueAsTree().toString());
        } else {
            return StringData.fromString(jp.getText());
        }
    }

    private JsonParserToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return jp -> {
            BigDecimal bigDecimal;
            if (jp.currentToken() == JsonToken.VALUE_STRING) {
                bigDecimal = new BigDecimal(jp.getText().trim());
            } else {
                bigDecimal = jp.getDecimalValue();
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private JsonParserToRowDataConverter createArrayConverter(ArrayType arrayType) {
        JsonParserToRowDataConverter elementConverter = createConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return jp -> {
            if (jp.currentToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Illegal JSON array data...");
            }
            List<Object> result = new ArrayList<>();
            while (jp.nextToken() != JsonToken.END_ARRAY) {
                Object convertField = elementConverter.convert(jp);
                result.add(convertField);
            }
            final Object[] array = (Object[]) Array.newInstance(elementClass, result.size());
            return new GenericArrayData(result.toArray(array));
        };
    }

    private JsonParserToRowDataConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final JsonParserToRowDataConverter keyConverter = createConverter(keyType);
        final JsonParserToRowDataConverter valueConverter = createConverter(valueType);

        return jp -> {
            if (jp.currentToken() != JsonToken.START_OBJECT) {
                throw new IllegalStateException("Illegal JSON map data...");
            }
            Map<Object, Object> result = new HashMap<>();
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                Object key = keyConverter.convert(jp);
                jp.nextToken();
                Object value = valueConverter.convert(jp);
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    public JsonParserToRowDataConverter createRowConverter(RowType rowType) {
        final JsonParserToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createConverter)
                        .toArray(JsonParserToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        Map<String, Integer> nameIdxMap = new HashMap<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            nameIdxMap.put(fieldNames[i], i);
        }

        return jp -> {
            if (jp.currentToken() != JsonToken.START_OBJECT) {
                throw new IllegalStateException("Illegal JSON object data...");
            }
            int arity = nameIdxMap.size();
            GenericRowData row = new GenericRowData(arity);
            int cnt = 0;
            jp.nextToken();
            while (jp.currentToken() != JsonToken.END_OBJECT) {
                if (cnt >= arity) {
                    skipToNextField(jp);
                    continue;
                }
                String fieldName = jp.getText();
                jp.nextToken();
                Integer idx = nameIdxMap.get(fieldName);
                if (idx != null) {
                    try {
                        Object convertField = fieldConverters[idx].convert(jp);
                        row.setField(idx, convertField);
                    } catch (Throwable t) {
                        throw new JsonParseException(
                                String.format("Fail to deserialize at field: %s.", fieldName));
                    }
                    jp.nextToken();
                    cnt++;
                } else {
                    skipToNextField(jp);
                }
            }
            if (cnt < arity && failOnMissingField) {
                throw new JsonParseException("Some field is missing in the JSON data.");
            }
            return row;
        };
    }

    public static void skipToNextField(JsonParser jp) throws IOException {
        switch (jp.currentToken()) {
            case START_OBJECT:
            case START_ARRAY:
                int match = 1;
                JsonToken token;
                while (match > 0) {
                    token = jp.nextToken();
                    if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
                        match--;
                    } else if (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT) {
                        match++;
                    }
                }
                break;
            default:
        }
        jp.nextToken();
    }

    private JsonParserToRowDataConverter wrapIntoNullableConverter(
            JsonParserToRowDataConverter converter) {
        return jp -> {
            if (jp == null
                    || jp.currentToken() == null
                    || jp.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            try {
                return converter.convert(jp);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    private void addFieldConverter(
            Map<String, ProjectedConverter> converters,
            String[] nestedField,
            int depth,
            int outputPos,
            LogicalType type) {
        String name = nestedField[depth];
        if (depth == nestedField.length - 1) {
            FieldConverter fieldConverter = new FieldConverter(outputPos, type);
            ProjectedConverter converter = converters.get(name);
            if (converter instanceof RowNestedConverter) {
                // Has RowNestedConverter, fallback to get nested field from a converted row field.
                for (Map.Entry<Integer, String[]> entry :
                        ((RowNestedConverter) converter).outputPosToPath.entrySet()) {
                    fieldConverter.addNestedFieldPath(entry.getKey(), entry.getValue());
                }
            } else if (converter instanceof FieldConverter) {
                throw new RuntimeException("This is a bug, contains duplicated fields.");
            }
            converters.put(name, fieldConverter);
        } else {
            ProjectedConverter converter =
                    converters.computeIfAbsent(name, k -> new RowNestedConverter());
            String[] namePath = ArrayUtils.subarray(nestedField, depth + 1, nestedField.length);
            if (converter instanceof FieldConverter) {
                // Already converted this row field, just get nested field from it.
                ((FieldConverter) converter).addNestedFieldPath(outputPos, namePath);
            } else {
                RowNestedConverter rowConverter = (RowNestedConverter) converter;
                rowConverter.outputPosToPath.put(outputPos, namePath);
                addFieldConverter(
                        rowConverter.fieldConverters, nestedField, depth + 1, outputPos, type);
            }
        }
    }

    /**
     * Runtime converter that converts {@link JsonParser}s into objects of Flink Table & SQL
     * internal data structures.
     */
    private abstract static class ProjectedConverter implements Serializable {

        private static final long serialVersionUID = 1L;

        public final void convert(JsonParser jp, GenericRowData outputRow) throws IOException {
            if (jp != null
                    && jp.currentToken() != null
                    && jp.getCurrentToken() != JsonToken.VALUE_NULL) {
                convertNotNull(jp, outputRow);
            }
        }

        public abstract void convertNotNull(JsonParser jp, GenericRowData outputRow)
                throws IOException;
    }

    private class FieldConverter extends ProjectedConverter {

        private static final long serialVersionUID = 1L;

        private final int outputPos;

        private final JsonParserToRowDataConverter converter;

        private final LogicalType type;

        private final Map<Integer, int[]> outputPosToPath = new HashMap<>();

        public FieldConverter(int outputPos, LogicalType type) {
            this.outputPos = outputPos;
            this.converter = createConverter(type);
            this.type = type;
        }

        @Override
        public void convertNotNull(JsonParser jp, GenericRowData outputRow) throws IOException {
            Object field = converter.convert(jp);
            outputRow.setField(outputPos, field);
            if (field != null && !outputPosToPath.isEmpty()) {
                outNestedFields(field, outputRow);
            }
        }

        private void outNestedFields(Object field, GenericRowData outputRow) {
            outputLoop:
            for (Map.Entry<Integer, int[]> entry : outputPosToPath.entrySet()) {
                Object currentField = field;
                for (int i : entry.getValue()) {
                    if (currentField == null) {
                        continue outputLoop;
                    }
                    currentField = ((GenericRowData) currentField).getField(i);
                }
                outputRow.setField(entry.getKey(), currentField);
            }
        }

        public void addNestedFieldPath(int pos, String[] namePath) {
            int[] path = new int[namePath.length];
            LogicalType currentType = type;
            for (int i = 0; i < path.length; i++) {
                if (!(currentType instanceof RowType)) {
                    throw new RuntimeException(
                            "This is a bug, currentType should be row type, but is: "
                                    + currentType);
                }

                int fieldIndex = ((RowType) currentType).getFieldNames().indexOf(namePath[i]);
                currentType = currentType.getChildren().get(fieldIndex);
                path[i] = fieldIndex;
            }
            outputPosToPath.put(pos, path);
        }
    }

    private class RowNestedConverter extends ProjectedConverter {

        private static final long serialVersionUID = 1L;

        private final Map<String, ProjectedConverter> fieldConverters = new HashMap<>();

        // Keep path here for fallback to get nested field from a converted row field.
        private final Map<Integer, String[]> outputPosToPath = new HashMap<>();

        @Override
        public void convertNotNull(JsonParser jp, GenericRowData outputRow) throws IOException {
            if (jp.currentToken() != JsonToken.START_OBJECT) {
                throw new IllegalStateException("Illegal Json Data...");
            }
            int arity = fieldConverters.size();
            int cnt = 0;
            jp.nextToken();
            while (jp.currentToken() != JsonToken.END_OBJECT) {
                if (cnt >= arity) {
                    skipToNextField(jp);
                    continue;
                }
                String fieldName = jp.getText();
                jp.nextToken();
                ProjectedConverter converter = fieldConverters.get(fieldName);
                if (converter != null) {
                    converter.convert(jp, outputRow);
                    jp.nextToken();
                    cnt++;
                } else {
                    skipToNextField(jp);
                }
            }
            if (cnt < arity && failOnMissingField) {
                throw new JsonParseException("Some field is missing in the Json data.");
            }
        }
    }
}
