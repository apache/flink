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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIME_FORMAT;

/** Tool class used to convert from {@link RowData} to {@link JsonNode}. * */
@Internal
public class RowDataToJsonConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /** The handling mode when serializing null keys for map data. */
    private final JsonOptions.MapNullKeyMode mapNullKeyMode;

    /** The string literal when handling mode for map null key LITERAL. is */
    private final String mapNullKeyLiteral;

    public RowDataToJsonConverters(
            TimestampFormat timestampFormat,
            JsonOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
    }

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding {@link JsonNode}s.
     */
    public interface RowDataToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
    }

    /** Creates a runtime converter which is null safe. */
    public RowDataToJsonConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private RowDataToJsonConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (mapper, reuse, value) -> mapper.getNodeFactory().nullNode();
            case BOOLEAN:
                return (mapper, reuse, value) ->
                        mapper.getNodeFactory().booleanNode((boolean) value);
            case TINYINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((byte) value);
            case SMALLINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((short) value);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((int) value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((long) value);
            case FLOAT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((float) value);
            case DOUBLE:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((double) value);
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (mapper, reuse, value) -> mapper.getNodeFactory().textNode(value.toString());
            case BINARY:
            case VARBINARY:
                return (mapper, reuse, value) -> mapper.getNodeFactory().binaryNode((byte[]) value);
            case DATE:
                return createDateConverter();
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return createTimestampWithLocalZone();
            case DECIMAL:
                return createDecimalConverter();
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
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToJsonConverter createDecimalConverter() {
        return (mapper, reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return mapper.getNodeFactory().numberNode(bd);
        };
    }

    private RowDataToJsonConverter createDateConverter() {
        return (mapper, reuse, value) -> {
            int days = (int) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format(date));
        };
    }

    private RowDataToJsonConverter createTimeConverter() {
        return (mapper, reuse, value) -> {
            int millisecond = (int) value;
            LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
            return mapper.getNodeFactory().textNode(SQL_TIME_FORMAT.format(time));
        };
    }

    private RowDataToJsonConverter createTimestampConverter() {
        switch (timestampFormat) {
            case ISO_8601:
                return (mapper, reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return mapper.getNodeFactory()
                            .textNode(ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            case SQL:
                return (mapper, reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return mapper.getNodeFactory()
                            .textNode(SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToJsonConverter createTimestampWithLocalZone() {
        switch (timestampFormat) {
            case ISO_8601:
                return (mapper, reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData) value;
                    return mapper.getNodeFactory()
                            .textNode(
                                    ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                            timestampWithLocalZone
                                                    .toInstant()
                                                    .atOffset(ZoneOffset.UTC)));
                };
            case SQL:
                return (mapper, reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData) value;
                    return mapper.getNodeFactory()
                            .textNode(
                                    SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                            timestampWithLocalZone
                                                    .toInstant()
                                                    .atOffset(ZoneOffset.UTC)));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToJsonConverter createArrayConverter(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final RowDataToJsonConverter elementConverter = createConverter(elementType);
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return (mapper, reuse, value) -> {
            ArrayNode node;

            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createArrayNode();
            } else {
                node = (ArrayNode) reuse;
                node.removeAll();
            }

            ArrayData array = (ArrayData) value;
            int numElements = array.size();
            for (int i = 0; i < numElements; i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                node.add(elementConverter.convert(mapper, null, element));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final RowDataToJsonConverter valueConverter = createConverter(valueType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return (mapper, reuse, object) -> {
            ObjectNode node;
            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
                node.removeAll();
            }

            MapData map = (MapData) object;
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();
            int numElements = map.size();
            for (int i = 0; i < numElements; i++) {
                String fieldName = null;
                if (keyArray.isNullAt(i)) {
                    // when map key is null
                    switch (mapNullKeyMode) {
                        case LITERAL:
                            fieldName = mapNullKeyLiteral;
                            break;
                        case DROP:
                            continue;
                        case FAIL:
                            throw new RuntimeException(
                                    String.format(
                                            "JSON format doesn't support to serialize map data with null keys. "
                                                    + "You can drop null key entries or encode null in literals by specifying %s option.",
                                            JsonOptions.MAP_NULL_KEY_MODE.key()));
                        default:
                            throw new RuntimeException(
                                    "Unsupported map null key mode. Validator should have checked that.");
                    }
                } else {
                    fieldName = keyArray.getString(i).toString();
                }

                Object value = valueGetter.getElementOrNull(valueArray, i);
                node.set(fieldName, valueConverter.convert(mapper, node.get(fieldName), value));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createRowConverter(RowType type) {
        final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowDataToJsonConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(RowDataToJsonConverter[]::new);
        final int fieldCount = type.getFieldCount();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return (mapper, reuse, value) -> {
            ObjectNode node;
            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
            }
            RowData row = (RowData) value;
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                Object field = fieldGetters[i].getFieldOrNull(row);
                node.set(fieldName, fieldConverters[i].convert(mapper, node.get(fieldName), field));
            }
            return node;
        };
    }

    private RowDataToJsonConverter wrapIntoNullableConverter(RowDataToJsonConverter converter) {
        return (mapper, reuse, object) -> {
            if (object == null) {
                return mapper.getNodeFactory().nullNode();
            }

            return converter.convert(mapper, reuse, object);
        };
    }
}
