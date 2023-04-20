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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_CLASS;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_ELEMENT_TYPE;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELDS;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELD_NAME;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELD_TYPE;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_FILED_DESCRIPTION;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_FRACTIONAL_PRECISION;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_KEY_TYPE;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_LENGTH;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_NULLABLE;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_PRECISION;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_RESOLUTION;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_SCALE;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_SERIALIZER;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_TYPE_NAME;
import static org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer.FIELD_NAME_VALUE_TYPE;

/**
 * JSON deserializer for {@link LogicalType}.
 *
 * @see LogicalTypeJsonSerializer for the reverse operation.
 */
@Internal
public final class LogicalTypeJsonDeserializer extends StdDeserializer<LogicalType> {

    private static final long serialVersionUID = 1L;

    LogicalTypeJsonDeserializer() {
        super(LogicalType.class);
    }

    @Override
    public LogicalType deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        JsonNode logicalTypeNode = jsonParser.readValueAsTree();
        if (logicalTypeNode.has(FIELD_NAME_TYPE_NAME)) {
            return deserializeInternal(logicalTypeNode);
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Cannot parse this Json String:\n%s", logicalTypeNode.toPrettyString()));
    }

    /**
     * Deserialize json according to the original type root. It's reverse operation of {@code
     * SerializerWIP#serializeinternal}.
     */
    private LogicalType deserializeInternal(JsonNode logicalTypeNode) {
        LogicalTypeRoot typeRoot =
                LogicalTypeRoot.valueOf(logicalTypeNode.get(FIELD_NAME_TYPE_NAME).asText());
        // the NullType's Json doesn't have other field, so return in advance
        if (typeRoot.equals(LogicalTypeRoot.NULL)) {
            return new NullType();
        }
        boolean isNullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        switch (typeRoot) {
            case BOOLEAN:
                return new BooleanType(isNullable);
            case TINYINT:
                return new TinyIntType(isNullable);
            case SMALLINT:
                return new SmallIntType(isNullable);
            case INTEGER:
                return new IntType(isNullable);
            case BIGINT:
                return new BigIntType(isNullable);
            case FLOAT:
                return new FloatType(isNullable);
            case DOUBLE:
                return new DoubleType(isNullable);
            case DATE:
                return new DateType(isNullable);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return deserializeLengthFieldType(typeRoot, logicalTypeNode).copy(isNullable);
            case DECIMAL:
                return new DecimalType(
                        isNullable,
                        logicalTypeNode.get(FIELD_NAME_PRECISION).asInt(),
                        logicalTypeNode.get(FIELD_NAME_SCALE).asInt());
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return deserializeTimestamp(typeRoot, logicalTypeNode).copy(isNullable);
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
                return deserializeInterval(isNullable, typeRoot, logicalTypeNode);
            case MAP:
                return deserializeMap(logicalTypeNode).copy(isNullable);
            case ARRAY:
            case MULTISET:
                return deserializeCollection(typeRoot, logicalTypeNode).copy(isNullable);
            case ROW:
                return deserializeRow(logicalTypeNode).copy(isNullable);
            case RAW:
                return deserializeRaw(logicalTypeNode).copy(isNullable);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unable to deserialize a logical type of type root '%s'. Please check the documentation for supported types.",
                                typeRoot.name()));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods for some complex types
    // --------------------------------------------------------------------------------------------

    private LogicalType deserializeLengthFieldType(
            LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        int length = logicalTypeNode.get(FIELD_NAME_LENGTH).asInt();
        switch (typeRoot) {
            case CHAR:
                return length == 0 ? CharType.ofEmptyLiteral() : new CharType(length);
            case VARCHAR:
                return length == 0 ? VarCharType.ofEmptyLiteral() : new VarCharType(length);
            case BINARY:
                return length == 0 ? BinaryType.ofEmptyLiteral() : new BinaryType(length);
            case VARBINARY:
                return length == 0 ? VarBinaryType.ofEmptyLiteral() : new VarBinaryType(length);
            default:
                throw new SqlGatewayException(
                        String.format(
                                "Cannot convert JSON string '%s' to the logical type '%s', '%s', '%s' or '%s'.",
                                logicalTypeNode.toPrettyString(),
                                LogicalTypeRoot.CHAR.name(),
                                LogicalTypeRoot.VARCHAR.name(),
                                LogicalTypeRoot.BINARY.name(),
                                LogicalTypeRoot.VARBINARY.name()));
        }
    }

    private LogicalType deserializeTimestamp(LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        int precision = logicalTypeNode.get(FIELD_NAME_PRECISION).asInt();
        switch (typeRoot) {
            case TIME_WITHOUT_TIME_ZONE:
                return new TimeType(precision);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampType(precision);
            case TIMESTAMP_WITH_TIME_ZONE:
                return new ZonedTimestampType(precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(precision);
            default:
                throw new TableException("Timestamp type root expected.");
        }
    }

    private LogicalType deserializeInterval(
            boolean isNullable, LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        int precision = logicalTypeNode.get(FIELD_NAME_PRECISION).asInt();
        switch (typeRoot) {
            case INTERVAL_YEAR_MONTH:
                return new YearMonthIntervalType(
                        isNullable,
                        YearMonthIntervalType.YearMonthResolution.valueOf(
                                logicalTypeNode.get(FIELD_NAME_RESOLUTION).asText()),
                        precision);
            case INTERVAL_DAY_TIME:
                return new DayTimeIntervalType(
                        isNullable,
                        DayTimeIntervalType.DayTimeResolution.valueOf(
                                logicalTypeNode.get(FIELD_NAME_RESOLUTION).asText()),
                        precision,
                        logicalTypeNode.get(FIELD_NAME_FRACTIONAL_PRECISION).asInt());
            default:
                throw new TableException("Interval type root expected.");
        }
    }

    private LogicalType deserializeMap(JsonNode logicalTypeNode) {
        JsonNode keyNode = logicalTypeNode.get(FIELD_NAME_KEY_TYPE);
        LogicalType keyType = deserializeInternal(keyNode);
        JsonNode valueNode = logicalTypeNode.get(FIELD_NAME_VALUE_TYPE);
        LogicalType valueType = deserializeInternal(valueNode);
        return new MapType(keyType, valueType);
    }

    private LogicalType deserializeCollection(LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        JsonNode elementNode = logicalTypeNode.get(FIELD_NAME_ELEMENT_TYPE);
        LogicalType elementType = deserializeInternal(elementNode);
        switch (typeRoot) {
            case ARRAY:
                return new ArrayType(elementType);
            case MULTISET:
                return new MultisetType(elementType);
            default:
                throw new TableException("Collection type root expected.");
        }
    }

    private LogicalType deserializeRow(JsonNode logicalTypeNode) {
        ArrayNode fieldNodes = (ArrayNode) logicalTypeNode.get(FIELD_NAME_FIELDS);
        List<RowField> fields = new ArrayList<>();
        fieldNodes.forEach(
                fieldNode -> {
                    String fieldName = fieldNode.get(FIELD_NAME_FIELD_NAME).asText();
                    LogicalType fieldType =
                            deserializeInternal(fieldNode.get(FIELD_NAME_FIELD_TYPE));
                    String description = null;
                    if (fieldNode.has(FIELD_NAME_FILED_DESCRIPTION)) {
                        description = fieldNode.get(FIELD_NAME_FILED_DESCRIPTION).asText();
                    }
                    fields.add(new RowField(fieldName, fieldType, description));
                });
        return new RowType(fields);
    }

    private LogicalType deserializeRaw(JsonNode logicalTypeNode) {
        String className = logicalTypeNode.get(FIELD_NAME_CLASS).asText();
        String serializer = logicalTypeNode.get(FIELD_NAME_SERIALIZER).asText();
        return RawType.restore(
                LogicalTypeJsonDeserializer.class.getClassLoader(), className, serializer);
    }
}
