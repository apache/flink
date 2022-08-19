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

package org.apache.flink.table.gateway.api.results.serde;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * Json serializer for {@link LogicalType}.
 *
 * @see LogicalTypeJsonDeserializer for the reverse operation.
 */
@PublicEvolving
public final class LogicalTypeJsonSerializer extends StdSerializer<LogicalType> {

    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------
    // Public string constants for serializer and deserializer
    // --------------------------------------------------------------------------------------------

    // Common fields
    public static final String FIELD_NAME_TYPE_NAME = "type";
    public static final String FIELD_NAME_NULLABLE = "nullable";

    // CHAR, VARCHAR, BINARY, VARBINARY
    public static final String FIELD_NAME_LENGTH = "length";

    // DECIMAL, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE
    public static final String FIELD_NAME_PRECISION = "precision";

    // DECIMAL
    public static final String FIELD_NAME_SCALE = "scale";

    // MAP
    public static final String FIELD_NAME_KEY_TYPE = "keyType";
    public static final String FIELD_NAME_VALUE_TYPE = "valueType";

    // ARRAY, MULTISET
    public static final String FIELD_NAME_ELEMENT_TYPE = "elementType";

    // ROW
    public static final String FIELD_NAME_FIELDS = "fields";
    public static final String FIELD_NAME_FIELD_NAME = "name";
    public static final String FIELD_NAME_FIELD_TYPE = "fieldType";

    // RAW
    public static final String FIELD_NAME_CLASS = "class";
    public static final String FIELD_NAME_SERIALIZER = "serializer";

    public LogicalTypeJsonSerializer() {
        super(LogicalType.class);
    }

    @Override
    public void serialize(
            LogicalType logicalType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        serializeInternal(logicalType, jsonGenerator);
    }

    private void serializeInternal(LogicalType logicalType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();

        // write common fields shared by all types
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, logicalType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, logicalType.isNullable());
        // write special fields according to type root
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case NULL:
                break;
            case CHAR:
                jsonGenerator.writeNumberField(
                        FIELD_NAME_LENGTH, ((CharType) logicalType).getLength());
                break;
            case VARCHAR:
                jsonGenerator.writeNumberField(
                        FIELD_NAME_LENGTH, ((VarCharType) logicalType).getLength());
                break;
            case BINARY:
                jsonGenerator.writeNumberField(
                        FIELD_NAME_LENGTH, ((BinaryType) logicalType).getLength());
                break;
            case VARBINARY:
                jsonGenerator.writeNumberField(
                        FIELD_NAME_LENGTH, ((VarBinaryType) logicalType).getLength());
                break;
            case DECIMAL:
                jsonGenerator.writeNumberField(
                        FIELD_NAME_PRECISION, ((DecimalType) logicalType).getPrecision());
                jsonGenerator.writeNumberField(
                        FIELD_NAME_SCALE, ((DecimalType) logicalType).getScale());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                jsonGenerator.writeNumberField(
                        FIELD_NAME_PRECISION, ((TimestampType) logicalType).getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                jsonGenerator.writeNumberField(
                        FIELD_NAME_PRECISION,
                        ((LocalZonedTimestampType) logicalType).getPrecision());
                break;
            case MAP:
                serializeMap((MapType) logicalType, jsonGenerator);
                break;
            case ARRAY:
                serializeCollection(((ArrayType) logicalType).getElementType(), jsonGenerator);
                break;
            case MULTISET:
                serializeCollection(((MultisetType) logicalType).getElementType(), jsonGenerator);
                break;
            case ROW:
                serializeRow((RowType) logicalType, jsonGenerator);
                break;
            case RAW:
                if (logicalType instanceof RawType) {
                    serializeRaw((RawType<?>) logicalType, jsonGenerator);
                    break;
                }
                // fall through
            default:
                throw new ValidationException(
                        String.format(
                                "Unable to serialize logical type '%s'. Please check the documentation for supported types.",
                                logicalType.asSummaryString()));
        }

        jsonGenerator.writeEndObject();
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods for some complex types
    // --------------------------------------------------------------------------------------------

    private void serializeMap(MapType mapType, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_KEY_TYPE);
        serializeInternal(mapType.getKeyType(), jsonGenerator);
        jsonGenerator.writeFieldName(FIELD_NAME_VALUE_TYPE);
        serializeInternal(mapType.getValueType(), jsonGenerator);
    }

    private void serializeCollection(LogicalType elementType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_ELEMENT_TYPE);
        serializeInternal(elementType, jsonGenerator);
    }

    private void serializeRow(RowType rowType, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeArrayFieldStart(FIELD_NAME_FIELDS);
        for (RowType.RowField rowField : rowType.getFields()) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_FIELD_NAME, rowField.getName());
            jsonGenerator.writeFieldName(FIELD_NAME_FIELD_TYPE);
            serializeInternal(rowField.getType(), jsonGenerator);
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }

    private void serializeRaw(RawType<?> rawType, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStringField(FIELD_NAME_CLASS, rawType.getOriginatingClass().getName());
        jsonGenerator.writeStringField(FIELD_NAME_SERIALIZER, rawType.getSerializerString());
    }
}
