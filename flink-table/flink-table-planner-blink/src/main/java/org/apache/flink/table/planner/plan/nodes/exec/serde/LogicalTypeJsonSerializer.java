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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.List;

/**
 * JSON serializer for {@link LogicalType}. refer to {@link LogicalTypeJsonDeserializer} for
 * deserializer.
 */
public class LogicalTypeJsonSerializer extends StdSerializer<LogicalType> {
    private static final long serialVersionUID = 1L;
    // common fields
    public static final String FIELD_NAME_TYPE_NAME = "type";
    public static final String FIELD_NAME_NULLABLE = "nullable";
    // CharType's field
    public static final String FIELD_NAME_LENGTH = "length";
    // SymbolType's field
    public static final String FIELD_NAME_SYMBOL_CLASS = "symbolClass";
    // TypeInformationRawType's field
    public static final String FIELD_NAME_TYPE_INFO = "typeInfo";
    // StructuredType's fields
    public static final String FIELD_NAME_IDENTIFIER = "identifier";
    public static final String FIELD_NAME_IMPLEMENTATION_CLASS = "implementationClass";
    public static final String FIELD_NAME_ATTRIBUTES = "attributes";
    public static final String FIELD_NAME_NAME = "name";
    public static final String FIELD_NAME_LOGICAL_TYPE = "logicalType";
    public static final String FIELD_NAME_DESCRIPTION = "description";
    public static final String FIELD_NAME_FINAL = "final";
    public static final String FIELD_NAME_INSTANTIABLE = "instantiable";
    public static final String FIELD_NAME_COMPARISION = "comparision";
    public static final String FIELD_NAME_SUPPER_TYPE = "supperType";
    // DistinctType's fields
    public static final String FIELD_NAME_SOURCE_TYPE = "sourceType";
    // TimestampType's fields
    public static final String FIELD_NAME_PRECISION = "precision";
    public static final String FIELD_NAME_TIMESTAMP_KIND = "kind";
    // RowType
    public static final String FIELD_NAME_FIELDS = "fields";
    // MapType
    public static final String FIELD_NAME_KEY_TYPE = "keyType";
    public static final String FIELD_NAME_VALUE_TYPE = "valueType";
    // ArrayType/MultiSetType
    public static final String FIELD_NAME_ELEMENT_TYPE = "elementType";

    public LogicalTypeJsonSerializer() {
        super(LogicalType.class);
    }

    @Override
    public void serialize(
            LogicalType logicalType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        if (logicalType instanceof CharType) {
            // Zero-length character strings have no serializable string representation.
            serialize((CharType) logicalType, jsonGenerator);
        } else if (logicalType instanceof VarCharType) {
            // Zero-length character strings have no serializable string representation.
            serialize((VarCharType) logicalType, jsonGenerator);
        } else if (logicalType instanceof BinaryType) {
            // Zero-length binary strings have no serializable string representation.
            serialize((BinaryType) logicalType, jsonGenerator);
        } else if (logicalType instanceof VarBinaryType) {
            // Zero-length binary strings have no serializable string representation.
            serialize((VarBinaryType) logicalType, jsonGenerator);
        } else if (logicalType instanceof SymbolType) {
            // SymbolType does not support `asSerializableString`
            serialize((SymbolType<?>) logicalType, jsonGenerator);
        } else if (logicalType instanceof TypeInformationRawType) {
            // TypeInformationRawType does not support `asSerializableString`
            serialize((TypeInformationRawType<?>) logicalType, jsonGenerator);
        } else if (logicalType instanceof StructuredType) {
            //  StructuredType does not full support `asSerializableString`
            serialize((StructuredType) logicalType, jsonGenerator);
        } else if (logicalType instanceof DistinctType) {
            //  DistinctType does not full support `asSerializableString`
            serialize((DistinctType) logicalType, jsonGenerator);
        } else if (logicalType instanceof TimestampType) {
            // TimestampType does not consider `TimestampKind`
            serialize((TimestampType) logicalType, jsonGenerator);
        } else if (logicalType instanceof ZonedTimestampType) {
            // ZonedTimestampType does not consider `TimestampKind`
            serialize((ZonedTimestampType) logicalType, jsonGenerator);
        } else if (logicalType instanceof LocalZonedTimestampType) {
            // LocalZonedTimestampType does not consider `TimestampKind`
            serialize((LocalZonedTimestampType) logicalType, jsonGenerator);
        } else if (logicalType instanceof RowType) {
            serializeRowType((RowType) logicalType, jsonGenerator, serializerProvider);
        } else if (logicalType instanceof MapType) {
            serializeMapType((MapType) logicalType, jsonGenerator, serializerProvider);
        } else if (logicalType instanceof ArrayType) {
            serializeArrayType((ArrayType) logicalType, jsonGenerator, serializerProvider);
        } else if (logicalType instanceof MultisetType) {
            serializeMultisetType((MultisetType) logicalType, jsonGenerator, serializerProvider);
        } else if (logicalType instanceof UnresolvedUserDefinedType) {
            throw new TableException(
                    "Can not serialize an UnresolvedUserDefinedType instance. \n"
                            + "It needs to be resolved into a proper user-defined type.\"");
        } else {
            jsonGenerator.writeObject(logicalType.asSerializableString());
        }
    }

    private void serializeRowType(
            RowType rowType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, rowType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, rowType.isNullable());
        List<RowType.RowField> fields = rowType.getFields();
        jsonGenerator.writeArrayFieldStart(FIELD_NAME_FIELDS);
        for (RowType.RowField rowField : fields) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName(rowField.getName());
            serialize(rowField.getType(), jsonGenerator, serializerProvider);
            if (rowField.getDescription().isPresent()) {
                jsonGenerator.writeStringField(
                        FIELD_NAME_DESCRIPTION, rowField.getDescription().get());
            }
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
    }

    private void serializeMapType(
            MapType mapType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, mapType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, mapType.isNullable());
        jsonGenerator.writeFieldName(FIELD_NAME_KEY_TYPE);
        serialize(mapType.getKeyType(), jsonGenerator, serializerProvider);
        jsonGenerator.writeFieldName(FIELD_NAME_VALUE_TYPE);
        serialize(mapType.getValueType(), jsonGenerator, serializerProvider);
        jsonGenerator.writeEndObject();
    }

    private void serializeArrayType(
            ArrayType arrayType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, arrayType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, arrayType.isNullable());
        jsonGenerator.writeFieldName(FIELD_NAME_ELEMENT_TYPE);
        serialize(arrayType.getElementType(), jsonGenerator, serializerProvider);
        jsonGenerator.writeEndObject();
    }

    private void serializeMultisetType(
            MultisetType multisetType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, multisetType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, multisetType.isNullable());
        jsonGenerator.writeFieldName(FIELD_NAME_ELEMENT_TYPE);
        serialize(multisetType.getElementType(), jsonGenerator, serializerProvider);
        jsonGenerator.writeEndObject();
    }

    private void serialize(CharType charType, JsonGenerator jsonGenerator) throws IOException {
        // Zero-length character strings have no serializable string representation.
        if (charType.getLength() == CharType.EMPTY_LITERAL_LENGTH) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, charType.getTypeRoot().name());
            jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, charType.isNullable());
            jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, charType.getLength());
            jsonGenerator.writeEndObject();
        } else {
            jsonGenerator.writeObject(charType.asSerializableString());
        }
    }

    private void serialize(VarCharType varCharType, JsonGenerator jsonGenerator)
            throws IOException {
        // Zero-length character strings have no serializable string representation.
        if (varCharType.getLength() == VarCharType.EMPTY_LITERAL_LENGTH) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, varCharType.getTypeRoot().name());
            jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, varCharType.isNullable());
            jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, varCharType.getLength());
            jsonGenerator.writeEndObject();
        } else {
            jsonGenerator.writeObject(varCharType.asSerializableString());
        }
    }

    private void serialize(BinaryType binaryType, JsonGenerator jsonGenerator) throws IOException {
        // Zero-length binary strings have no serializable string representation.
        if (binaryType.getLength() == BinaryType.EMPTY_LITERAL_LENGTH) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, binaryType.getTypeRoot().name());
            jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, binaryType.isNullable());
            jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, binaryType.getLength());
            jsonGenerator.writeEndObject();
        } else {
            jsonGenerator.writeObject(binaryType.asSerializableString());
        }
    }

    private void serialize(VarBinaryType varBinaryType, JsonGenerator jsonGenerator)
            throws IOException {
        // Zero-length binary strings have no serializable string representation.
        if (varBinaryType.getLength() == VarBinaryType.EMPTY_LITERAL_LENGTH) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(
                    FIELD_NAME_TYPE_NAME, varBinaryType.getTypeRoot().name());
            jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, varBinaryType.isNullable());
            jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, varBinaryType.getLength());
            jsonGenerator.writeEndObject();
        } else {
            jsonGenerator.writeObject(varBinaryType.asSerializableString());
        }
    }

    private void serialize(SymbolType<?> symbolType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, symbolType.isNullable());
        jsonGenerator.writeStringField(
                FIELD_NAME_SYMBOL_CLASS, symbolType.getDefaultConversion().getName());
        jsonGenerator.writeEndObject();
    }

    private void serialize(TypeInformationRawType<?> rawType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, rawType.isNullable());
        jsonGenerator.writeStringField(
                FIELD_NAME_TYPE_INFO,
                EncodingUtils.encodeObjectToString(rawType.getTypeInformation()));
        jsonGenerator.writeEndObject();
    }

    private void serialize(StructuredType structuredType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(
                FIELD_NAME_TYPE_NAME, LogicalTypeRoot.STRUCTURED_TYPE.name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, structuredType.isNullable());
        if (structuredType.getObjectIdentifier().isPresent()) {
            jsonGenerator.writeObjectField(
                    FIELD_NAME_IDENTIFIER, structuredType.getObjectIdentifier().get());
        }
        if (structuredType.getImplementationClass().isPresent()) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_IMPLEMENTATION_CLASS,
                    structuredType.getImplementationClass().get().getName());
        }
        jsonGenerator.writeFieldName(FIELD_NAME_ATTRIBUTES);
        jsonGenerator.writeStartArray();
        for (StructuredType.StructuredAttribute attribute : structuredType.getAttributes()) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_NAME, attribute.getName());
            jsonGenerator.writeObjectField(FIELD_NAME_LOGICAL_TYPE, attribute.getType());
            if (attribute.getDescription().isPresent()) {
                jsonGenerator.writeStringField(
                        FIELD_NAME_DESCRIPTION, attribute.getDescription().get());
            }
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeBooleanField(FIELD_NAME_FINAL, structuredType.isFinal());
        jsonGenerator.writeBooleanField(FIELD_NAME_INSTANTIABLE, structuredType.isInstantiable());
        jsonGenerator.writeStringField(
                FIELD_NAME_COMPARISION, structuredType.getComparision().name());
        if (structuredType.getSuperType().isPresent()) {
            jsonGenerator.writeObjectField(
                    FIELD_NAME_SUPPER_TYPE, structuredType.getSuperType().get());
        }
        if (structuredType.getDescription().isPresent()) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_DESCRIPTION, structuredType.getDescription().get());
        }
        jsonGenerator.writeEndObject();
    }

    private void serialize(DistinctType distinctType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, LogicalTypeRoot.DISTINCT_TYPE.name());
        Preconditions.checkArgument(distinctType.getObjectIdentifier().isPresent());
        jsonGenerator.writeObjectField(
                FIELD_NAME_IDENTIFIER, distinctType.getObjectIdentifier().get());
        jsonGenerator.writeObjectField(FIELD_NAME_SOURCE_TYPE, distinctType.getSourceType());
        if (distinctType.getDescription().isPresent()) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_DESCRIPTION, distinctType.getDescription().get());
        }
        jsonGenerator.writeEndObject();
    }

    private void serialize(TimestampType timestampType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, timestampType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, timestampType.isNullable());
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, timestampType.getPrecision());
        jsonGenerator.writeObjectField(FIELD_NAME_TIMESTAMP_KIND, timestampType.getKind());
        jsonGenerator.writeEndObject();
    }

    private void serialize(ZonedTimestampType timestampType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, timestampType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, timestampType.isNullable());
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, timestampType.getPrecision());
        jsonGenerator.writeObjectField(FIELD_NAME_TIMESTAMP_KIND, timestampType.getKind());
        jsonGenerator.writeEndObject();
    }

    private void serialize(LocalZonedTimestampType timestampType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, timestampType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, timestampType.isNullable());
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, timestampType.getPrecision());
        jsonGenerator.writeObjectField(FIELD_NAME_TIMESTAMP_KIND, timestampType.getKind());
        jsonGenerator.writeEndObject();
    }
}
