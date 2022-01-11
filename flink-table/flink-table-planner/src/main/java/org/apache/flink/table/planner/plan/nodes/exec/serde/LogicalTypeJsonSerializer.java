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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.dataview.NullSerializer;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * JSON serializer for {@link LogicalType}.
 *
 * <p>Since types are used frequently in every plan, the serializer tries to create a compact JSON
 * whenever possible. The compact representation is {@link LogicalType#asSerializableString()},
 * otherwise generic serialization is used that excludes default values.
 *
 * @see LogicalTypeJsonDeserializer for the reverse operation.
 */
@Internal
public final class LogicalTypeJsonSerializer extends StdSerializer<LogicalType> {
    private static final long serialVersionUID = 1L;

    // Common fields
    public static final String FIELD_NAME_TYPE_NAME = "type";
    public static final String FIELD_NAME_NULLABLE = "nullable";
    public static final String FIELD_NAME_DESCRIPTION = "description";

    // CHAR, VARCHAR, BINARY, VARBINARY
    public static final String FIELD_NAME_LENGTH = "length";

    // TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE
    public static final String FIELD_NAME_PRECISION = "precision";
    public static final String FIELD_NAME_TIMESTAMP_KIND = "kind";

    // ARRAY, MULTISET
    public static final String FIELD_NAME_ELEMENT_TYPE = "elementType";

    // MAP
    public static final String FIELD_NAME_KEY_TYPE = "keyType";
    public static final String FIELD_NAME_VALUE_TYPE = "valueType";

    // ROW
    public static final String FIELD_NAME_FIELDS = "fields";
    public static final String FIELD_NAME_FIELD_NAME = "name";
    public static final String FIELD_NAME_FIELD_TYPE = "fieldType";
    public static final String FIELD_NAME_FIELD_DESCRIPTION = "description";

    // DISTINCT_TYPE
    public static final String FIELD_NAME_SOURCE_TYPE = "sourceType";

    // STRUCTURED_TYPE
    public static final String FIELD_NAME_OBJECT_IDENTIFIER = "objectIdentifier";
    public static final String FIELD_NAME_IMPLEMENTATION_CLASS = "implementationClass";
    public static final String FIELD_NAME_ATTRIBUTES = "attributes";
    public static final String FIELD_NAME_ATTRIBUTE_NAME = "name";
    public static final String FIELD_NAME_ATTRIBUTE_TYPE = "attributeType";
    public static final String FIELD_NAME_ATTRIBUTE_DESCRIPTION = "description";
    public static final String FIELD_NAME_FINAL = "final";
    public static final String FIELD_NAME_INSTANTIABLE = "instantiable";
    public static final String FIELD_NAME_COMPARISON = "comparison";
    public static final String FIELD_NAME_SUPER_TYPE = "superType";

    // RAW
    public static final String FIELD_NAME_CLASS = "class";
    public static final String FIELD_NAME_EXTERNAL_DATA_TYPE = "externalDataType";
    public static final String FIELD_NAME_SPECIAL_SERIALIZER = "specialSerializer";
    public static final String FIELD_VALUE_EXTERNAL_SERIALIZER_NULL = "NULL";

    public LogicalTypeJsonSerializer() {
        super(LogicalType.class);
    }

    @Override
    public void serialize(
            LogicalType logicalType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        final ReadableConfig config = SerdeContext.get(serializerProvider).getConfiguration();
        final boolean serializeCatalogObjects =
                !config.get(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS)
                        .equals(CatalogPlanCompilation.IDENTIFIER);
        serializeInternal(logicalType, jsonGenerator, serializerProvider, serializeCatalogObjects);
    }

    private static void serializeInternal(
            LogicalType logicalType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        if (supportsCompactSerialization(logicalType, serializeCatalogObjects)) {
            serializeTypeWithCompactSerialization(logicalType, jsonGenerator);
        } else {
            // fallback to generic serialization that might still use compact serialization for
            // individual fields
            serializeTypeWithGenericSerialization(
                    logicalType, jsonGenerator, serializerProvider, serializeCatalogObjects);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Generic Serialization
    // --------------------------------------------------------------------------------------------

    private static void serializeTypeWithGenericSerialization(
            LogicalType logicalType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, logicalType.getTypeRoot().name());
        if (!logicalType.isNullable()) {
            jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, false);
        }

        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                serializeZeroLengthString(jsonGenerator);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final TimestampType timestampType = (TimestampType) logicalType;
                serializeTimestamp(
                        timestampType.getPrecision(),
                        timestampType.getKind(),
                        jsonGenerator,
                        serializerProvider);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                final ZonedTimestampType zonedTimestampType = (ZonedTimestampType) logicalType;
                serializeTimestamp(
                        zonedTimestampType.getPrecision(),
                        zonedTimestampType.getKind(),
                        jsonGenerator,
                        serializerProvider);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) logicalType;
                serializeTimestamp(
                        localZonedTimestampType.getPrecision(),
                        localZonedTimestampType.getKind(),
                        jsonGenerator,
                        serializerProvider);
                break;
            case ARRAY:
                serializeCollection(
                        ((ArrayType) logicalType).getElementType(),
                        jsonGenerator,
                        serializerProvider,
                        serializeCatalogObjects);
                break;
            case MULTISET:
                serializeCollection(
                        ((MultisetType) logicalType).getElementType(),
                        jsonGenerator,
                        serializerProvider,
                        serializeCatalogObjects);
                break;
            case MAP:
                serializeMap(
                        (MapType) logicalType,
                        jsonGenerator,
                        serializerProvider,
                        serializeCatalogObjects);
                break;
            case ROW:
                serializeRow(
                        (RowType) logicalType,
                        jsonGenerator,
                        serializerProvider,
                        serializeCatalogObjects);
                break;
            case DISTINCT_TYPE:
                serializeDistinctType(
                        (DistinctType) logicalType,
                        jsonGenerator,
                        serializerProvider,
                        serializeCatalogObjects);
                break;
            case STRUCTURED_TYPE:
                serializeStructuredType(
                        (StructuredType) logicalType,
                        jsonGenerator,
                        serializerProvider,
                        serializeCatalogObjects);
                break;
            case SYMBOL:
                // type root is enough
                break;
            case RAW:
                if (logicalType instanceof RawType) {
                    serializeSpecializedRaw(
                            (RawType<?>) logicalType, jsonGenerator, serializerProvider);
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

    private static void serializeZeroLengthString(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, 0);
    }

    private static void serializeTimestamp(
            int precision,
            TimestampKind kind,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, precision);
        serializerProvider.defaultSerializeField(FIELD_NAME_TIMESTAMP_KIND, kind, jsonGenerator);
    }

    private static void serializeCollection(
            LogicalType elementType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_ELEMENT_TYPE);
        serializeInternal(elementType, jsonGenerator, serializerProvider, serializeCatalogObjects);
    }

    private static void serializeMap(
            MapType mapType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_KEY_TYPE);
        serializeInternal(
                mapType.getKeyType(), jsonGenerator, serializerProvider, serializeCatalogObjects);
        jsonGenerator.writeFieldName(FIELD_NAME_VALUE_TYPE);
        serializeInternal(
                mapType.getValueType(), jsonGenerator, serializerProvider, serializeCatalogObjects);
    }

    private static void serializeRow(
            RowType rowType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        jsonGenerator.writeArrayFieldStart(FIELD_NAME_FIELDS);
        for (RowType.RowField rowField : rowType.getFields()) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_FIELD_NAME, rowField.getName());
            jsonGenerator.writeFieldName(FIELD_NAME_FIELD_TYPE);
            serializeInternal(
                    rowField.getType(), jsonGenerator, serializerProvider, serializeCatalogObjects);
            if (rowField.getDescription().isPresent()) {
                jsonGenerator.writeStringField(
                        FIELD_NAME_FIELD_DESCRIPTION, rowField.getDescription().get());
            }
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }

    private static void serializeDistinctType(
            DistinctType distinctType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        serializerProvider.defaultSerializeField(
                FIELD_NAME_OBJECT_IDENTIFIER,
                distinctType.getObjectIdentifier().orElseThrow(IllegalStateException::new),
                jsonGenerator);
        if (distinctType.getDescription().isPresent()) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_FIELD_DESCRIPTION, distinctType.getDescription().get());
        }
        jsonGenerator.writeFieldName(FIELD_NAME_SOURCE_TYPE);
        serializeInternal(
                distinctType.getSourceType(),
                jsonGenerator,
                serializerProvider,
                serializeCatalogObjects);
    }

    private static void serializeStructuredType(
            StructuredType structuredType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        if (structuredType.getObjectIdentifier().isPresent()) {
            serializerProvider.defaultSerializeField(
                    FIELD_NAME_OBJECT_IDENTIFIER,
                    structuredType.getObjectIdentifier().get(),
                    jsonGenerator);
        }
        if (structuredType.getDescription().isPresent()) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_DESCRIPTION, structuredType.getDescription().get());
        }
        if (structuredType.getImplementationClass().isPresent()) {
            serializerProvider.defaultSerializeField(
                    FIELD_NAME_IMPLEMENTATION_CLASS,
                    structuredType.getImplementationClass().get(),
                    jsonGenerator);
        }
        jsonGenerator.writeFieldName(FIELD_NAME_ATTRIBUTES);
        jsonGenerator.writeStartArray();
        for (StructuredAttribute attribute : structuredType.getAttributes()) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_ATTRIBUTE_NAME, attribute.getName());
            jsonGenerator.writeFieldName(FIELD_NAME_ATTRIBUTE_TYPE);
            serializeInternal(
                    attribute.getType(),
                    jsonGenerator,
                    serializerProvider,
                    serializeCatalogObjects);
            if (attribute.getDescription().isPresent()) {
                jsonGenerator.writeStringField(
                        FIELD_NAME_ATTRIBUTE_DESCRIPTION, attribute.getDescription().get());
            }
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
        if (!structuredType.isFinal()) {
            jsonGenerator.writeBooleanField(FIELD_NAME_FINAL, false);
        }
        if (!structuredType.isInstantiable()) {
            jsonGenerator.writeBooleanField(FIELD_NAME_INSTANTIABLE, false);
        }
        if (structuredType.getComparison() != StructuredComparison.NONE) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_COMPARISON, structuredType.getComparison().name());
        }
        if (structuredType.getSuperType().isPresent()) {
            serializerProvider.defaultSerializeField(
                    FIELD_NAME_SUPER_TYPE, structuredType.getSuperType().get(), jsonGenerator);
        }
    }

    private static void serializeSpecializedRaw(
            RawType<?> rawType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStringField(FIELD_NAME_CLASS, rawType.getOriginatingClass().getName());
        final TypeSerializer<?> serializer = rawType.getTypeSerializer();
        if (serializer.equals(NullSerializer.INSTANCE)) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_SPECIAL_SERIALIZER, FIELD_VALUE_EXTERNAL_SERIALIZER_NULL);
        } else if (serializer instanceof ExternalSerializer) {
            final ExternalSerializer<?, ?> externalSerializer =
                    (ExternalSerializer<?, ?>) rawType.getTypeSerializer();
            if (externalSerializer.isInternalInput()) {
                throw new TableException(
                        "Asymmetric external serializers are currently not supported. "
                                + "The input must not be internal if the output is external.");
            }
            serializerProvider.defaultSerializeField(
                    FIELD_NAME_EXTERNAL_DATA_TYPE, externalSerializer.getDataType(), jsonGenerator);
        } else {
            throw new TableException("Unsupported special case for RAW type.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Compact Serialization
    // --------------------------------------------------------------------------------------------

    private static boolean supportsCompactSerialization(
            LogicalType logicalType, boolean serializeCatalogObjects) {
        return logicalType.accept(new CompactSerializationChecker(serializeCatalogObjects));
    }

    private static void serializeTypeWithCompactSerialization(
            LogicalType logicalType, JsonGenerator jsonGenerator) throws IOException {
        final String compactString = logicalType.asSerializableString();
        jsonGenerator.writeString(compactString);
    }

    /**
     * Checks whether the given type can be serialized as a compact string created from {@link
     * LogicalType#asSerializableString()}.
     */
    private static class CompactSerializationChecker extends LogicalTypeDefaultVisitor<Boolean> {

        private final boolean serializeCatalogObjects;

        CompactSerializationChecker(boolean serializeCatalogObjects) {
            this.serializeCatalogObjects = serializeCatalogObjects;
        }

        @Override
        public Boolean visit(CharType charType) {
            return charType.getLength() > 0;
        }

        @Override
        public Boolean visit(VarCharType varCharType) {
            return varCharType.getLength() > 0;
        }

        @Override
        public Boolean visit(BinaryType binaryType) {
            return binaryType.getLength() > 0;
        }

        @Override
        public Boolean visit(VarBinaryType varBinaryType) {
            return varBinaryType.getLength() > 0;
        }

        @Override
        public Boolean visit(TimestampType timestampType) {
            return timestampType.getKind() == TimestampKind.REGULAR;
        }

        @Override
        public Boolean visit(ZonedTimestampType zonedTimestampType) {
            return zonedTimestampType.getKind() == TimestampKind.REGULAR;
        }

        @Override
        public Boolean visit(LocalZonedTimestampType localZonedTimestampType) {
            return localZonedTimestampType.getKind() == TimestampKind.REGULAR;
        }

        @Override
        public Boolean visit(DistinctType distinctType) {
            // catalog-based distinct types are always string serializable,
            // however, depending on the configuration, we serialize the entire type
            return !serializeCatalogObjects;
        }

        @Override
        public Boolean visit(StructuredType structuredType) {
            // catalog-based structured types are always string serializable,
            // however, depending on the configuration, we serialize the entire type
            return structuredType.getObjectIdentifier().isPresent() && !serializeCatalogObjects;
        }

        @Override
        protected Boolean defaultMethod(LogicalType logicalType) {
            if (!logicalType.getChildren().stream().allMatch(t -> t.accept(this))) {
                return false;
            }
            switch (logicalType.getTypeRoot()) {
                case RAW:
                    if (logicalType instanceof TypeInformationRawType) {
                        return false;
                    }
                    final RawType<?> rawType = (RawType<?>) logicalType;
                    final TypeSerializer<?> serializer = rawType.getTypeSerializer();
                    // external serializer will go through data type serialization
                    if (serializer instanceof ExternalSerializer) {
                        return false;
                    }
                    // null serializer will go through special serialization
                    if (serializer.equals(NullSerializer.INSTANCE)) {
                        return false;
                    }
                    // fall through
                case BOOLEAN:
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                case ARRAY:
                case MULTISET:
                case MAP:
                case ROW:
                case NULL:
                    return true;
                default:
                    // fall back to generic serialization
                    return false;
            }
        }
    }
}
