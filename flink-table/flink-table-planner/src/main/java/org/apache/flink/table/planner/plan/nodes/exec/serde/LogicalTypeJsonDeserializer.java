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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.dataview.NullSerializer;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.loadClass;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ATTRIBUTES;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ATTRIBUTE_DESCRIPTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ATTRIBUTE_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ATTRIBUTE_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_COMPARISON;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_DESCRIPTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ELEMENT_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_EXTERNAL_DATA_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELDS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELD_DESCRIPTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELD_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELD_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FINAL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_IMPLEMENTATION_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_INSTANTIABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_KEY_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_LENGTH;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_NULLABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_OBJECT_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_PRECISION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SOURCE_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SPECIAL_SERIALIZER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SUPER_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_TIMESTAMP_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_TYPE_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_VALUE_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_VALUE_EXTERNAL_SERIALIZER_NULL;

/**
 * JSON deserializer for {@link LogicalType}.
 *
 * @see LogicalTypeJsonSerializer for the reverse operation
 */
@Internal
final class LogicalTypeJsonDeserializer extends StdDeserializer<LogicalType> {
    private static final long serialVersionUID = 1L;

    LogicalTypeJsonDeserializer() {
        super(LogicalType.class);
    }

    @Override
    public LogicalType deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final JsonNode logicalTypeNode = jsonParser.readValueAsTree();
        final SerdeContext serdeContext = SerdeContext.get(ctx);
        return deserialize(logicalTypeNode, serdeContext);
    }

    static LogicalType deserialize(JsonNode logicalTypeNode, SerdeContext serdeContext) {
        if (logicalTypeNode.isTextual()) {
            return deserializeWithCompactSerialization(logicalTypeNode.asText(), serdeContext);
        } else {
            return deserializeWithExtendedSerialization(logicalTypeNode, serdeContext);
        }
    }

    private static LogicalType deserializeWithCompactSerialization(
            String serializableString, SerdeContext serdeContext) {
        final DataTypeFactory dataTypeFactory =
                serdeContext.getFlinkContext().getCatalogManager().getDataTypeFactory();
        return dataTypeFactory.createLogicalType(serializableString);
    }

    private static LogicalType deserializeWithExtendedSerialization(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final LogicalType logicalType = deserializeFromRoot(logicalTypeNode, serdeContext);
        if (logicalTypeNode.has(FIELD_NAME_NULLABLE)) {
            final boolean isNullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
            return logicalType.copy(isNullable);
        }
        return logicalType.copy(true);
    }

    private static LogicalType deserializeFromRoot(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final LogicalTypeRoot typeRoot =
                LogicalTypeRoot.valueOf(logicalTypeNode.get(FIELD_NAME_TYPE_NAME).asText());
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return deserializeZeroLengthString(typeRoot, logicalTypeNode);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return deserializeTimestamp(typeRoot, logicalTypeNode);
            case ARRAY:
            case MULTISET:
                return deserializeCollection(typeRoot, logicalTypeNode, serdeContext);
            case MAP:
                return deserializeMap(logicalTypeNode, serdeContext);
            case ROW:
                return deserializeRow(logicalTypeNode, serdeContext);
            case DISTINCT_TYPE:
                return deserializeDistinctType(logicalTypeNode, serdeContext);
            case STRUCTURED_TYPE:
                return deserializeStructuredType(logicalTypeNode, serdeContext);
            case SYMBOL:
                return new SymbolType<>();
            case RAW:
                return deserializeSpecializedRaw(logicalTypeNode, serdeContext);
            default:
                throw new TableException("Unsupported type root: " + typeRoot);
        }
    }

    private static LogicalType deserializeZeroLengthString(
            LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        final int length = logicalTypeNode.get(FIELD_NAME_LENGTH).asInt();
        if (length != 0) {
            throw new TableException("String length should be 0.");
        }
        switch (typeRoot) {
            case CHAR:
                return CharType.ofEmptyLiteral();
            case VARCHAR:
                return VarCharType.ofEmptyLiteral();
            case BINARY:
                return BinaryType.ofEmptyLiteral();
            case VARBINARY:
                return VarBinaryType.ofEmptyLiteral();
            default:
                throw new TableException("String type root expected.");
        }
    }

    private static LogicalType deserializeTimestamp(
            LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        final int precision = logicalTypeNode.get(FIELD_NAME_PRECISION).asInt();
        final TimestampKind kind =
                TimestampKind.valueOf(logicalTypeNode.get(FIELD_NAME_TIMESTAMP_KIND).asText());
        switch (typeRoot) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampType(true, kind, precision);
            case TIMESTAMP_WITH_TIME_ZONE:
                return new ZonedTimestampType(true, kind, precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(true, kind, precision);
            default:
                throw new TableException("Timestamp type root expected.");
        }
    }

    private static LogicalType deserializeCollection(
            LogicalTypeRoot typeRoot, JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final JsonNode elementNode = logicalTypeNode.get(FIELD_NAME_ELEMENT_TYPE);
        final LogicalType elementType = deserialize(elementNode, serdeContext);
        switch (typeRoot) {
            case ARRAY:
                return new ArrayType(elementType);
            case MULTISET:
                return new MultisetType(elementType);
            default:
                throw new TableException("Collection type root expected.");
        }
    }

    private static LogicalType deserializeMap(JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final JsonNode keyNode = logicalTypeNode.get(FIELD_NAME_KEY_TYPE);
        final LogicalType keyType = deserialize(keyNode, serdeContext);
        final JsonNode valueNode = logicalTypeNode.get(FIELD_NAME_VALUE_TYPE);
        final LogicalType valueType = deserialize(valueNode, serdeContext);
        return new MapType(keyType, valueType);
    }

    private static LogicalType deserializeRow(JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final ArrayNode fieldNodes = (ArrayNode) logicalTypeNode.get(FIELD_NAME_FIELDS);
        final List<RowField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldNodes) {
            final String fieldName = fieldNode.get(FIELD_NAME_FIELD_NAME).asText();
            final LogicalType fieldType =
                    deserialize(fieldNode.get(FIELD_NAME_FIELD_TYPE), serdeContext);
            final String fieldDescription;
            if (fieldNode.has(FIELD_NAME_FIELD_DESCRIPTION)) {
                fieldDescription = fieldNode.get(FIELD_NAME_FIELD_DESCRIPTION).asText();
            } else {
                fieldDescription = null;
            }
            fields.add(new RowField(fieldName, fieldType, fieldDescription));
        }
        return new RowType(fields);
    }

    private static LogicalType deserializeDistinctType(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final ObjectIdentifier identifier =
                ObjectIdentifierJsonDeserializer.deserialize(
                        logicalTypeNode.get(FIELD_NAME_OBJECT_IDENTIFIER).asText(), serdeContext);
        final CatalogPlanRestore restoreStrategy =
                serdeContext
                        .getConfiguration()
                        .get(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS);
        switch (restoreStrategy) {
            case ALL:
                if (logicalTypeNode.has(FIELD_NAME_SOURCE_TYPE)) {
                    return deserializeDistinctTypeFromPlan(
                            identifier, logicalTypeNode, serdeContext);
                }
                return deserializeUserDefinedTypeFromCatalog(identifier, serdeContext);
            case ALL_ENFORCED:
                return deserializeDistinctTypeFromPlan(identifier, logicalTypeNode, serdeContext);
            case IDENTIFIER:
                return deserializeUserDefinedTypeFromCatalog(identifier, serdeContext);
            default:
                throw new TableException("Unsupported catalog restore strategy.");
        }
    }

    private static LogicalType deserializeDistinctTypeFromPlan(
            ObjectIdentifier identifier, JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final LogicalType sourceType =
                deserialize(logicalTypeNode.get(FIELD_NAME_SOURCE_TYPE), serdeContext);
        final DistinctType.Builder builder = DistinctType.newBuilder(identifier, sourceType);
        if (logicalTypeNode.has(FIELD_NAME_FIELD_DESCRIPTION)) {
            builder.description(logicalTypeNode.get(FIELD_NAME_FIELD_DESCRIPTION).asText());
        }
        return builder.build();
    }

    private static LogicalType deserializeStructuredType(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        // inline structured types have no object identifier
        if (!logicalTypeNode.has(FIELD_NAME_OBJECT_IDENTIFIER)) {
            return deserializeStructuredTypeFromPlan(logicalTypeNode, serdeContext);
        }

        // for catalog structured types
        final ObjectIdentifier identifier =
                ObjectIdentifierJsonDeserializer.deserialize(
                        logicalTypeNode.get(FIELD_NAME_OBJECT_IDENTIFIER).asText(), serdeContext);
        final CatalogPlanRestore restoreStrategy =
                serdeContext
                        .getConfiguration()
                        .get(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS);
        switch (restoreStrategy) {
            case ALL:
                if (logicalTypeNode.has(FIELD_NAME_ATTRIBUTES)) {
                    return deserializeStructuredTypeFromPlan(logicalTypeNode, serdeContext);
                }
                return deserializeUserDefinedTypeFromCatalog(identifier, serdeContext);
            case ALL_ENFORCED:
                return deserializeStructuredTypeFromPlan(logicalTypeNode, serdeContext);
            case IDENTIFIER:
                return deserializeUserDefinedTypeFromCatalog(identifier, serdeContext);
            default:
                throw new TableException("Unsupported catalog restore strategy.");
        }
    }

    private static LogicalType deserializeUserDefinedTypeFromCatalog(
            ObjectIdentifier identifier, SerdeContext serdeContext) {
        final DataTypeFactory dataTypeFactory =
                serdeContext.getFlinkContext().getCatalogManager().getDataTypeFactory();
        return dataTypeFactory.createLogicalType(UnresolvedIdentifier.of(identifier));
    }

    private static LogicalType deserializeStructuredTypeFromPlan(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final ObjectIdentifier identifier;
        if (logicalTypeNode.has(FIELD_NAME_OBJECT_IDENTIFIER)) {
            identifier =
                    ObjectIdentifierJsonDeserializer.deserialize(
                            logicalTypeNode.get(FIELD_NAME_OBJECT_IDENTIFIER).asText(),
                            serdeContext);
        } else {
            identifier = null;
        }

        final Class<?> implementationClass;
        if (logicalTypeNode.has(FIELD_NAME_IMPLEMENTATION_CLASS)) {
            implementationClass =
                    loadClass(
                            logicalTypeNode.get(FIELD_NAME_IMPLEMENTATION_CLASS).asText(),
                            serdeContext,
                            "structured type");
        } else {
            implementationClass = null;
        }

        final StructuredType.Builder builder;
        if (identifier != null && implementationClass != null) {
            builder = StructuredType.newBuilder(identifier, implementationClass);
        } else if (identifier != null) {
            builder = StructuredType.newBuilder(identifier);
        } else {
            builder = StructuredType.newBuilder(implementationClass);
        }

        if (logicalTypeNode.has(FIELD_NAME_DESCRIPTION)) {
            builder.description(logicalTypeNode.get(FIELD_NAME_FIELD_DESCRIPTION).asText());
        }

        final ArrayNode attributeNodes = (ArrayNode) logicalTypeNode.get(FIELD_NAME_ATTRIBUTES);
        final List<StructuredAttribute> attributes = new ArrayList<>();
        for (JsonNode attributeNode : attributeNodes) {
            final String attributeName = attributeNode.get(FIELD_NAME_ATTRIBUTE_NAME).asText();
            final LogicalType attributeType =
                    deserialize(attributeNode.get(FIELD_NAME_ATTRIBUTE_TYPE), serdeContext);
            final String attributeDescription;
            if (attributeNode.has(FIELD_NAME_ATTRIBUTE_DESCRIPTION)) {
                attributeDescription = attributeNode.get(FIELD_NAME_ATTRIBUTE_DESCRIPTION).asText();
            } else {
                attributeDescription = null;
            }
            attributes.add(
                    new StructuredAttribute(attributeName, attributeType, attributeDescription));
        }
        builder.attributes(attributes);

        if (logicalTypeNode.has(FIELD_NAME_FINAL)) {
            builder.setFinal(logicalTypeNode.get(FIELD_NAME_FINAL).asBoolean());
        }

        if (logicalTypeNode.has(FIELD_NAME_INSTANTIABLE)) {
            builder.setInstantiable(logicalTypeNode.get(FIELD_NAME_INSTANTIABLE).asBoolean());
        }

        if (logicalTypeNode.has(FIELD_NAME_COMPARISON)) {
            builder.comparison(
                    StructuredComparison.valueOf(
                            logicalTypeNode.get(FIELD_NAME_COMPARISON).asText()));
        }

        if (logicalTypeNode.has(FIELD_NAME_SUPER_TYPE)) {
            final StructuredType superType =
                    (StructuredType)
                            deserialize(logicalTypeNode.get(FIELD_NAME_SUPER_TYPE), serdeContext);
            builder.superType(superType);
        }

        return builder.build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static LogicalType deserializeSpecializedRaw(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final Class<?> clazz =
                loadClass(logicalTypeNode.get(FIELD_NAME_CLASS).asText(), serdeContext, "RAW type");

        final TypeSerializer<?> serializer;
        if (logicalTypeNode.has(FIELD_NAME_SPECIAL_SERIALIZER)) {
            final String specialSerializer =
                    logicalTypeNode.get(FIELD_NAME_SPECIAL_SERIALIZER).asText();
            if (FIELD_VALUE_EXTERNAL_SERIALIZER_NULL.equals(specialSerializer)) {
                serializer = NullSerializer.INSTANCE;
            } else {
                throw new TableException("Unknown external serializer: " + specialSerializer);
            }
        } else if (logicalTypeNode.has(FIELD_NAME_EXTERNAL_DATA_TYPE)) {
            final DataType dataType =
                    DataTypeJsonDeserializer.deserialize(
                            logicalTypeNode.get(FIELD_NAME_EXTERNAL_DATA_TYPE), serdeContext);
            serializer = ExternalSerializer.of(dataType);
        } else {
            throw new TableException("Invalid RAW type.");
        }

        return new RawType(clazz, serializer);
    }
}
