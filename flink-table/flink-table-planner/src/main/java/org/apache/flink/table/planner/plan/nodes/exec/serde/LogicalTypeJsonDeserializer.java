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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
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
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ATTRIBUTES;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_COMPARISION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_DESCRIPTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ELEMENT_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FIELDS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FINAL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_IMPLEMENTATION_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_INSTANTIABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_KEY_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_LENGTH;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_LOGICAL_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_NULLABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_PRECISION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SOURCE_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SUPPER_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SYMBOL_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_TIMESTAMP_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_TYPE_INFO;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_TYPE_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_VALUE_TYPE;

/**
 * JSON deserializer for {@link LogicalType}. refer to {@link LogicalTypeJsonSerializer} for
 * serializer.
 */
public class LogicalTypeJsonDeserializer extends StdDeserializer<LogicalType> {
    private static final long serialVersionUID = 1L;

    public LogicalTypeJsonDeserializer() {
        super(LogicalType.class);
    }

    @Override
    public LogicalType deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException, JsonProcessingException {
        final JsonNode logicalTypeNode = jsonParser.readValueAsTree();
        SerdeContext serdeCtx = ((FlinkDeserializationContext) ctx).getSerdeContext();
        return deserialize(logicalTypeNode, serdeCtx);
    }

    public LogicalType deserialize(JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        if (logicalTypeNode.get(FIELD_NAME_TYPE_NAME) != null) {
            String typeName = logicalTypeNode.get(FIELD_NAME_TYPE_NAME).asText().toUpperCase();
            LogicalTypeRoot root = LogicalTypeRoot.valueOf(typeName);
            switch (root) {
                case CHAR:
                    // Zero-length character strings have no serializable string representation.
                    return deserializeCharType(logicalTypeNode);
                case VARCHAR:
                    // Zero-length character strings have no serializable string representation.
                    return deserializeVarCharType(logicalTypeNode);
                case BINARY:
                    // Zero-length binary strings have no serializable string representation.
                    return deserializeBinaryType(logicalTypeNode);
                case VARBINARY:
                    // Zero-length binary strings have no serializable string representation.
                    return deserializeVarBinaryType(logicalTypeNode);
                case DISTINCT_TYPE:
                    return deserializeDistinctType(logicalTypeNode, serdeCtx);
                case STRUCTURED_TYPE:
                    return deserializeStructuredType(logicalTypeNode, serdeCtx);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return deserializeTimestampType(logicalTypeNode);
                case TIMESTAMP_WITH_TIME_ZONE:
                    return deserializeZonedTimestampType(logicalTypeNode);
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return deserializeLocalZonedTimestampType(logicalTypeNode);
                case ROW:
                    return deserializeRowType(logicalTypeNode, serdeCtx);
                case MAP:
                    return deserializeMapType(logicalTypeNode, serdeCtx);
                case ARRAY:
                    return deserializeArrayType(logicalTypeNode, serdeCtx);
                case MULTISET:
                    return deserializeMultisetType(logicalTypeNode, serdeCtx);
                default:
                    throw new TableException("Unsupported type name:" + typeName);
            }
        } else if (logicalTypeNode.get(FIELD_NAME_SYMBOL_CLASS) != null) {
            return deserializeSymbolType(logicalTypeNode);
        } else if (logicalTypeNode.get(FIELD_NAME_TYPE_INFO) != null) {
            return deserializeTypeInformationRawType(logicalTypeNode, serdeCtx);
        } else {
            return LogicalTypeParser.parse(logicalTypeNode.asText());
        }
    }

    private RowType deserializeRowType(JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        List<RowType.RowField> rowFields = new ArrayList<>();
        Iterator<JsonNode> elements = logicalTypeNode.get(FIELD_NAME_FIELDS).elements();
        while (elements.hasNext()) {
            JsonNode node = elements.next();
            String filedName = node.fieldNames().next();
            LogicalType fieldType = deserialize(node.get(filedName), serdeCtx);
            if (node.has(FIELD_NAME_DESCRIPTION)) {
                rowFields.add(
                        new RowType.RowField(
                                filedName, fieldType, node.get(FIELD_NAME_DESCRIPTION).asText()));
            } else {
                rowFields.add(new RowType.RowField(filedName, fieldType));
            }
        }
        return new RowType(nullable, rowFields);
    }

    private MapType deserializeMapType(JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        LogicalType keyLogicalType =
                deserialize(logicalTypeNode.get(FIELD_NAME_KEY_TYPE), serdeCtx);
        LogicalType valueLogicalType =
                deserialize(logicalTypeNode.get(FIELD_NAME_VALUE_TYPE), serdeCtx);
        return new MapType(nullable, keyLogicalType, valueLogicalType);
    }

    private ArrayType deserializeArrayType(JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        LogicalType elementType =
                deserialize(logicalTypeNode.get(FIELD_NAME_ELEMENT_TYPE), serdeCtx);
        return new ArrayType(nullable, elementType);
    }

    private MultisetType deserializeMultisetType(JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        LogicalType elementType =
                deserialize(logicalTypeNode.get(FIELD_NAME_ELEMENT_TYPE), serdeCtx);
        return new MultisetType(nullable, elementType);
    }

    private CharType deserializeCharType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        int length = logicalTypeNode.get(FIELD_NAME_LENGTH).asInt();
        if (length == CharType.EMPTY_LITERAL_LENGTH) {
            return (CharType) CharType.ofEmptyLiteral().copy(nullable);
        } else {
            return new CharType(nullable, length);
        }
    }

    private VarCharType deserializeVarCharType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        int length = logicalTypeNode.get(FIELD_NAME_LENGTH).asInt();
        if (length == VarCharType.EMPTY_LITERAL_LENGTH) {
            return (VarCharType) VarCharType.ofEmptyLiteral().copy(nullable);
        } else {
            return new VarCharType(nullable, length);
        }
    }

    private BinaryType deserializeBinaryType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        int length = logicalTypeNode.get(FIELD_NAME_LENGTH).asInt();
        if (length == BinaryType.EMPTY_LITERAL_LENGTH) {
            return (BinaryType) BinaryType.ofEmptyLiteral().copy(nullable);
        } else {
            return new BinaryType(nullable, length);
        }
    }

    private VarBinaryType deserializeVarBinaryType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        int length = logicalTypeNode.get(FIELD_NAME_LENGTH).asInt();
        if (length == VarBinaryType.EMPTY_LITERAL_LENGTH) {
            return (VarBinaryType) VarBinaryType.ofEmptyLiteral().copy(nullable);
        } else {
            return new VarBinaryType(nullable, length);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private SymbolType<?> deserializeSymbolType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        String className = logicalTypeNode.get(FIELD_NAME_SYMBOL_CLASS).asText();
        try {
            Class<?> clazz = Class.forName(className);
            return new SymbolType(nullable, clazz);
        } catch (ClassNotFoundException e) {
            throw new TableException("Failed to deserialize symbol type", e);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private TypeInformationRawType<?> deserializeTypeInformationRawType(
            JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        String typeInfoString = logicalTypeNode.get(FIELD_NAME_TYPE_INFO).asText();
        final TypeInformation typeInfo =
                EncodingUtils.decodeStringToObject(
                        typeInfoString, TypeInformation.class, serdeCtx.getClassLoader());
        return new TypeInformationRawType(nullable, typeInfo);
    }

    private StructuredType deserializeStructuredType(
            JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        StructuredType.Builder builder;
        final ObjectIdentifier identifier;
        if (logicalTypeNode.get(FIELD_NAME_IDENTIFIER) != null) {
            JsonNode identifierNode = logicalTypeNode.get(FIELD_NAME_IDENTIFIER);
            identifier = ObjectIdentifierJsonDeserializer.deserialize(identifierNode);
        } else {
            identifier = null;
        }
        final Class<?> implementationClass;
        if (logicalTypeNode.get(FIELD_NAME_IMPLEMENTATION_CLASS) != null) {
            String classString = logicalTypeNode.get(FIELD_NAME_IMPLEMENTATION_CLASS).asText();
            try {
                implementationClass = Class.forName(classString, true, serdeCtx.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new TableException(classString + " is not found.");
            }
        } else {
            implementationClass = null;
        }
        if (identifier != null && implementationClass != null) {
            builder = StructuredType.newBuilder(identifier, implementationClass);
        } else if (identifier != null) {
            builder = StructuredType.newBuilder(identifier);
        } else if (implementationClass != null) {
            builder = StructuredType.newBuilder(implementationClass);
        } else {
            throw new TableException("This should not happen.");
        }

        List<StructuredType.StructuredAttribute> attributes = new ArrayList<>();
        for (JsonNode attributeNode : logicalTypeNode.get(FIELD_NAME_ATTRIBUTES)) {
            String name = attributeNode.get(FIELD_NAME_NAME).asText();
            LogicalType logicalType =
                    deserialize(attributeNode.get(FIELD_NAME_LOGICAL_TYPE), serdeCtx);
            final String description;
            if (attributeNode.get(FIELD_NAME_DESCRIPTION) != null) {
                description = attributeNode.get(FIELD_NAME_DESCRIPTION).asText();
            } else {
                description = null;
            }
            attributes.add(new StructuredType.StructuredAttribute(name, logicalType, description));
        }
        builder.attributes(attributes);

        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        builder.setNullable(nullable);

        boolean isFinal = logicalTypeNode.get(FIELD_NAME_FINAL).asBoolean();
        builder.setFinal(isFinal);

        boolean isInstantiable = logicalTypeNode.get(FIELD_NAME_INSTANTIABLE).asBoolean();
        builder.setInstantiable(isInstantiable);

        StructuredType.StructuredComparision comparision =
                StructuredType.StructuredComparision.valueOf(
                        logicalTypeNode.get(FIELD_NAME_COMPARISION).asText().toUpperCase());
        builder.comparision(comparision);

        if (logicalTypeNode.get(FIELD_NAME_SUPPER_TYPE) != null) {
            StructuredType supperType =
                    deserializeStructuredType(
                            logicalTypeNode.get(FIELD_NAME_SUPPER_TYPE), serdeCtx);
            builder.superType(supperType);
        }
        if (logicalTypeNode.get(FIELD_NAME_DESCRIPTION) != null) {
            String description = logicalTypeNode.get(FIELD_NAME_DESCRIPTION).asText();
            builder.description(description);
        }
        return builder.build();
    }

    private DistinctType deserializeDistinctType(JsonNode logicalTypeNode, SerdeContext serdeCtx) {
        JsonNode identifierNode = logicalTypeNode.get(FIELD_NAME_IDENTIFIER);
        ObjectIdentifier identifier = ObjectIdentifierJsonDeserializer.deserialize(identifierNode);
        LogicalType sourceType = deserialize(logicalTypeNode.get(FIELD_NAME_SOURCE_TYPE), serdeCtx);
        DistinctType.Builder builder = DistinctType.newBuilder(identifier, sourceType);
        if (logicalTypeNode.get(FIELD_NAME_DESCRIPTION) != null) {
            String description = logicalTypeNode.get(FIELD_NAME_DESCRIPTION).asText();
            builder.description(description);
        }
        return builder.build();
    }

    private TimestampType deserializeTimestampType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        int precision = logicalTypeNode.get(FIELD_NAME_PRECISION).asInt();
        TimestampKind timestampKind =
                TimestampKind.valueOf(logicalTypeNode.get(FIELD_NAME_TIMESTAMP_KIND).asText());
        return new TimestampType(nullable, timestampKind, precision);
    }

    private ZonedTimestampType deserializeZonedTimestampType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        int precision = logicalTypeNode.get(FIELD_NAME_PRECISION).asInt();
        TimestampKind timestampKind =
                TimestampKind.valueOf(logicalTypeNode.get(FIELD_NAME_TIMESTAMP_KIND).asText());
        return new ZonedTimestampType(nullable, timestampKind, precision);
    }

    private LocalZonedTimestampType deserializeLocalZonedTimestampType(JsonNode logicalTypeNode) {
        boolean nullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
        int precision = logicalTypeNode.get(FIELD_NAME_PRECISION).asInt();
        TimestampKind timestampKind =
                TimestampKind.valueOf(logicalTypeNode.get(FIELD_NAME_TIMESTAMP_KIND).asText());
        return new LocalZonedTimestampType(nullable, timestampKind, precision);
    }
}
