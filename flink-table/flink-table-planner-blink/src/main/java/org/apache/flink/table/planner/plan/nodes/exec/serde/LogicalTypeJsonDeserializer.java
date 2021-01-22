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
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_ATTRIBUTES;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_COMPARISION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_DESCRIPTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_FINAL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_IMPLEMENTATION_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_INSTANTIABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_LOGICAL_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_NULLABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SOURCE_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SUPPER_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_SYMBOL_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_TYPE_INFO;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer.FIELD_NAME_TYPE_NAME;

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
        if (logicalTypeNode.get(FIELD_NAME_SYMBOL_CLASS) != null) {
            return deserializeSymbolType(logicalTypeNode);
        } else if (logicalTypeNode.get(FIELD_NAME_TYPE_INFO) != null) {
            return deserializeTypeInformationRawType(logicalTypeNode, serdeCtx);
        } else if (logicalTypeNode.get(FIELD_NAME_TYPE_NAME) != null
                && logicalTypeNode
                        .get(FIELD_NAME_TYPE_NAME)
                        .asText()
                        .toUpperCase()
                        .equals(LogicalTypeRoot.STRUCTURED_TYPE.name())) {
            return deserializeStructuredType(logicalTypeNode, serdeCtx);
        } else if (logicalTypeNode.get(FIELD_NAME_TYPE_NAME) != null
                && logicalTypeNode
                        .get(FIELD_NAME_TYPE_NAME)
                        .asText()
                        .toUpperCase()
                        .equals(LogicalTypeRoot.DISTINCT_TYPE.name())) {
            return deserializeDistinctType(logicalTypeNode, serdeCtx);
        } else {
            return LogicalTypeParser.parse(logicalTypeNode.asText());
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
}
