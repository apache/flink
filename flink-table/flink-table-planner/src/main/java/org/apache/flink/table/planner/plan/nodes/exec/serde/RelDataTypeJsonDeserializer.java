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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_ELEMENT;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_FIELDS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_FILED_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_KEY;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_NULLABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_PRECISION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_RAW_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_SCALE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_STRUCTURED_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_STRUCT_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_TIMESTAMP_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_TYPE_INFO;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_TYPE_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RelDataTypeJsonSerializer.FIELD_NAME_VALUE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * JSON deserializer for {@link RelDataType}. refer to {@link RelDataTypeJsonSerializer} for
 * serializer.
 */
public class RelDataTypeJsonDeserializer extends StdDeserializer<RelDataType> {
    private static final long serialVersionUID = 1L;

    public RelDataTypeJsonDeserializer() {
        super(RelDataType.class);
    }

    @Override
    public RelDataType deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException, JsonProcessingException {
        JsonNode jsonNode = jsonParser.readValueAsTree();
        return deserialize(jsonNode, ((FlinkDeserializationContext) ctx));
    }

    private RelDataType deserialize(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws JsonProcessingException {
        FlinkTypeFactory typeFactory = ctx.getSerdeContext().getTypeFactory();
        if (jsonNode instanceof ObjectNode) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            if (objectNode.has(FIELD_NAME_TIMESTAMP_KIND)) {
                boolean nullable = objectNode.get(FIELD_NAME_NULLABLE).booleanValue();
                String typeName = objectNode.get(FIELD_NAME_TYPE_NAME).textValue();
                boolean isTimestampLtz =
                        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name().equals(typeName);
                TimestampKind timestampKind =
                        TimestampKind.valueOf(
                                objectNode.get(FIELD_NAME_TIMESTAMP_KIND).asText().toUpperCase());
                switch (timestampKind) {
                    case ROWTIME:
                        return typeFactory.createRowtimeIndicatorType(nullable, isTimestampLtz);
                    case PROCTIME:
                        return typeFactory.createProctimeIndicatorType(nullable);
                    default:
                        throw new TableException(timestampKind + " is not supported.");
                }
            } else if (objectNode.has(FIELD_NAME_STRUCTURED_TYPE)) {
                JsonNode structuredTypeNode = objectNode.get(FIELD_NAME_STRUCTURED_TYPE);
                LogicalType structuredType =
                        ctx.getObjectMapper()
                                .readValue(structuredTypeNode.toPrettyString(), LogicalType.class);
                checkArgument(structuredType instanceof StructuredType);
                return ctx.getSerdeContext()
                        .getTypeFactory()
                        .createFieldTypeFromLogicalType(structuredType);
            } else if (objectNode.has(FIELD_NAME_STRUCT_KIND)) {
                ArrayNode arrayNode = (ArrayNode) objectNode.get(FIELD_NAME_FIELDS);
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                for (JsonNode node : arrayNode) {
                    builder.add(node.get(FIELD_NAME_FILED_NAME).asText(), deserialize(node, ctx));
                }
                StructKind structKind =
                        StructKind.valueOf(
                                objectNode.get(FIELD_NAME_STRUCT_KIND).asText().toUpperCase());
                boolean nullable = objectNode.get(FIELD_NAME_NULLABLE).booleanValue();
                return builder.kind(structKind).nullableRecord(nullable).build();
            } else if (objectNode.has(FIELD_NAME_FIELDS)) {
                JsonNode fields = objectNode.get(FIELD_NAME_FIELDS);
                // Nested struct
                return deserialize(fields, ctx);
            } else {
                SqlTypeName sqlTypeName =
                        Util.enumVal(
                                SqlTypeName.class, objectNode.get(FIELD_NAME_TYPE_NAME).asText());
                boolean nullable = objectNode.get(FIELD_NAME_NULLABLE).booleanValue();
                if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
                    TimeUnit startUnit = sqlTypeName.getStartUnit();
                    TimeUnit endUnit = sqlTypeName.getEndUnit();
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlIntervalType(
                                    new SqlIntervalQualifier(
                                            startUnit, endUnit, SqlParserPos.ZERO)),
                            nullable);
                }
                if (sqlTypeName == SqlTypeName.OTHER && objectNode.has(FIELD_NAME_RAW_TYPE)) {
                    RawType<?> rawType =
                            (RawType<?>)
                                    LogicalTypeParser.parse(
                                            objectNode.get(FIELD_NAME_RAW_TYPE).asText(),
                                            ctx.getSerdeContext().getClassLoader());
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createFieldTypeFromLogicalType(rawType), nullable);
                }
                if (sqlTypeName == SqlTypeName.ANY && objectNode.has(FIELD_NAME_RAW_TYPE)) {
                    JsonNode rawTypeNode = objectNode.get(FIELD_NAME_RAW_TYPE);
                    boolean nullableOfTypeInfo =
                            rawTypeNode.get(FIELD_NAME_NULLABLE).booleanValue();
                    TypeInformation<?> typeInfo =
                            EncodingUtils.decodeStringToObject(
                                    rawTypeNode.get(FIELD_NAME_TYPE_INFO).asText(),
                                    TypeInformation.class,
                                    ctx.getSerdeContext().getClassLoader());
                    TypeInformationRawType<?> rawType =
                            new TypeInformationRawType<>(nullableOfTypeInfo, typeInfo);
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createFieldTypeFromLogicalType(rawType), nullable);
                }

                Integer precision =
                        objectNode.has(FIELD_NAME_PRECISION)
                                ? objectNode.get(FIELD_NAME_PRECISION).intValue()
                                : null;
                Integer scale =
                        objectNode.has(FIELD_NAME_SCALE)
                                ? objectNode.get(FIELD_NAME_SCALE).intValue()
                                : null;
                final RelDataType type;
                if (sqlTypeName == SqlTypeName.ARRAY) {
                    RelDataType elementType = deserialize(objectNode.get(FIELD_NAME_ELEMENT), ctx);
                    type = typeFactory.createArrayType(elementType, -1);
                } else if (sqlTypeName == SqlTypeName.MULTISET) {
                    RelDataType elementType = deserialize(objectNode.get(FIELD_NAME_ELEMENT), ctx);
                    type = typeFactory.createMultisetType(elementType, -1);
                } else if (sqlTypeName == SqlTypeName.MAP) {
                    RelDataType keyType = deserialize(objectNode.get(FIELD_NAME_KEY), ctx);
                    RelDataType valueType = deserialize(objectNode.get(FIELD_NAME_VALUE), ctx);
                    type = typeFactory.createMapType(keyType, valueType);
                } else if (precision == null) {
                    type = typeFactory.createSqlType(sqlTypeName);
                } else if (scale == null) {
                    type = typeFactory.createSqlType(sqlTypeName, precision);
                } else {
                    type = typeFactory.createSqlType(sqlTypeName, precision, scale);
                }
                return typeFactory.createTypeWithNullability(type, nullable);
            }
        } else if (jsonNode instanceof TextNode) {
            SqlTypeName sqlTypeName = Util.enumVal(SqlTypeName.class, jsonNode.asText());
            return typeFactory.createSqlType(sqlTypeName);
        } else {
            throw new TableException("Unknown type: " + jsonNode.toPrettyString());
        }
    }
}
