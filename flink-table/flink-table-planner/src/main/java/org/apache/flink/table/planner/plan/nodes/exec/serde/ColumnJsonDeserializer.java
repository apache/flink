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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.COMMENT;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.DATA_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.EXPRESSION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.IS_VIRTUAL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.KIND_COMPUTED;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.KIND_METADATA;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.KIND_PHYSICAL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.METADATA_KEY;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ColumnJsonSerializer.NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.deserializeOptionalField;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.traverse;

/**
 * JSON deserializer for {@link Column}.
 *
 * @see ColumnJsonSerializer for the reverse operation
 */
@Internal
final class ColumnJsonDeserializer extends StdDeserializer<Column> {

    private static final String SUPPORTED_KINDS =
            Arrays.toString(new String[] {KIND_PHYSICAL, KIND_COMPUTED, KIND_METADATA});

    ColumnJsonDeserializer() {
        super(Column.class);
    }

    @Override
    public Column deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        ObjectNode jsonNode = jsonParser.readValueAsTree();
        String columnName = jsonNode.required(NAME).asText();
        String columnKind =
                Optional.ofNullable(jsonNode.get(KIND)).map(JsonNode::asText).orElse(KIND_PHYSICAL);

        Column column;
        switch (columnKind) {
            case KIND_PHYSICAL:
                column =
                        deserializePhysicalColumn(columnName, jsonNode, jsonParser.getCodec(), ctx);
                break;
            case KIND_COMPUTED:
                column =
                        deserializeComputedColumn(columnName, jsonNode, jsonParser.getCodec(), ctx);
                break;
            case KIND_METADATA:
                column =
                        deserializeMetadataColumn(columnName, jsonNode, jsonParser.getCodec(), ctx);
                break;
            default:
                throw new ValidationException(
                        String.format(
                                "Cannot recognize column type '%s'. Allowed types: %s.",
                                columnKind, SUPPORTED_KINDS));
        }
        return column.withComment(
                deserializeOptionalField(
                                jsonNode, COMMENT, String.class, jsonParser.getCodec(), ctx)
                        .orElse(null));
    }

    private static Column.PhysicalColumn deserializePhysicalColumn(
            String columnName, ObjectNode jsonNode, ObjectCodec codec, DeserializationContext ctx)
            throws IOException {
        return Column.physical(
                columnName,
                ctx.readValue(traverse(jsonNode.required(DATA_TYPE), codec), DataType.class));
    }

    private static Column.ComputedColumn deserializeComputedColumn(
            String columnName, ObjectNode jsonNode, ObjectCodec codec, DeserializationContext ctx)
            throws IOException {
        return Column.computed(
                columnName,
                ctx.readValue(
                        traverse(jsonNode.required(EXPRESSION), codec), ResolvedExpression.class));
    }

    private static Column.MetadataColumn deserializeMetadataColumn(
            String columnName, ObjectNode jsonNode, ObjectCodec codec, DeserializationContext ctx)
            throws IOException {
        return Column.metadata(
                columnName,
                ctx.readValue(traverse(jsonNode.required(DATA_TYPE), codec), DataType.class),
                deserializeOptionalField(jsonNode, METADATA_KEY, String.class, codec, ctx)
                        .orElse(null),
                jsonNode.required(IS_VIRTUAL).asBoolean());
    }
}
