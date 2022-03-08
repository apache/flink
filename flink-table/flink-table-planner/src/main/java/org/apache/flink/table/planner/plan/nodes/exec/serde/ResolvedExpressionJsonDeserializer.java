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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.RexNodeExpression;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.calcite.rex.RexNode;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedExpressionJsonSerializer.REX_NODE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedExpressionJsonSerializer.SERIALIZABLE_STRING;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedExpressionJsonSerializer.TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedExpressionJsonSerializer.TYPE_REX_NODE_EXPRESSION;

/**
 * JSON deserializer for {@link ResolvedExpression}.
 *
 * @see ResolvedExpressionJsonSerializer for the reverse operation
 */
@Internal
final class ResolvedExpressionJsonDeserializer extends StdDeserializer<ResolvedExpression> {

    ResolvedExpressionJsonDeserializer() {
        super(ResolvedExpression.class);
    }

    @Override
    public ResolvedExpression deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        ObjectNode jsonNode = jsonParser.readValueAsTree();
        String expressionType =
                Optional.ofNullable(jsonNode.get(TYPE))
                        .map(JsonNode::asText)
                        .orElse(TYPE_REX_NODE_EXPRESSION);

        if (TYPE_REX_NODE_EXPRESSION.equals(expressionType)) {
            return deserializeRexNodeExpression(jsonNode, jsonParser.getCodec(), ctx);
        } else {
            throw new ValidationException(
                    String.format(
                            "Expression '%s' cannot be deserialized. "
                                    + "Currently, only SQL expressions can be deserialized from the persisted plan.",
                            jsonNode));
        }
    }

    private ResolvedExpression deserializeRexNodeExpression(
            ObjectNode jsonNode, ObjectCodec codec, DeserializationContext ctx) throws IOException {
        RexNode node = ctx.readValue(jsonNode.get(REX_NODE).traverse(codec), RexNode.class);
        String serializableString = jsonNode.get(SERIALIZABLE_STRING).asText();
        return new RexNodeExpression(
                node,
                DataTypes.of(FlinkTypeFactory.toLogicalType(node.getType())),
                node.toString(),
                serializableString);
    }
}
