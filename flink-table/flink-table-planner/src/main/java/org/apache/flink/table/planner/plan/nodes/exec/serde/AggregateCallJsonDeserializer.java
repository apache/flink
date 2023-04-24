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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_APPROXIMATE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_ARG_LIST;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_DISTINCT;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_FILTER_ARG;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_IGNORE_NULLS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_TYPE;

/**
 * JSON deserializer for {@link AggregateCall}.
 *
 * @see AggregateCallJsonSerializer for the reverse operation
 */
@Internal
final class AggregateCallJsonDeserializer extends StdDeserializer<AggregateCall> {
    private static final long serialVersionUID = 1L;

    AggregateCallJsonDeserializer() {
        super(AggregateCall.class);
    }

    @Override
    public AggregateCall deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final JsonNode jsonNode = jsonParser.readValueAsTree();
        final SerdeContext serdeContext = SerdeContext.get(ctx);

        JsonNode nameNode = jsonNode.required(FIELD_NAME_NAME);
        final String name = nameNode.getNodeType() == JsonNodeType.NULL ? null : nameNode.asText();
        final SqlAggFunction aggFunction =
                (SqlAggFunction)
                        RexNodeJsonDeserializer.deserializeSqlOperator(jsonNode, serdeContext);
        final List<Integer> argList = new ArrayList<>();
        final JsonNode argListNode = jsonNode.required(FIELD_NAME_ARG_LIST);
        for (JsonNode argNode : argListNode) {
            argList.add(argNode.intValue());
        }
        final int filterArg = jsonNode.required(FIELD_NAME_FILTER_ARG).asInt();
        final boolean distinct = jsonNode.required(FIELD_NAME_DISTINCT).asBoolean();
        final boolean approximate = jsonNode.required(FIELD_NAME_APPROXIMATE).asBoolean();
        final boolean ignoreNulls = jsonNode.required(FIELD_NAME_IGNORE_NULLS).asBoolean();
        final RelDataType relDataType =
                RelDataTypeJsonDeserializer.deserialize(
                        jsonNode.required(FIELD_NAME_TYPE), serdeContext);

        return AggregateCall.create(
                aggFunction,
                distinct,
                approximate,
                ignoreNulls,
                argList,
                filterArg,
                null,
                RelCollations.EMPTY,
                relDataType,
                name);
    }
}
