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
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.planner.expressions.PlannerWindowReference;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_ALIAS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_FIELD_INDEX;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_FIELD_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_FIELD_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_GAP;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_INPUT_INDEX;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_IS_TIME_WINDOW;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_SIZE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_SLIDE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.FIELD_NAME_TIME_FIELD;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.KIND_SESSION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.KIND_SLIDING;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalWindowJsonSerializer.KIND_TUMBLING;

/**
 * JSON deserializer for {@link LogicalWindow}, refer to {@link LogicalWindowJsonSerializer} for
 * serializer.
 */
public class LogicalWindowJsonDeserializer extends StdDeserializer<LogicalWindow> {
    private static final long serialVersionUID = 1L;

    public LogicalWindowJsonDeserializer() {
        super(LogicalWindow.class);
    }

    @Override
    public LogicalWindow deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException {
        FlinkDeserializationContext flinkDeserializationContext =
                (FlinkDeserializationContext) deserializationContext;
        ObjectMapper mapper = flinkDeserializationContext.getObjectMapper();
        JsonNode jsonNode = jsonParser.readValueAsTree();
        String kind = jsonNode.get(FIELD_NAME_KIND).asText().toUpperCase();
        PlannerWindowReference alias =
                mapper.readValue(
                        jsonNode.get(FIELD_NAME_ALIAS).toString(), PlannerWindowReference.class);
        FieldReferenceExpression timeField =
                deserializeFieldReferenceExpression(jsonNode.get(FIELD_NAME_TIME_FIELD), mapper);

        switch (kind) {
            case KIND_TUMBLING:
                boolean isTimeTumblingWindow = jsonNode.get(FIELD_NAME_IS_TIME_WINDOW).asBoolean();
                if (isTimeTumblingWindow) {
                    Duration size =
                            mapper.readValue(
                                    jsonNode.get(FIELD_NAME_SIZE).toString(), Duration.class);
                    return new TumblingGroupWindow(
                            alias, timeField, new ValueLiteralExpression(size));
                } else {
                    long size = jsonNode.get(FIELD_NAME_SIZE).asLong();
                    return new TumblingGroupWindow(
                            alias, timeField, new ValueLiteralExpression(size));
                }
            case KIND_SLIDING:
                boolean isTimeSlidingWindow = jsonNode.get(FIELD_NAME_IS_TIME_WINDOW).asBoolean();
                if (isTimeSlidingWindow) {
                    Duration size =
                            mapper.readValue(
                                    jsonNode.get(FIELD_NAME_SIZE).toString(), Duration.class);
                    Duration slide =
                            mapper.readValue(
                                    jsonNode.get(FIELD_NAME_SLIDE).toString(), Duration.class);
                    return new SlidingGroupWindow(
                            alias,
                            timeField,
                            new ValueLiteralExpression(size),
                            new ValueLiteralExpression(slide));
                } else {
                    long size = jsonNode.get(FIELD_NAME_SIZE).asLong();
                    long slide = jsonNode.get(FIELD_NAME_SLIDE).asLong();
                    return new SlidingGroupWindow(
                            alias,
                            timeField,
                            new ValueLiteralExpression(size),
                            new ValueLiteralExpression(slide));
                }
            case KIND_SESSION:
                Duration gap =
                        mapper.readValue(jsonNode.get(FIELD_NAME_GAP).toString(), Duration.class);
                return new SessionGroupWindow(alias, timeField, new ValueLiteralExpression(gap));

            default:
                throw new TableException("Unknown Logical Window:" + jsonNode);
        }
    }

    private FieldReferenceExpression deserializeFieldReferenceExpression(
            JsonNode input, ObjectMapper mapper) throws JsonProcessingException {
        String name = input.get(FIELD_NAME_FIELD_NAME).asText();
        int fieldIndex = input.get(FIELD_NAME_FIELD_INDEX).asInt();
        int inputIndex = input.get(FIELD_NAME_INPUT_INDEX).asInt();
        LogicalType type =
                mapper.readValue(input.get(FIELD_NAME_FIELD_TYPE).toString(), LogicalType.class);
        return new FieldReferenceExpression(name, new AtomicDataType(type), fieldIndex, inputIndex);
    }
}
