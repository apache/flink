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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.FIELD_NAME_IS_FOLLOWING;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.FIELD_NAME_IS_PRECEDING;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.FIELD_NAME_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.FIELD_NAME_OFFSET;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.KIND_BOUNDED_WINDOW;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.KIND_CURRENT_ROW;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.KIND_UNBOUNDED_FOLLOWING;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer.KIND_UNBOUNDED_PRECEDING;

/**
 * JSON deserializer for {@link RexWindowBound}. refer to {@link RexWindowBoundJsonSerializer} for
 * serializer.
 */
public class RexWindowBoundJsonDeserializer extends StdDeserializer<RexWindowBound> {

    public RexWindowBoundJsonDeserializer() {
        super(RexWindowBound.class);
    }

    @Override
    public RexWindowBound deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        JsonNode jsonNode = jsonParser.readValueAsTree();
        String kind = jsonNode.get(FIELD_NAME_KIND).asText().toUpperCase();
        switch (kind) {
            case KIND_CURRENT_ROW:
                return RexWindowBounds.CURRENT_ROW;
            case KIND_UNBOUNDED_FOLLOWING:
                return RexWindowBounds.UNBOUNDED_FOLLOWING;
            case KIND_UNBOUNDED_PRECEDING:
                return RexWindowBounds.UNBOUNDED_PRECEDING;
            case KIND_BOUNDED_WINDOW:
                FlinkDeserializationContext flinkDeserializationContext =
                        (FlinkDeserializationContext) deserializationContext;
                RexNode offset = null;
                if (jsonNode.get(FIELD_NAME_OFFSET) != null) {
                    offset =
                            flinkDeserializationContext
                                    .getObjectMapper()
                                    .readValue(
                                            jsonNode.get(FIELD_NAME_OFFSET).toString(),
                                            RexNode.class);
                }
                if (offset != null && jsonNode.get(FIELD_NAME_IS_FOLLOWING) != null) {
                    return RexWindowBounds.following(offset);
                } else if (offset != null && jsonNode.get(FIELD_NAME_IS_PRECEDING) != null) {
                    return RexWindowBounds.preceding(offset);
                } else {
                    throw new TableException("Unknown RexWindowBound: " + jsonNode.toString());
                }
            default:
                throw new TableException("Unknown RexWindowBound: " + jsonNode.toString());
        }
    }
}
