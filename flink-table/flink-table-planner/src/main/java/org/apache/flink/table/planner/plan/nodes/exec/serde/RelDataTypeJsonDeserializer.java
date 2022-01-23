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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import org.apache.calcite.rel.type.RelDataType;

import java.io.IOException;

/**
 * JSON deserializer for {@link RelDataType}.
 *
 * @see RelDataTypeJsonSerializer for the reverse operation
 */
@Internal
public class RelDataTypeJsonDeserializer extends StdDeserializer<RelDataType> {
    private static final long serialVersionUID = 1L;

    public RelDataTypeJsonDeserializer() {
        super(RelDataType.class);
    }

    @Override
    public RelDataType deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final JsonNode logicalTypeNode = jsonParser.readValueAsTree();
        final SerdeContext serdeContext = SerdeContext.get(ctx);
        final FlinkTypeFactory typeFactory = serdeContext.getTypeFactory();
        final LogicalType logicalType =
                LogicalTypeJsonDeserializer.deserialize(logicalTypeNode, serdeContext);
        return LogicalRelDataTypeConverter.toRelDataType(logicalType, typeFactory);
    }
}
