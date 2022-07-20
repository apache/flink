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
import org.apache.flink.table.runtime.groupwindow.WindowReference;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.WindowReferenceJsonSerializer.FIELD_NAME_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.WindowReferenceJsonSerializer.FIELD_NAME_TYPE;

/**
 * JSON deserializer for {@link WindowReference}.
 *
 * @see WindowReferenceJsonSerializer for the reverse operation
 * @deprecated Use for legacy windows.
 */
@Deprecated
@Internal
final class WindowReferenceJsonDeserializer extends StdDeserializer<WindowReference> {
    private static final long serialVersionUID = 1L;

    WindowReferenceJsonDeserializer() {
        super(WindowReference.class);
    }

    @Override
    public WindowReference deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        final JsonNode input = jsonParser.readValueAsTree();
        String name = input.get(FIELD_NAME_NAME).asText();
        LogicalType type =
                deserializationContext.readValue(
                        input.get(FIELD_NAME_TYPE).traverse(jsonParser.getCodec()),
                        LogicalType.class);
        return new WindowReference(name, type);
    }
}
