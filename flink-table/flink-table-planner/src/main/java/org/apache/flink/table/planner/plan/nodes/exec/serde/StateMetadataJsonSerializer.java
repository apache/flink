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
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.StateMetadata.FIELD_NAME_STATE_INDEX;
import static org.apache.flink.table.planner.plan.nodes.exec.StateMetadata.FIELD_NAME_STATE_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.StateMetadata.FIELD_NAME_STATE_TTL;

/**
 * JSON serializer for {@link StateMetadata}.
 *
 * @see StateMetadataJsonDeserializer for the reverse operation.
 */
@Internal
public class StateMetadataJsonSerializer extends StdSerializer<StateMetadata> {

    private static final long serialVersionUID = 1L;

    StateMetadataJsonSerializer() {
        super(StateMetadata.class);
    }

    @Override
    public void serialize(
            StateMetadata stateMetadata,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField(FIELD_NAME_STATE_INDEX, stateMetadata.getStateIndex());
        jsonGenerator.writeStringField(FIELD_NAME_STATE_TTL, stateMetadata.getStateTtl() + " ms");
        jsonGenerator.writeStringField(FIELD_NAME_STATE_NAME, stateMetadata.getStateName());
        jsonGenerator.writeEndObject();
    }
}
