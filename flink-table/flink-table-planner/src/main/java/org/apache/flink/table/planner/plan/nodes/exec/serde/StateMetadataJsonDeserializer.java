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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.StateMetadata.FIELD_NAME_STATE_INDEX;
import static org.apache.flink.table.planner.plan.nodes.exec.StateMetadata.FIELD_NAME_STATE_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.StateMetadata.FIELD_NAME_STATE_TTL;

/**
 * JSON deserializer for {@link StateMetadata}.
 *
 * @see StateMetadataJsonSerializer for the reverse operation.
 */
@Internal
public class StateMetadataJsonDeserializer extends StdDeserializer<StateMetadata> {

    private static final long serialVersionUID = 1L;

    @VisibleForTesting static final String DEFAULT_STATE_NAME = "_defaultState";

    StateMetadataJsonDeserializer() {
        super(StateMetadata.class);
    }

    @Override
    public StateMetadata deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final JsonNode jsonNode = jsonParser.readValueAsTree();
        try {
            // stateIndex and stateTtl are mandatory
            final int stateIndex = jsonNode.required(FIELD_NAME_STATE_INDEX).asInt();
            final long stateTtl =
                    Long.parseLong(
                            jsonNode.required(FIELD_NAME_STATE_TTL).asText().split("ms")[0].trim());
            final String stateName =
                    jsonNode.hasNonNull(FIELD_NAME_STATE_NAME)
                            ? jsonNode.get(FIELD_NAME_STATE_NAME).asText()
                            : DEFAULT_STATE_NAME;
            return new StateMetadata(stateIndex, stateTtl, stateName);
        } catch (IllegalArgumentException e) {
            throw new TableException(
                    String.format("Cannot deserialize state metadata. %s", e.getMessage()), e);
        }
    }
}
