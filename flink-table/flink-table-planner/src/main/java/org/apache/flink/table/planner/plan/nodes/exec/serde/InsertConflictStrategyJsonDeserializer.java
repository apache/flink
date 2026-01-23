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
import org.apache.flink.table.api.InsertConflictStrategy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * JSON deserializer for {@link InsertConflictStrategy}.
 *
 * @see InsertConflictStrategyJsonSerializer for the reverse operation
 */
@Internal
final class InsertConflictStrategyJsonDeserializer extends StdDeserializer<InsertConflictStrategy> {
    private static final long serialVersionUID = 1L;

    InsertConflictStrategyJsonDeserializer() {
        super(InsertConflictStrategy.class);
    }

    @Override
    public InsertConflictStrategy deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        JsonNode node = jsonParser.readValueAsTree();
        String behaviorName = node.get("behavior").asText();
        InsertConflictStrategy.ConflictBehavior behavior =
                InsertConflictStrategy.ConflictBehavior.valueOf(behaviorName);
        switch (behavior) {
            case ERROR:
                return InsertConflictStrategy.error();
            case NOTHING:
                return InsertConflictStrategy.nothing();
            case DEDUPLICATE:
                return InsertConflictStrategy.deduplicate();
            default:
                return InsertConflictStrategy.newBuilder().withBehavior(behavior).build();
        }
    }
}
