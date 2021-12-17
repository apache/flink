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
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.Shuffle;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/** JSON deserializer for {@link Shuffle}. */
public class ShuffleJsonDeserializer extends StdDeserializer<Shuffle> {
    private static final long serialVersionUID = 1L;

    public ShuffleJsonDeserializer() {
        super(Shuffle.class);
    }

    @Override
    public Shuffle deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException, JsonProcessingException {
        JsonNode jsonNode = jsonParser.getCodec().readTree(jsonParser);
        Shuffle.Type type = Shuffle.Type.valueOf(jsonNode.get("type").asText().toUpperCase());
        switch (type) {
            case ANY:
                return ExecEdge.ANY_SHUFFLE;
            case SINGLETON:
                return ExecEdge.SINGLETON_SHUFFLE;
            case BROADCAST:
                return ExecEdge.BROADCAST_SHUFFLE;
            case FORWARD:
                return ExecEdge.FORWARD_SHUFFLE;
            case HASH:
                JsonNode keysNode = jsonNode.get("keys");
                if (keysNode == null || keysNode.size() == 0) {
                    throw new TableException("Hash shuffle requires non-empty hash keys.");
                }
                int[] keys = new int[keysNode.size()];
                for (int i = 0; i < keysNode.size(); ++i) {
                    keys[i] = keysNode.get(i).asInt();
                }
                return ExecEdge.hashShuffle(keys);
            default:
                throw new TableException("Unsupported shuffle type: " + type);
        }
    }
}
