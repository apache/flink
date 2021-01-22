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
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.HashShuffle;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.Shuffle;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/** JSON serializer for {@link Shuffle}. */
public class ShuffleJsonSerializer extends StdSerializer<Shuffle> {
    private static final long serialVersionUID = 1L;

    public ShuffleJsonSerializer() {
        super(Shuffle.class);
    }

    @Override
    public void serialize(
            Shuffle shuffle, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        Shuffle.Type type = shuffle.getType();
        jsonGenerator.writeStringField("type", type.name());
        switch (type) {
            case ANY:
            case SINGLETON:
            case BROADCAST:
            case FORWARD:
                // do nothing, type name is enough
                break;
            case HASH:
                HashShuffle hashShuffle = (HashShuffle) shuffle;
                jsonGenerator.writeFieldName("keys");
                jsonGenerator.writeArray(
                        hashShuffle.getKeys(),
                        0, // offset
                        hashShuffle.getKeys().length);
                break;
            default:
                throw new TableException("Unsupported shuffle type: " + type);
        }
        jsonGenerator.writeEndObject();
    }
}
