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

package org.apache.flink.streaming.examples.statemachine.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.examples.statemachine.event.Event;
import org.apache.flink.streaming.examples.statemachine.event.EventType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** A serializer and deserializer for the {@link Event} type. */
public class EventDeSerializer implements DeserializationSchema<Event>, SerializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(Event evt) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(0, evt.sourceAddress());
        byteBuffer.putInt(4, evt.type().ordinal());
        return byteBuffer.array();
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
        int address = buffer.getInt(0);
        int typeOrdinal = buffer.getInt(4);
        return new Event(EventType.values()[typeOrdinal], address);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
