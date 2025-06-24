/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.json;

import org.apache.flink.connector.testutils.formats.DummyInitializationContext;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class JsonSerDeSchemaTest {
    private static final JsonSerializationSchema<Event> SERIALIZATION_SCHEMA;
    private static final JsonDeserializationSchema<Event> DESERIALIZATION_SCHEMA;
    private static final String JSON = "{\"x\":34,\"y\":\"hello\"}";

    static {
        SERIALIZATION_SCHEMA = new JsonSerializationSchema<>();
        SERIALIZATION_SCHEMA.open(new DummyInitializationContext());
        DESERIALIZATION_SCHEMA = new JsonDeserializationSchema<>(Event.class);
        DESERIALIZATION_SCHEMA.open(new DummyInitializationContext());
    }

    @Test
    void testSrialization() throws IOException {
        final byte[] serialized = SERIALIZATION_SCHEMA.serialize(new Event(34, "hello"));
        assertThat(serialized).isEqualTo(JSON.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testDeserialization() throws IOException {
        final Event deserialized =
                DESERIALIZATION_SCHEMA.deserialize(JSON.getBytes(StandardCharsets.UTF_8));
        assertThat(deserialized).isEqualTo(new Event(34, "hello"));
    }

    @Test
    void testRoundTrip() throws IOException {
        final Event original = new Event(34, "hello");

        final byte[] serialized = SERIALIZATION_SCHEMA.serialize(original);

        final Event deserialized = DESERIALIZATION_SCHEMA.deserialize(serialized);

        assertThat(deserialized).isEqualTo(original);
    }

    private static class Event {

        private int x;
        private String y = null;

        public Event() {}

        public Event(int x, String y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public void setX(int x) {
            this.x = x;
        }

        public String getY() {
            return y;
        }

        public void setY(String y) {
            this.y = y;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Event event = (Event) o;
            return x == event.x && Objects.equals(y, event.y);
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y);
        }
    }
}
