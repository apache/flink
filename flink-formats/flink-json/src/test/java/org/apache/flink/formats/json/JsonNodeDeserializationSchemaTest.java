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
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JsonNodeDeserializationSchema}. */
@SuppressWarnings("deprecation")
class JsonNodeDeserializationSchemaTest {

    @Test
    void testDeserialize() throws IOException {
        ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("key", 4).put("value", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        JsonNodeDeserializationSchema schema = new JsonNodeDeserializationSchema();
        schema.open(new DummyInitializationContext());
        ObjectNode deserializedValue = schema.deserialize(serializedValue);

        assertThat(deserializedValue.get("key").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("value").asText()).isEqualTo("world");
    }
}
