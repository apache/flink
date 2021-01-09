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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the{@link JSONKeyValueDeserializationSchema}. */
public class JSONKeyValueDeserializationSchemaTest {

    @Test
    public void testDeserializeWithoutMetadata() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode initialKey = mapper.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        ObjectNode deserializedValue =
                schema.deserialize(newConsumerRecord(serializedKey, serializedValue));

        Assertions.assertTrue(deserializedValue.get("metadata") == null);
        Assertions.assertEquals(4, deserializedValue.get("key").get("index").asInt());
        Assertions.assertEquals("world", deserializedValue.get("value").get("word").asText());
    }

    @Test
    public void testDeserializeWithoutKey() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        byte[] serializedKey = null;

        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        ObjectNode deserializedValue =
                schema.deserialize(newConsumerRecord(serializedKey, serializedValue));

        Assertions.assertTrue(deserializedValue.get("metadata") == null);
        Assertions.assertTrue(deserializedValue.get("key") == null);
        Assertions.assertEquals("world", deserializedValue.get("value").get("word").asText());
    }

    private static ConsumerRecord<byte[], byte[]> newConsumerRecord(
            byte[] serializedKey, byte[] serializedValue) {
        return newConsumerRecord("", 0, 0L, serializedKey, serializedValue);
    }

    private static ConsumerRecord<byte[], byte[]> newConsumerRecord(
            String topic,
            int partition,
            long offset,
            byte[] serializedKey,
            byte[] serializedValue) {

        return new ConsumerRecord<>(topic, partition, offset, serializedKey, serializedValue);
    }

    @Test
    public void testDeserializeWithoutValue() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode initialKey = mapper.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

        byte[] serializedValue = null;

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        ObjectNode deserializedValue =
                schema.deserialize(newConsumerRecord(serializedKey, serializedValue));

        Assertions.assertTrue(deserializedValue.get("metadata") == null);
        Assertions.assertEquals(4, deserializedValue.get("key").get("index").asInt());
        Assertions.assertTrue(deserializedValue.get("value") == null);
    }

    @Test
    public void testDeserializeWithMetadata() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode initialKey = mapper.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(true);
        final ConsumerRecord<byte[], byte[]> consumerRecord =
                newConsumerRecord("topic#1", 3, 4L, serializedKey, serializedValue);
        ObjectNode deserializedValue = schema.deserialize(consumerRecord);

        Assertions.assertEquals(4, deserializedValue.get("key").get("index").asInt());
        Assertions.assertEquals("world", deserializedValue.get("value").get("word").asText());
        Assertions.assertEquals("topic#1", deserializedValue.get("metadata").get("topic").asText());
        Assertions.assertEquals(4, deserializedValue.get("metadata").get("offset").asInt());
        Assertions.assertEquals(3, deserializedValue.get("metadata").get("partition").asInt());
    }
}
