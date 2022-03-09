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

package org.apache.flink.connector.kafka.source.reader.deserializer;

import org.apache.flink.connector.testutils.source.deserialization.TestingDeserializationContext;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for KafkaRecordDeserializationSchema. */
public class KafkaRecordDeserializationSchemaTest {

    private static Map<String, ?> configurableConfiguration;
    private static Map<String, ?> configuration;
    private static boolean isKeyDeserializer;

    @Before
    public void setUp() {
        configurableConfiguration = new HashMap<>(1);
        configuration = new HashMap<>(1);
        isKeyDeserializer = false;
    }

    @Test
    public void testKafkaDeserializationSchemaWrapper() throws IOException {
        final ConsumerRecord<byte[], byte[]> consumerRecord = getConsumerRecord();
        KafkaRecordDeserializationSchema<ObjectNode> schema =
                KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true));
        SimpleCollector<ObjectNode> collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertEquals(1, collector.list.size());
        ObjectNode deserializedValue = collector.list.get(0);

        assertEquals(4, deserializedValue.get("key").get("index").asInt());
        assertEquals("world", deserializedValue.get("value").get("word").asText());
        assertEquals("topic#1", deserializedValue.get("metadata").get("topic").asText());
        assertEquals(4, deserializedValue.get("metadata").get("offset").asInt());
        assertEquals(3, deserializedValue.get("metadata").get("partition").asInt());
    }

    @Test
    public void testKafkaValueDeserializationSchemaWrapper() throws IOException {
        final ConsumerRecord<byte[], byte[]> consumerRecord = getConsumerRecord();
        KafkaRecordDeserializationSchema<ObjectNode> schema =
                KafkaRecordDeserializationSchema.valueOnly(new JsonNodeDeserializationSchema());
        SimpleCollector<ObjectNode> collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertEquals(1, collector.list.size());
        ObjectNode deserializedValue = collector.list.get(0);

        assertEquals("world", deserializedValue.get("word").asText());
        assertNull(deserializedValue.get("key"));
        assertNull(deserializedValue.get("metadata"));
    }

    @Test
    public void testKafkaValueDeserializerWrapper() throws Exception {
        final String topic = "Topic";
        byte[] value = new StringSerializer().serialize(topic, "world");
        final ConsumerRecord<byte[], byte[]> consumerRecord =
                new ConsumerRecord<>(topic, 0, 0L, null, value);
        KafkaRecordDeserializationSchema<String> schema =
                KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class);
        schema.open(new TestingDeserializationContext());

        SimpleCollector<String> collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertEquals(1, collector.list.size());
        assertEquals("world", collector.list.get(0));
    }

    @Test
    public void testKafkaValueDeserializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = ImmutableMap.of("simpleKey", "simpleValue");
        KafkaRecordDeserializationSchema<String> schema =
                KafkaRecordDeserializationSchema.valueOnly(SimpleStringSerializer.class, config);
        schema.open(new TestingDeserializationContext());
        Assertions.assertEquals(configuration, config);
        assertFalse(isKeyDeserializer);
        assertTrue(configurableConfiguration.isEmpty());
    }

    @Test
    public void testKafkaValueDeserializerWrapperWithConfigurable() throws Exception {
        final Map<String, String> config = ImmutableMap.of("configKey", "configValue");
        KafkaRecordDeserializationSchema<String> schema =
                KafkaRecordDeserializationSchema.valueOnly(
                        ConfigurableStringSerializer.class, config);
        schema.open(new TestingDeserializationContext());
        Assertions.assertEquals(configurableConfiguration, config);
        assertFalse(isKeyDeserializer);
        assertTrue(configuration.isEmpty());
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecord() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode initialKey = mapper.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        return new ConsumerRecord<>("topic#1", 3, 4L, serializedKey, serializedValue);
    }

    private static class SimpleCollector<T> implements Collector<T> {

        private final List<T> list = new ArrayList<>();

        @Override
        public void collect(T record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    /**
     * Serializer based on Kafka's serialization stack. This is the special case that implements
     * {@link Configurable}
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class ConfigurableStringSerializer extends StringDeserializer
            implements Configurable {
        @Override
        public void configure(Map<String, ?> configs) {
            configurableConfiguration = configs;
        }
    }

    /**
     * Serializer based on Kafka's serialization stack.
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class SimpleStringSerializer extends StringDeserializer {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            configuration = configs;
            isKeyDeserializer = isKey;
        }
    }
}
