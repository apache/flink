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

import org.apache.flink.connector.testutils.formats.DummyInitializationContext;
import org.apache.flink.connector.testutils.source.deserialization.TestingDeserializationContext;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for KafkaRecordDeserializationSchema. */
public class KafkaRecordDeserializationSchemaTest {

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

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
    public void testKafkaDeserializationSchemaWrapper() throws Exception {
        final ConsumerRecord<byte[], byte[]> consumerRecord = getConsumerRecord();
        KafkaRecordDeserializationSchema<ObjectNode> schema =
                KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true));
        schema.open(new DummyInitializationContext());
        SimpleCollector<ObjectNode> collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertThat(collector.list).hasSize(1);
        ObjectNode deserializedValue = collector.list.get(0);

        assertThat(deserializedValue.get("key").get("index").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("value").get("word").asText()).isEqualTo("world");
        assertThat(deserializedValue.get("metadata").get("topic").asText()).isEqualTo("topic#1");
        assertThat(deserializedValue.get("metadata").get("offset").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("metadata").get("partition").asInt()).isEqualTo(3);
    }

    @Test
    public void testKafkaValueDeserializationSchemaWrapper() throws Exception {
        final ConsumerRecord<byte[], byte[]> consumerRecord = getConsumerRecord();
        KafkaRecordDeserializationSchema<ObjectNode> schema =
                KafkaRecordDeserializationSchema.valueOnly(
                        new JsonDeserializationSchema<>(ObjectNode.class));
        schema.open(new DummyInitializationContext());
        SimpleCollector<ObjectNode> collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertThat(collector.list).hasSize(1);
        ObjectNode deserializedValue = collector.list.get(0);

        assertThat(deserializedValue.get("word").asText()).isEqualTo("world");
        assertThat(deserializedValue.get("key")).isNull();
        assertThat(deserializedValue.get("metadata")).isNull();
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

        assertThat(collector.list).hasSize(1);
        assertThat(collector.list.get(0)).isEqualTo("world");
    }

    @Test
    public void testKafkaValueDeserializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = ImmutableMap.of("simpleKey", "simpleValue");
        KafkaRecordDeserializationSchema<String> schema =
                KafkaRecordDeserializationSchema.valueOnly(SimpleStringSerializer.class, config);
        schema.open(new TestingDeserializationContext());
        assertThat(config).isEqualTo(configuration);
        assertThat(isKeyDeserializer).isFalse();
        assertThat(configurableConfiguration).isEmpty();
    }

    @Test
    public void testKafkaValueDeserializerWrapperWithConfigurable() throws Exception {
        final Map<String, String> config = ImmutableMap.of("configKey", "configValue");
        KafkaRecordDeserializationSchema<String> schema =
                KafkaRecordDeserializationSchema.valueOnly(
                        ConfigurableStringSerializer.class, config);
        schema.open(new TestingDeserializationContext());
        assertThat(config).isEqualTo(configurableConfiguration);
        assertThat(isKeyDeserializer).isFalse();
        assertThat(configuration).isEmpty();
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecord() throws JsonProcessingException {
        ObjectNode initialKey = OBJECT_MAPPER.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = OBJECT_MAPPER.writeValueAsBytes(initialKey);

        ObjectNode initialValue = OBJECT_MAPPER.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = OBJECT_MAPPER.writeValueAsBytes(initialValue);

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
