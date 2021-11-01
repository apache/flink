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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link KafkaRecordSerializationSchemaBuilder}. */
public class KafkaRecordSerializationSchemaBuilderTest extends TestLogger {

    private static final String DEFAULT_TOPIC = "test";

    private static Map<String, ?> configuration;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
    }

    @Test
    public void testDoNotAllowMultipleKeySerializer() {
        assertOnlyOneSerializerAllowed(keySerializationSetter());
    }

    @Test
    public void testDoNotAllowMultipleValueSerializer() {
        assertOnlyOneSerializerAllowed(valueSerializationSetter());
    }

    @Test
    public void testDoNotAllowMultipleTopicSelector() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        KafkaRecordSerializationSchema.builder()
                                .setTopicSelector(e -> DEFAULT_TOPIC)
                                .setTopic(DEFAULT_TOPIC));
        assertThrows(
                IllegalStateException.class,
                () ->
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(DEFAULT_TOPIC)
                                .setTopicSelector(e -> DEFAULT_TOPIC));
    }

    @Test
    public void testExpectTopicSelector() {
        assertThrows(
                IllegalStateException.class,
                KafkaRecordSerializationSchema.builder()
                                .setValueSerializationSchema(new SimpleStringSchema())
                        ::build);
    }

    @Test
    public void testExpectValueSerializer() {
        assertThrows(
                IllegalStateException.class,
                KafkaRecordSerializationSchema.builder().setTopic(DEFAULT_TOPIC)::build);
    }

    @Test
    public void testSerializeRecordWithTopicSelector() {
        final TopicSelector<String> topicSelector =
                (e) -> {
                    if (e.equals("a")) {
                        return "topic-a";
                    }
                    return "topic-b";
                };
        final KafkaRecordSerializationSchemaBuilder<String> builder =
                KafkaRecordSerializationSchema.builder().setTopicSelector(topicSelector);
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final KafkaRecordSerializationSchema<String> schema =
                builder.setValueSerializationSchema(serializationSchema).build();
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", null, null);
        assertEquals("topic-a", record.topic());
        assertNull(record.key());
        assertArrayEquals(serializationSchema.serialize("a"), record.value());

        final ProducerRecord<byte[], byte[]> record2 = schema.serialize("b", null, null);
        assertEquals("topic-b", record2.topic());
        assertNull(record2.key());
        assertArrayEquals(serializationSchema.serialize("b"), record2.value());
    }

    @Test
    public void testSerializeRecordWithPartitioner() throws Exception {
        AtomicBoolean opened = new AtomicBoolean(false);
        final int partition = 5;
        final FlinkKafkaPartitioner<Object> partitioner =
                new ConstantPartitioner<>(opened, partition);
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(partitioner)
                        .build();
        final KafkaRecordSerializationSchema.KafkaSinkContext sinkContext = new TestSinkContext();
        schema.open(null, sinkContext);
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", sinkContext, null);
        assertEquals(partition, record.partition());
        assertTrue(opened.get());
    }

    @Test
    public void testSerializeRecordWithKey() {
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", null, null);
        assertArrayEquals(record.key(), serializationSchema.serialize("a"));
        assertArrayEquals(record.value(), serializationSchema.serialize("a"));
    }

    @Test
    public void testSerializeRecordWithKafkaSerializer() throws Exception {
        final Map<String, String> config = ImmutableMap.of("configKey", "configValue");
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setKafkaValueSerializer(ConfigurableStringSerializer.class, config)
                        .build();
        schema.open(
                new SerializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return null;
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return new UserCodeClassLoader() {
                            @Override
                            public ClassLoader asClassLoader() {
                                return KafkaRecordSerializationSchemaBuilderTest.class
                                        .getClassLoader();
                            }

                            @Override
                            public void registerReleaseHookIfAbsent(
                                    String releaseHookName, Runnable releaseHook) {}
                        };
                    }
                },
                null);
        assertEquals(configuration, config);
        final Deserializer<String> deserializer = new StringDeserializer();
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", null, null);
        assertEquals("a", deserializer.deserialize(DEFAULT_TOPIC, record.value()));
    }

    @Test
    public void testSerializeRecordWithTimestamp() {
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();
        final ProducerRecord<byte[], byte[]> recordWithTimestamp =
                schema.serialize("a", null, 100L);
        assertEquals(100L, (long) recordWithTimestamp.timestamp());

        final ProducerRecord<byte[], byte[]> recordWithTimestampZero =
                schema.serialize("a", null, 0L);
        assertEquals(0L, (long) recordWithTimestampZero.timestamp());

        final ProducerRecord<byte[], byte[]> recordWithoutTimestamp =
                schema.serialize("a", null, null);
        assertNull(recordWithoutTimestamp.timestamp());

        final ProducerRecord<byte[], byte[]> recordWithInvalidTimestamp =
                schema.serialize("a", null, -100L);
        assertNull(recordWithInvalidTimestamp.timestamp());
    }

    private static void assertOnlyOneSerializerAllowed(
            List<
                            Function<
                                    KafkaRecordSerializationSchemaBuilder<String>,
                                    KafkaRecordSerializationSchemaBuilder<String>>>
                    serializers) {
        for (final Function<
                        KafkaRecordSerializationSchemaBuilder<String>,
                        KafkaRecordSerializationSchemaBuilder<String>>
                setter : serializers) {
            final KafkaRecordSerializationSchemaBuilder<String> builder =
                    KafkaRecordSerializationSchema.<String>builder().setTopic(DEFAULT_TOPIC);
            setter.apply(builder);
            for (final Function<
                            KafkaRecordSerializationSchemaBuilder<String>,
                            KafkaRecordSerializationSchemaBuilder<String>>
                    updater : serializers) {
                assertThrows(IllegalStateException.class, () -> updater.apply(builder));
            }
        }
    }

    private static List<
                    Function<
                            KafkaRecordSerializationSchemaBuilder<String>,
                            KafkaRecordSerializationSchemaBuilder<String>>>
            valueSerializationSetter() {
        return ImmutableList.of(
                (b) -> b.setKafkaValueSerializer(StringSerializer.class),
                (b) -> b.setValueSerializationSchema(new SimpleStringSchema()),
                (b) ->
                        b.setKafkaValueSerializer(
                                ConfigurableStringSerializer.class, Collections.emptyMap()));
    }

    private static List<
                    Function<
                            KafkaRecordSerializationSchemaBuilder<String>,
                            KafkaRecordSerializationSchemaBuilder<String>>>
            keySerializationSetter() {
        return ImmutableList.of(
                (b) -> b.setKafkaKeySerializer(StringSerializer.class),
                (b) -> b.setKeySerializationSchema(new SimpleStringSchema()),
                (b) ->
                        b.setKafkaKeySerializer(
                                ConfigurableStringSerializer.class, Collections.emptyMap()));
    }

    /**
     * Serializer based on Kafka's serialization stack.
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class ConfigurableStringSerializer extends StringSerializer
            implements Configurable {
        @Override
        public void configure(Map<String, ?> configs) {
            configuration = configs;
        }
    }

    private static class TestSinkContext
            implements KafkaRecordSerializationSchema.KafkaSinkContext {
        @Override
        public int getParallelInstanceId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelInstances() {
            return 0;
        }

        @Override
        public int[] getPartitionsForTopic(String topic) {
            return new int[0];
        }
    }

    private static class ConstantPartitioner<T> extends FlinkKafkaPartitioner<T> {

        private final AtomicBoolean opened;
        private final int partition;

        ConstantPartitioner(AtomicBoolean opened, int partition) {
            this.opened = opened;
            this.partition = partition;
        }

        @Override
        public void open(int parallelInstanceId, int parallelInstances) {
            opened.set(true);
        }

        @Override
        public int partition(
                T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return partition;
        }
    }
}
