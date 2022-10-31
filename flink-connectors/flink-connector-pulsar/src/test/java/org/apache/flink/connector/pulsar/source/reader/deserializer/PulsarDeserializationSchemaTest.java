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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.testutils.source.deserialization.TestingDeserializationContext;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link PulsarDeserializationSchema}. */
class PulsarDeserializationSchemaTest extends PulsarTestSuiteBase {

    @Test
    void createFromFlinkDeserializationSchema() throws Exception {
        PulsarDeserializationSchema<String> schema =
                new PulsarDeserializationSchemaWrapper<>(new SimpleStringSchema());
        schema.open(new TestingDeserializationContext(), mock(SourceConfiguration.class));
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema));

        Message<byte[]> message = getMessage("some-sample-message", String::getBytes);
        SingleMessageCollector<String> collector = new SingleMessageCollector<>();
        schema.deserialize(message, collector);

        assertNotNull(collector.result);
        assertEquals(collector.result, "some-sample-message");
    }

    @Test
    void createFromPulsarSchema() throws Exception {
        Schema<TestMessage> schema1 = PROTOBUF_NATIVE(TestMessage.class);
        PulsarDeserializationSchema<TestMessage> schema2 =
                new PulsarSchemaWrapper<>(schema1, TestMessage.class);
        schema2.open(new TestingDeserializationContext(), mock(SourceConfiguration.class));
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema2));

        TestMessage message1 =
                TestMessage.newBuilder()
                        .setStringField(randomAlphabetic(10))
                        .setDoubleField(ThreadLocalRandom.current().nextDouble())
                        .setIntField(ThreadLocalRandom.current().nextInt())
                        .build();
        Message<byte[]> message2 = getMessage(message1, schema1::encode);
        SingleMessageCollector<TestMessage> collector = new SingleMessageCollector<>();
        schema2.deserialize(message2, collector);

        assertNotNull(collector.result);
        assertEquals(collector.result, message1);
    }

    @Test
    void createFromFlinkTypeInformation() throws Exception {
        PulsarDeserializationSchema<String> schema =
                new PulsarTypeInformationWrapper<>(Types.STRING, null);
        schema.open(new TestingDeserializationContext(), mock(SourceConfiguration.class));
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema));

        Message<byte[]> message =
                getMessage(
                        "test-content",
                        s -> {
                            DataOutputSerializer serializer = new DataOutputSerializer(10);
                            StringValue.writeString(s, serializer);
                            return serializer.getSharedBuffer();
                        });
        SingleMessageCollector<String> collector = new SingleMessageCollector<>();
        schema.deserialize(message, collector);

        assertNotNull(collector.result);
        assertEquals(collector.result, "test-content");
    }

    @Test
    void primitiveStringPulsarSchema() {
        final String topicName =
                "primitiveString-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        String expectedMessage = randomAlphabetic(10);
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.STRING,
                        expectedMessage);
        PulsarSource<String> source =
                createSource(topicName, new PulsarSchemaWrapper<>(Schema.STRING));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    @Test
    void unversionedJsonStructPulsarSchema() {
        final String topicName =
                "unversionedJsonStruct-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        TestingUser expectedMessage = createRandomUser();
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.JSON(TestingUser.class),
                        expectedMessage);
        PulsarSource<TestingUser> source =
                createSource(
                        topicName,
                        new PulsarSchemaWrapper<>(
                                Schema.JSON(TestingUser.class), TestingUser.class));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    @Test
    void keyValueJsonStructPulsarSchema() {
        final String topicName =
                "keyValueJsonStruct-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        KeyValue<TestingUser, TestingUser> expectedMessage =
                new KeyValue<>(createRandomUser(), createRandomUser());
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.KeyValue(
                                Schema.JSON(TestingUser.class), Schema.JSON(TestingUser.class)),
                        expectedMessage);
        PulsarSource<KeyValue<TestingUser, TestingUser>> source =
                createSource(
                        topicName,
                        new PulsarSchemaWrapper<>(
                                Schema.KeyValue(
                                        Schema.JSON(TestingUser.class),
                                        Schema.JSON(TestingUser.class)),
                                TestingUser.class,
                                TestingUser.class));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    @Test
    void keyValueAvroStructPulsarSchema() {
        final String topicName =
                "keyValueAvroStruct-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        KeyValue<TestingUser, TestingUser> expectedMessage =
                new KeyValue<>(createRandomUser(), createRandomUser());
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.KeyValue(
                                Schema.AVRO(TestingUser.class), Schema.AVRO(TestingUser.class)),
                        expectedMessage);
        PulsarSource<KeyValue<TestingUser, TestingUser>> source =
                createSource(
                        topicName,
                        new PulsarSchemaWrapper<>(
                                Schema.KeyValue(
                                        Schema.AVRO(TestingUser.class),
                                        Schema.AVRO(TestingUser.class)),
                                TestingUser.class,
                                TestingUser.class));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    @Test
    void keyValuePrimitivePulsarSchema() {
        final String topicName =
                "keyValuePrimitive-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        KeyValue<String, Integer> expectedMessage = new KeyValue<>(randomAlphabetic(5), 5);
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.KeyValue(Schema.STRING, Schema.INT32),
                        expectedMessage);
        PulsarSource<KeyValue<String, Integer>> source =
                createSource(
                        topicName,
                        new PulsarSchemaWrapper<>(
                                Schema.KeyValue(Schema.STRING, Schema.INT32),
                                String.class,
                                Integer.class));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    @Test
    void keyValuePrimitiveKeyStructValuePulsarSchema() {
        final String topicName =
                "primitiveKeyStructValue-"
                        + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        KeyValue<String, TestingUser> expectedMessage =
                new KeyValue<>(randomAlphabetic(5), createRandomUser());
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.KeyValue(Schema.STRING, Schema.JSON(TestingUser.class)),
                        expectedMessage);
        PulsarSource<KeyValue<String, TestingUser>> source =
                createSource(
                        topicName,
                        new PulsarSchemaWrapper<>(
                                Schema.KeyValue(Schema.STRING, Schema.JSON(TestingUser.class)),
                                String.class,
                                TestingUser.class));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    @Test
    void keyValueStructKeyPrimitiveValuePulsarSchema() {
        final String topicName =
                "structKeyPrimitiveValue-"
                        + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        KeyValue<TestingUser, String> expectedMessage =
                new KeyValue<>(createRandomUser(), randomAlphabetic(5));
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.KeyValue(Schema.JSON(TestingUser.class), Schema.STRING),
                        expectedMessage);
        PulsarSource<KeyValue<TestingUser, String>> source =
                createSource(
                        topicName,
                        new PulsarSchemaWrapper<>(
                                Schema.KeyValue(Schema.JSON(TestingUser.class), Schema.STRING),
                                TestingUser.class,
                                String.class));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    @Test
    void simpleFlinkSchema() {
        final String topicName =
                "simpleFlinkSchema-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        operator().createTopic(topicName, 1);
        String expectedMessage = randomAlphabetic(5);
        operator()
                .sendMessage(
                        TopicNameUtils.topicNameWithPartition(topicName, 0),
                        Schema.STRING,
                        expectedMessage);
        PulsarSource<String> source =
                createSource(
                        topicName,
                        new PulsarDeserializationSchemaWrapper<>(new SimpleStringSchema()));
        assertThatCode(() -> runPipeline(source, expectedMessage)).doesNotThrowAnyException();
    }

    private PulsarSource createSource(
            String topicName, PulsarDeserializationSchema<?> deserializationSchema) {
        return PulsarSource.builder()
                .setDeserializationSchema(deserializationSchema)
                .setServiceUrl(operator().serviceUrl())
                .setAdminUrl(operator().adminUrl())
                .setTopics(topicName)
                .setSubscriptionType(SubscriptionType.Exclusive)
                .setSubscriptionName(topicName + "-subscription")
                .setBoundedStopCursor(StopCursor.latest())
                .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, -1L)
                .build();
    }

    private <T> void runPipeline(PulsarSource<T> source, T expected) throws Exception {
        try (CloseableIterator<T> iterator =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(1)
                        .fromSource(source, WatermarkStrategy.noWatermarks(), "testSource")
                        .executeAndCollect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isEqualTo(expected);
        }
    }

    /** A test POJO class. */
    public static class TestingUser implements Serializable {
        private static final long serialVersionUID = -1123545861004770003L;
        public String name;
        public Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingUser that = (TestingUser) o;
            return Objects.equals(name, that.name) && Objects.equals(age, that.age);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
    }

    private TestingUser createRandomUser() {
        TestingUser user = new TestingUser();
        user.setName(randomAlphabetic(5));
        user.setAge(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        return user;
    }

    /** Create a test message by given bytes. The message don't contains any meta data. */
    private <T> Message<byte[]> getMessage(
            T message, FunctionWithException<T, byte[], Exception> decoder) throws Exception {
        byte[] bytes = decoder.apply(message);
        MessageMetadata metadata = new MessageMetadata();
        ByteBuffer payload = ByteBuffer.wrap(bytes);

        return MessageImpl.create(metadata, payload, Schema.BYTES, "");
    }

    /** This collector is used for collecting only one message. Used for test purpose. */
    private static class SingleMessageCollector<T> implements Collector<T> {

        private T result;

        @Override
        public void collect(T record) {
            checkState(result == null);
            this.result = record;
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
