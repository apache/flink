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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for {@link FlinkKafkaProducer}. */
public class FlinkKafkaProducerTest {
    @Test
    public void testOpenSerializationSchemaProducer() throws Exception {
        OpenTestingSerializationSchema schema = new OpenTestingSerializationSchema();
        FlinkKafkaProducer<Integer> kafkaProducer =
                new FlinkKafkaProducer<>("localhost:9092", "test-topic", schema);

        OneInputStreamOperatorTestHarness<Integer, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new StreamSink<>(kafkaProducer),
                        1,
                        1,
                        0,
                        IntSerializer.INSTANCE,
                        new OperatorID(1, 1));

        testHarness.open();

        assertThat(schema.openCalled, equalTo(true));
    }

    @Test
    public void testOpenKafkaSerializationSchemaProducer() throws Exception {
        OpenTestingKafkaSerializationSchema schema = new OpenTestingKafkaSerializationSchema();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<Integer> kafkaProducer =
                new FlinkKafkaProducer<>(
                        "test-topic",
                        schema,
                        properties,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        OneInputStreamOperatorTestHarness<Integer, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new StreamSink<>(kafkaProducer),
                        1,
                        1,
                        0,
                        IntSerializer.INSTANCE,
                        new OperatorID(1, 1));

        testHarness.open();

        assertThat(schema.openCalled, equalTo(true));
    }

    @Test
    public void testOpenKafkaCustomPartitioner() throws Exception {
        CustomPartitioner<Integer> partitioner = new CustomPartitioner<>();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<Integer> kafkaProducer =
                new FlinkKafkaProducer<>(
                        "test-topic",
                        new OpenTestingSerializationSchema(),
                        properties,
                        Optional.of(partitioner));

        OneInputStreamOperatorTestHarness<Integer, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new StreamSink<>(kafkaProducer),
                        1,
                        1,
                        0,
                        IntSerializer.INSTANCE,
                        new OperatorID(1, 1));

        testHarness.open();

        assertThat(partitioner.openCalled, equalTo(true));
    }

    private static class CustomPartitioner<T> extends FlinkKafkaPartitioner<T> {
        private boolean openCalled;

        @Override
        public void open(int parallelInstanceId, int parallelInstances) {
            super.open(parallelInstanceId, parallelInstances);
            openCalled = true;
        }

        @Override
        public int partition(
                T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return 0;
        }
    }

    private static class OpenTestingKafkaSerializationSchema
            implements KafkaSerializationSchema<Integer> {
        private boolean openCalled;

        @Override
        public void open(SerializationSchema.InitializationContext context) throws Exception {
            openCalled = true;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Integer element, @Nullable Long timestamp) {
            return null;
        }
    }

    private static class OpenTestingSerializationSchema implements SerializationSchema<Integer> {
        private boolean openCalled;

        @Override
        public void open(SerializationSchema.InitializationContext context) throws Exception {
            openCalled = true;
        }

        @Override
        public byte[] serialize(Integer element) {
            return new byte[0];
        }
    }
}
