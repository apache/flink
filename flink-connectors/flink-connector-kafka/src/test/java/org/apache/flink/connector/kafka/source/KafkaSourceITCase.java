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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.testutils.KafkaMultipleTopicExternalContext;
import org.apache.flink.connector.kafka.source.testutils.KafkaSingleTopicExternalContext;
import org.apache.flink.connector.kafka.source.testutils.KafkaSourceTestEnv;
import org.apache.flink.connectors.test.common.environment.MiniClusterTestEnvironment;
import org.apache.flink.connectors.test.common.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalContextFactory;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.TestEnv;
import org.apache.flink.connectors.test.common.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unite test class for {@link KafkaSource}. */
public class KafkaSourceITCase {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class KafkaSpecificTests {
        @BeforeAll
        public void setup() throws Throwable {
            KafkaSourceTestEnv.setup();
            KafkaSourceTestEnv.setupTopic(
                    TOPIC1, true, true, KafkaSourceTestEnv::getRecordsForTopicWithoutTimestamp);
            KafkaSourceTestEnv.setupTopic(
                    TOPIC2, true, true, KafkaSourceTestEnv::getRecordsForTopicWithoutTimestamp);
        }

        @AfterAll
        public void tearDown() throws Exception {
            KafkaSourceTestEnv.tearDown();
        }

        @ParameterizedTest(name = "Object reuse in deserializer = {arguments}")
        @ValueSource(booleans = {false, true})
        public void testTimestamp(boolean enableObjectReuse) throws Throwable {
            final String topic =
                    "testTimestamp-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
            final long currentTimestamp = System.currentTimeMillis();
            KafkaSourceTestEnv.createTestTopic(topic, 1, 1);
            KafkaSourceTestEnv.produceToKafka(
                    Arrays.asList(
                            new ProducerRecord<>(topic, 0, currentTimestamp + 1L, "key0", 0),
                            new ProducerRecord<>(topic, 0, currentTimestamp + 2L, "key1", 1),
                            new ProducerRecord<>(topic, 0, currentTimestamp + 3L, "key2", 2)));

            KafkaSource<PartitionAndValue> source =
                    KafkaSource.<PartitionAndValue>builder()
                            .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                            .setGroupId("testTimestampAndWatermark")
                            .setTopics(topic)
                            .setDeserializer(
                                    new TestingKafkaRecordDeserializationSchema(enableObjectReuse))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setBounded(OffsetsInitializer.latest())
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<PartitionAndValue> stream =
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), "testTimestamp");

            // Verify that the timestamp and watermark are working fine.
            stream.transform(
                    "timestampVerifier",
                    TypeInformation.of(PartitionAndValue.class),
                    new WatermarkVerifyingOperator(v -> v));
            stream.addSink(new DiscardingSink<>());
            JobExecutionResult result = env.execute();

            assertEquals(
                    Arrays.asList(
                            currentTimestamp + 1L, currentTimestamp + 2L, currentTimestamp + 3L),
                    result.getAccumulatorResult("timestamp"));
        }

        @ParameterizedTest(name = "Object reuse in deserializer = {arguments}")
        @ValueSource(booleans = {false, true})
        public void testBasicRead(boolean enableObjectReuse) throws Exception {
            KafkaSource<PartitionAndValue> source =
                    KafkaSource.<PartitionAndValue>builder()
                            .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                            .setGroupId("testBasicRead")
                            .setTopics(Arrays.asList(TOPIC1, TOPIC2))
                            .setDeserializer(
                                    new TestingKafkaRecordDeserializationSchema(enableObjectReuse))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setBounded(OffsetsInitializer.latest())
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<PartitionAndValue> stream =
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");
            executeAndVerify(env, stream);
        }

        @Test
        public void testValueOnlyDeserializer() throws Exception {
            KafkaSource<Integer> source =
                    KafkaSource.<Integer>builder()
                            .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                            .setGroupId("testValueOnlyDeserializer")
                            .setTopics(Arrays.asList(TOPIC1, TOPIC2))
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setBounded(OffsetsInitializer.latest())
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            final CloseableIterator<Integer> resultIterator =
                    env.fromSource(
                                    source,
                                    WatermarkStrategy.noWatermarks(),
                                    "testValueOnlyDeserializer")
                            .executeAndCollect();

            AtomicInteger actualSum = new AtomicInteger();
            resultIterator.forEachRemaining(actualSum::addAndGet);

            // Calculate the actual sum of values
            // Values in a partition should start from partition ID, and end with
            // (NUM_RECORDS_PER_PARTITION - 1)
            // e.g. Values in partition 5 should be {5, 6, 7, 8, 9}
            int expectedSum = 0;
            for (int partition = 0; partition < KafkaSourceTestEnv.NUM_PARTITIONS; partition++) {
                for (int value = partition;
                        value < KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION;
                        value++) {
                    expectedSum += value;
                }
            }

            // Since we have two topics, the expected sum value should be doubled
            expectedSum *= 2;

            assertEquals(expectedSum, actualSum.get());
        }

        @ParameterizedTest(name = "Object reuse in deserializer = {arguments}")
        @ValueSource(booleans = {false, true})
        public void testRedundantParallelism(boolean enableObjectReuse) throws Exception {
            KafkaSource<PartitionAndValue> source =
                    KafkaSource.<PartitionAndValue>builder()
                            .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                            .setGroupId("testRedundantParallelism")
                            .setTopics(Collections.singletonList(TOPIC1))
                            .setDeserializer(
                                    new TestingKafkaRecordDeserializationSchema(enableObjectReuse))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setBounded(OffsetsInitializer.latest())
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Here we use (NUM_PARTITION + 1) as the parallelism, so one SourceReader will not be
            // assigned with any splits. The redundant SourceReader should also be signaled with a
            // NoMoreSplitsEvent and eventually spins to FINISHED state.
            env.setParallelism(KafkaSourceTestEnv.NUM_PARTITIONS + 1);
            DataStream<PartitionAndValue> stream =
                    env.fromSource(
                            source, WatermarkStrategy.noWatermarks(), "testRedundantParallelism");
            executeAndVerify(env, stream);
        }

        @ParameterizedTest(name = "Object reuse in deserializer = {arguments}")
        @ValueSource(booleans = {false, true})
        public void testBasicReadWithoutGroupId(boolean enableObjectReuse) throws Exception {
            KafkaSource<PartitionAndValue> source =
                    KafkaSource.<PartitionAndValue>builder()
                            .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                            .setTopics(Arrays.asList(TOPIC1, TOPIC2))
                            .setDeserializer(
                                    new TestingKafkaRecordDeserializationSchema(enableObjectReuse))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setBounded(OffsetsInitializer.latest())
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<PartitionAndValue> stream =
                    env.fromSource(
                            source,
                            WatermarkStrategy.noWatermarks(),
                            "testBasicReadWithoutGroupId");
            executeAndVerify(env, stream);
        }
    }

    /** Integration test based on connector testing framework. */
    @Nested
    class IntegrationTests extends SourceTestSuiteBase<String> {
        private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:5.5.2";

        // Defines test environment on Flink MiniCluster
        @SuppressWarnings("unused")
        @TestEnv
        MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

        // Defines external system
        @ExternalSystem
        DefaultContainerizedExternalSystem<KafkaContainer> kafka =
                DefaultContainerizedExternalSystem.builder()
                        .fromContainer(new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME)))
                        .build();

        // Defines 2 External context Factories, so test cases will be invoked twice using these two
        // kinds of external contexts.
        @SuppressWarnings("unused")
        @ExternalContextFactory
        KafkaSingleTopicExternalContext.Factory singleTopic =
                new KafkaSingleTopicExternalContext.Factory(kafka.getContainer());

        @SuppressWarnings("unused")
        @ExternalContextFactory
        KafkaMultipleTopicExternalContext.Factory multipleTopic =
                new KafkaMultipleTopicExternalContext.Factory(kafka.getContainer());
    }

    // -----------------

    private static class PartitionAndValue implements Serializable {
        private static final long serialVersionUID = 4813439951036021779L;
        private String tp;
        private int value;

        public PartitionAndValue() {}

        private PartitionAndValue(TopicPartition tp, int value) {
            this.tp = tp.toString();
            this.value = value;
        }
    }

    private static class TestingKafkaRecordDeserializationSchema
            implements KafkaRecordDeserializationSchema<PartitionAndValue> {
        private static final long serialVersionUID = -3765473065594331694L;
        private transient Deserializer<Integer> deserializer;
        private final boolean enableObjectReuse;
        private final PartitionAndValue reuse = new PartitionAndValue();

        public TestingKafkaRecordDeserializationSchema(boolean enableObjectReuse) {
            this.enableObjectReuse = enableObjectReuse;
        }

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record, Collector<PartitionAndValue> collector)
                throws IOException {
            if (deserializer == null) {
                deserializer = new IntegerDeserializer();
            }

            if (enableObjectReuse) {
                reuse.tp = new TopicPartition(record.topic(), record.partition()).toString();
                reuse.value = deserializer.deserialize(record.topic(), record.value());
                collector.collect(reuse);
            } else {
                collector.collect(
                        new PartitionAndValue(
                                new TopicPartition(record.topic(), record.partition()),
                                deserializer.deserialize(record.topic(), record.value())));
            }
        }

        @Override
        public TypeInformation<PartitionAndValue> getProducedType() {
            return TypeInformation.of(PartitionAndValue.class);
        }
    }

    private static class WatermarkVerifyingOperator
            extends StreamMap<PartitionAndValue, PartitionAndValue> {

        public WatermarkVerifyingOperator(
                MapFunction<PartitionAndValue, PartitionAndValue> mapper) {
            super(mapper);
        }

        private static final long serialVersionUID = 2868223355944228209L;

        @Override
        public void open() throws Exception {
            getRuntimeContext().addAccumulator("timestamp", new ListAccumulator<Long>());
        }

        @Override
        public void processElement(StreamRecord<PartitionAndValue> element) {
            getRuntimeContext().getAccumulator("timestamp").add(element.getTimestamp());
        }
    }

    private void executeAndVerify(
            StreamExecutionEnvironment env, DataStream<PartitionAndValue> stream) throws Exception {
        stream.addSink(
                new RichSinkFunction<PartitionAndValue>() {
                    @Override
                    public void open(Configuration parameters) {
                        getRuntimeContext()
                                .addAccumulator("result", new ListAccumulator<PartitionAndValue>());
                    }

                    @Override
                    public void invoke(PartitionAndValue value, Context context) {
                        getRuntimeContext().getAccumulator("result").add(value);
                    }
                });
        List<PartitionAndValue> result = env.execute().getAccumulatorResult("result");
        Map<String, List<Integer>> resultPerPartition = new HashMap<>();
        result.forEach(
                partitionAndValue ->
                        resultPerPartition
                                .computeIfAbsent(partitionAndValue.tp, ignored -> new ArrayList<>())
                                .add(partitionAndValue.value));
        resultPerPartition.forEach(
                (tp, values) -> {
                    int firstExpectedValue = Integer.parseInt(tp.substring(tp.indexOf('-') + 1));
                    for (int i = 0; i < values.size(); i++) {
                        assertEquals(
                                firstExpectedValue + i,
                                (int) values.get(i),
                                String.format(
                                        "The %d-th value for partition %s should be %d", i, tp, i));
                    }
                });
    }
}
