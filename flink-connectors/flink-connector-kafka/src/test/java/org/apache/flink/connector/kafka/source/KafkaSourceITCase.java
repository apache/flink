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

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Unite test class for {@link KafkaSource}. */
public class KafkaSourceITCase {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";

    @BeforeClass
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.setupTopic(TOPIC1, true, true);
        KafkaSourceTestEnv.setupTopic(TOPIC2, true, true);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testBasicRead() throws Exception {
        KafkaSource<PartitionAndValue> source =
                KafkaSource.<PartitionAndValue>builder()
                        .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                        .setGroupId("testBasicRead")
                        .setTopics(Arrays.asList(TOPIC1, TOPIC2))
                        .setDeserializer(new TestingKafkaRecordDeserializer())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<PartitionAndValue> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");
        executeAndVerify(env, stream);
    }

    // -----------------

    private static class PartitionAndValue implements Serializable {
        private static final long serialVersionUID = 4813439951036021779L;
        private final String tp;
        private final int value;

        private PartitionAndValue(TopicPartition tp, int value) {
            this.tp = tp.toString();
            this.value = value;
        }
    }

    private static class TestingKafkaRecordDeserializer
            implements KafkaRecordDeserializer<PartitionAndValue> {
        private static final long serialVersionUID = -3765473065594331694L;
        private transient Deserializer<Integer> deserializer;

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record, Collector<PartitionAndValue> collector)
                throws Exception {
            if (deserializer == null) {
                deserializer = new IntegerDeserializer();
            }
            collector.collect(
                    new PartitionAndValue(
                            new TopicPartition(record.topic(), record.partition()),
                            deserializer.deserialize(record.topic(), record.value())));
        }

        @Override
        public TypeInformation<PartitionAndValue> getProducedType() {
            return TypeInformation.of(PartitionAndValue.class);
        }
    }

    @SuppressWarnings("serial")
    private void executeAndVerify(
            StreamExecutionEnvironment env, DataStream<PartitionAndValue> stream) throws Exception {
        stream.addSink(
                new RichSinkFunction<PartitionAndValue>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        getRuntimeContext()
                                .addAccumulator("result", new ListAccumulator<PartitionAndValue>());
                    }

                    @Override
                    public void invoke(PartitionAndValue value, Context context) throws Exception {
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
                                String.format(
                                        "The %d-th value for partition %s should be %d", i, tp, i),
                                firstExpectedValue + i,
                                (int) values.get(i));
                    }
                });
    }
}
