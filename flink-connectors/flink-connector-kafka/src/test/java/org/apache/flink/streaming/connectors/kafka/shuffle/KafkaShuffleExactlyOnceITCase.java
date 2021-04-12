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

package org.apache.flink.streaming.connectors.kafka.shuffle;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.ValidatingExactlyOnceSink;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;
import static org.apache.flink.streaming.api.TimeCharacteristic.IngestionTime;
import static org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime;
import static org.apache.flink.test.util.TestUtils.tryExecute;

/** Failure Recovery IT Test for KafkaShuffle. */
public class KafkaShuffleExactlyOnceITCase extends KafkaShuffleTestBase {

    @Rule public final Timeout timeout = Timeout.millis(600000L);

    /**
     * Failure Recovery after processing 2/3 data with time characteristic: ProcessingTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testFailureRecoveryProcessingTime() throws Exception {
        testKafkaShuffleFailureRecovery(1000, ProcessingTime);
    }

    /**
     * Failure Recovery after processing 2/3 data with time characteristic: IngestionTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testFailureRecoveryIngestionTime() throws Exception {
        testKafkaShuffleFailureRecovery(1000, IngestionTime);
    }

    /**
     * Failure Recovery after processing 2/3 data with time characteristic: EventTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testFailureRecoveryEventTime() throws Exception {
        testKafkaShuffleFailureRecovery(1000, EventTime);
    }

    /**
     * Failure Recovery after data is repartitioned with time characteristic: ProcessingTime.
     *
     * <p>Producer Parallelism = 2; Kafka Partition # = 3; Consumer Parallelism = 3.
     */
    @Test
    public void testAssignedToPartitionFailureRecoveryProcessingTime() throws Exception {
        testAssignedToPartitionFailureRecovery(500, ProcessingTime);
    }

    /**
     * Failure Recovery after data is repartitioned with time characteristic: IngestionTime.
     *
     * <p>Producer Parallelism = 2; Kafka Partition # = 3; Consumer Parallelism = 3.
     */
    @Test
    public void testAssignedToPartitionFailureRecoveryIngestionTime() throws Exception {
        testAssignedToPartitionFailureRecovery(500, IngestionTime);
    }

    /**
     * Failure Recovery after data is repartitioned with time characteristic: EventTime.
     *
     * <p>Producer Parallelism = 2; Kafka Partition # = 3; Consumer Parallelism = 3.
     */
    @Test
    public void testAssignedToPartitionFailureRecoveryEventTime() throws Exception {
        testAssignedToPartitionFailureRecovery(500, EventTime);
    }

    /**
     * To test failure recovery after processing 2/3 data.
     *
     * <p>Schema: (key, timestamp, source instance Id). Producer Parallelism = 1; Kafka Partition #
     * = 1; Consumer Parallelism = 1
     */
    private void testKafkaShuffleFailureRecovery(
            int numElementsPerProducer, TimeCharacteristic timeCharacteristic) throws Exception {

        String topic = topic("failure_recovery", timeCharacteristic);
        final int numberOfPartitions = 1;
        final int producerParallelism = 1;
        final int failAfterElements = numElementsPerProducer * numberOfPartitions * 2 / 3;

        createTestTopic(topic, numberOfPartitions, 1);

        final StreamExecutionEnvironment env =
                createEnvironment(producerParallelism, timeCharacteristic).enableCheckpointing(500);

        createKafkaShuffle(
                        env,
                        topic,
                        numElementsPerProducer,
                        producerParallelism,
                        timeCharacteristic,
                        numberOfPartitions)
                .map(new FailingIdentityMapper<>(failAfterElements))
                .setParallelism(1)
                .map(new ToInteger(producerParallelism))
                .setParallelism(1)
                .addSink(
                        new ValidatingExactlyOnceSink(numElementsPerProducer * producerParallelism))
                .setParallelism(1);

        FailingIdentityMapper.failedBefore = false;

        tryExecute(env, topic);

        deleteTestTopic(topic);
    }

    /**
     * To test failure recovery with partition assignment after processing 2/3 data.
     *
     * <p>Schema: (key, timestamp, source instance Id). Producer Parallelism = 2; Kafka Partition #
     * = 3; Consumer Parallelism = 3
     */
    private void testAssignedToPartitionFailureRecovery(
            int numElementsPerProducer, TimeCharacteristic timeCharacteristic) throws Exception {
        String topic = topic("partition_failure_recovery", timeCharacteristic);
        final int numberOfPartitions = 3;
        final int producerParallelism = 2;
        final int failAfterElements = numElementsPerProducer * producerParallelism * 2 / 3;

        createTestTopic(topic, numberOfPartitions, 1);

        final StreamExecutionEnvironment env =
                createEnvironment(producerParallelism, timeCharacteristic);

        KeyedStream<Tuple3<Integer, Long, Integer>, Tuple> keyedStream =
                createKafkaShuffle(
                        env,
                        topic,
                        numElementsPerProducer,
                        producerParallelism,
                        timeCharacteristic,
                        numberOfPartitions);
        keyedStream
                .process(
                        new PartitionValidator(
                                keyedStream.getKeySelector(), numberOfPartitions, topic))
                .setParallelism(numberOfPartitions)
                .map(new ToInteger(producerParallelism))
                .setParallelism(numberOfPartitions)
                .map(new FailingIdentityMapper<>(failAfterElements))
                .setParallelism(1)
                .addSink(
                        new ValidatingExactlyOnceSink(numElementsPerProducer * producerParallelism))
                .setParallelism(1);

        FailingIdentityMapper.failedBefore = false;

        tryExecute(env, topic);

        deleteTestTopic(topic);
    }

    private StreamExecutionEnvironment createEnvironment(
            int producerParallelism, TimeCharacteristic timeCharacteristic) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(producerParallelism);
        env.setStreamTimeCharacteristic(timeCharacteristic);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.setBufferTimeout(0);
        env.enableCheckpointing(500);

        return env;
    }

    private static class ToInteger implements MapFunction<Tuple3<Integer, Long, Integer>, Integer> {
        private final int producerParallelism;

        ToInteger(int producerParallelism) {
            this.producerParallelism = producerParallelism;
        }

        @Override
        public Integer map(Tuple3<Integer, Long, Integer> element) throws Exception {

            return element.f0 * producerParallelism + element.f2;
        }
    }
}
