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

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaConsumerTestBase;
import org.apache.flink.streaming.connectors.kafka.KafkaProducerTestBase;
import org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * An IT case class that runs all the IT cases of the legacy {@link
 * org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer} with the new {@link KafkaSource}.
 */
class KafkaSourceLegacyITCase extends KafkaConsumerTestBase {

    public KafkaSourceLegacyITCase() throws Exception {
        super(true);
    }

    @BeforeAll
    public static void prepare() throws Exception {
        KafkaProducerTestBase.prepare();
        ((KafkaTestEnvironmentImpl) kafkaServer)
                .setProducerSemantic(FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }

    @Test
    void testFailOnNoBroker() throws Exception {
        runFailOnNoBrokerTest();
    }

    @Test
    void testConcurrentProducerConsumerTopology() throws Exception {
        runSimpleConcurrentProducerConsumerTopology();
    }

    @Test
    void testKeyValueSupport() throws Exception {
        runKeyValueTest();
    }

    // --- canceling / failures ---

    @Test
    void testCancelingEmptyTopic() throws Exception {
        runCancelingOnEmptyInputTest();
    }

    @Test
    void testCancelingFullTopic() throws Exception {
        runCancelingOnFullInputTest();
    }

    // --- source to partition mappings and exactly once ---

    @Test
    void testOneToOneSources() throws Exception {
        runOneToOneExactlyOnceTest();
    }

    @Test
    void testOneSourceMultiplePartitions() throws Exception {
        runOneSourceMultiplePartitionsExactlyOnceTest();
    }

    @Test
    void testMultipleSourcesOnePartition() throws Exception {
        runMultipleSourcesOnePartitionExactlyOnceTest();
    }

    // --- broker failure ---

    @Test
    @Disabled("FLINK-28267")
    void testBrokerFailure() throws Exception {
        runBrokerFailureTest();
    }

    // --- special executions ---

    @Test
    void testBigRecordJob() throws Exception {
        runBigRecordTestTopology();
    }

    @Test
    void testMultipleTopicsWithLegacySerializer() throws Exception {
        runProduceConsumeMultipleTopics(true);
    }

    @Test
    void testMultipleTopicsWithKafkaSerializer() throws Exception {
        runProduceConsumeMultipleTopics(false);
    }

    @Test
    void testAllDeletes() throws Exception {
        runAllDeletesTest();
    }

    // --- startup mode ---

    @Test
    void testStartFromEarliestOffsets() throws Exception {
        runStartFromEarliestOffsets();
    }

    @Test
    void testStartFromLatestOffsets() throws Exception {
        runStartFromLatestOffsets();
    }

    @Test
    void testStartFromGroupOffsets() throws Exception {
        runStartFromGroupOffsets();
    }

    @Test
    void testStartFromSpecificOffsets() throws Exception {
        runStartFromSpecificOffsets();
    }

    @Test
    void testStartFromTimestamp() throws Exception {
        runStartFromTimestamp();
    }

    // --- offset committing ---

    @Test
    void testCommitOffsetsToKafka() throws Exception {
        runCommitOffsetsToKafka();
    }

    @Test
    void testAutoOffsetRetrievalAndCommitToKafka() throws Exception {
        runAutoOffsetRetrievalAndCommitToKafka();
    }

    @Test
    void testCollectingSchema() throws Exception {
        runCollectingSchemaTest();
    }
}
