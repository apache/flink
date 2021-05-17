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

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * An IT case class that runs all the IT cases of the legacy {@link
 * org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer} with the new {@link KafkaSource}.
 */
public class KafkaSourceLegacyITCase extends KafkaConsumerTestBase {

    public KafkaSourceLegacyITCase() throws Exception {
        super(true);
    }

    @BeforeClass
    public static void prepare() throws Exception {
        KafkaProducerTestBase.prepare();
        ((KafkaTestEnvironmentImpl) kafkaServer)
                .setProducerSemantic(FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }

    @Test
    public void testFailOnNoBroker() throws Exception {
        runFailOnNoBrokerTest();
    }

    @Test
    public void testConcurrentProducerConsumerTopology() throws Exception {
        runSimpleConcurrentProducerConsumerTopology();
    }

    @Test
    public void testKeyValueSupport() throws Exception {
        runKeyValueTest();
    }

    // --- canceling / failures ---

    @Test
    public void testCancelingEmptyTopic() throws Exception {
        runCancelingOnEmptyInputTest();
    }

    @Test
    public void testCancelingFullTopic() throws Exception {
        runCancelingOnFullInputTest();
    }

    // --- source to partition mappings and exactly once ---

    @Test
    public void testOneToOneSources() throws Exception {
        runOneToOneExactlyOnceTest();
    }

    @Test
    public void testOneSourceMultiplePartitions() throws Exception {
        runOneSourceMultiplePartitionsExactlyOnceTest();
    }

    @Test
    public void testMultipleSourcesOnePartition() throws Exception {
        runMultipleSourcesOnePartitionExactlyOnceTest();
    }

    // --- broker failure ---

    @Test
    public void testBrokerFailure() throws Exception {
        runBrokerFailureTest();
    }

    // --- special executions ---

    @Test
    public void testBigRecordJob() throws Exception {
        runBigRecordTestTopology();
    }

    @Test
    public void testMultipleTopicsWithLegacySerializer() throws Exception {
        runProduceConsumeMultipleTopics(true);
    }

    @Test
    public void testMultipleTopicsWithKafkaSerializer() throws Exception {
        runProduceConsumeMultipleTopics(false);
    }

    @Test
    public void testAllDeletes() throws Exception {
        runAllDeletesTest();
    }

    // --- startup mode ---

    @Test
    public void testStartFromEarliestOffsets() throws Exception {
        runStartFromEarliestOffsets();
    }

    @Test
    public void testStartFromLatestOffsets() throws Exception {
        runStartFromLatestOffsets();
    }

    @Test
    public void testStartFromGroupOffsets() throws Exception {
        runStartFromGroupOffsets();
    }

    @Test
    public void testStartFromSpecificOffsets() throws Exception {
        runStartFromSpecificOffsets();
    }

    @Test
    public void testStartFromTimestamp() throws Exception {
        runStartFromTimestamp();
    }

    // --- offset committing ---

    @Test
    public void testCommitOffsetsToKafka() throws Exception {
        runCommitOffsetsToKafka();
    }

    @Test
    public void testAutoOffsetRetrievalAndCommitToKafka() throws Exception {
        runAutoOffsetRetrievalAndCommitToKafka();
    }

    @Test
    public void testCollectingSchema() throws Exception {
        runCollectingSchemaTest();
    }
}
