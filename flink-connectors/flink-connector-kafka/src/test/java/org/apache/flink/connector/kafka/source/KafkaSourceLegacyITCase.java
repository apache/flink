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

    @Test(timeout = 120000)
    public void testFailOnNoBroker() throws Exception {
        runFailOnNoBrokerTest();
    }

    @Test(timeout = 60000)
    public void testConcurrentProducerConsumerTopology() throws Exception {
        runSimpleConcurrentProducerConsumerTopology();
    }

    @Test(timeout = 60000)
    public void testKeyValueSupport() throws Exception {
        runKeyValueTest();
    }

    // --- canceling / failures ---

    @Test(timeout = 60000)
    public void testCancelingEmptyTopic() throws Exception {
        runCancelingOnEmptyInputTest();
    }

    @Test(timeout = 60000)
    public void testCancelingFullTopic() throws Exception {
        runCancelingOnFullInputTest();
    }

    // --- source to partition mappings and exactly once ---

    @Test(timeout = 60000)
    public void testOneToOneSources() throws Exception {
        runOneToOneExactlyOnceTest();
    }

    @Test(timeout = 60000)
    public void testOneSourceMultiplePartitions() throws Exception {
        runOneSourceMultiplePartitionsExactlyOnceTest();
    }

    @Test(timeout = 60000)
    public void testMultipleSourcesOnePartition() throws Exception {
        runMultipleSourcesOnePartitionExactlyOnceTest();
    }

    // --- broker failure ---

    @Test(timeout = 60000)
    public void testBrokerFailure() throws Exception {
        runBrokerFailureTest();
    }

    // --- special executions ---

    @Test(timeout = 60000)
    public void testBigRecordJob() throws Exception {
        runBigRecordTestTopology();
    }

    @Test(timeout = 60000)
    public void testMultipleTopicsWithLegacySerializer() throws Exception {
        runProduceConsumeMultipleTopics(true);
    }

    @Test(timeout = 60000)
    public void testMultipleTopicsWithKafkaSerializer() throws Exception {
        runProduceConsumeMultipleTopics(false);
    }

    @Test(timeout = 60000)
    public void testAllDeletes() throws Exception {
        runAllDeletesTest();
    }

    // --- startup mode ---

    @Test(timeout = 60000)
    public void testStartFromEarliestOffsets() throws Exception {
        runStartFromEarliestOffsets();
    }

    @Test(timeout = 60000)
    public void testStartFromLatestOffsets() throws Exception {
        runStartFromLatestOffsets();
    }

    @Test(timeout = 60000)
    public void testStartFromGroupOffsets() throws Exception {
        runStartFromGroupOffsets();
    }

    @Test(timeout = 60000)
    public void testStartFromSpecificOffsets() throws Exception {
        runStartFromSpecificOffsets();
    }

    @Test(timeout = 60000)
    public void testStartFromTimestamp() throws Exception {
        runStartFromTimestamp();
    }

    // --- offset committing ---

    @Test(timeout = 60000)
    public void testCommitOffsetsToKafka() throws Exception {
        runCommitOffsetsToKafka();
    }

    @Test(timeout = 60000)
    public void testAutoOffsetRetrievalAndCommitToKafka() throws Exception {
        runAutoOffsetRetrievalAndCommitToKafka();
    }

    @Test(timeout = 60000)
    public void testCollectingSchema() throws Exception {
        runCollectingSchemaTest();
    }
}
