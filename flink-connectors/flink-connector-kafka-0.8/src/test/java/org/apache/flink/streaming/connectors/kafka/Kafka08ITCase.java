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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.ZookeeperOffsetHandler;

import org.apache.curator.framework.CuratorFramework;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * IT cases for Kafka 0.8 .
 */
public class Kafka08ITCase extends KafkaConsumerTestBase {

	@BeforeClass
	public static void prepare() throws Exception {
		// Somehow KafkaConsumer 0.8 doesn't handle broker failures if they are behind a proxy
		prepare(false);
	}

	// ------------------------------------------------------------------------
	//  Suite of Tests
	// ------------------------------------------------------------------------

	@Test(timeout = 60000)
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

	@Test(timeout = 60000)
	public void testInvalidOffset() throws Exception {

		final int parallelism = 1;

		// write 20 messages into topic:
		final String topic = writeSequence("invalidOffsetTopic", 20, parallelism, 1);

		// set invalid offset:
		try (CuratorFramework curatorClient = ((KafkaTestEnvironmentImpl) kafkaServer).createCuratorClient()) {
			ZookeeperOffsetHandler.setOffsetInZooKeeper(curatorClient, standardProps.getProperty("group.id"), topic, 0, 1234);
		}

		// read from topic
		final int valuesCount = 20;
		final int startFrom = 0;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		readSequence(env, StartupMode.GROUP_OFFSETS, null, null, standardProps, parallelism, topic, valuesCount, startFrom);

		deleteTestTopic(topic);
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

	// --- offset committing ---

	@Test(timeout = 60000)
	public void testCommitOffsetsToZookeeper() throws Exception {
		runCommitOffsetsToKafka();
	}

	@Test(timeout = 60000)
	public void testAutoOffsetRetrievalAndCommitToZookeeper() throws Exception {
		runAutoOffsetRetrievalAndCommitToKafka();
	}

	@Test
	public void runOffsetManipulationInZooKeeperTest() {
		try {
			final String topicName = "ZookeeperOffsetHandlerTest-Topic";
			final String groupId = "ZookeeperOffsetHandlerTest-Group";

			final Long offset = (long) (Math.random() * Long.MAX_VALUE);

			final Long fetchedOffset;
			try (CuratorFramework curatorFramework = ((KafkaTestEnvironmentImpl) kafkaServer).createCuratorClient()) {
				kafkaServer.createTestTopic(topicName, 3, 2);

				ZookeeperOffsetHandler.setOffsetInZooKeeper(curatorFramework, groupId, topicName, 0, offset);

				fetchedOffset = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, groupId, topicName, 0);
			}

			assertEquals(offset, fetchedOffset);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test(timeout = 60000)
	public void testOffsetAutocommitTest() throws Exception {
		final int parallelism = 3;

		// write a sequence from 0 to 99 to each of the 3 partitions.
		final String topicName = writeSequence("testOffsetAutocommit", 100, parallelism, 1);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// NOTE: We are not enabling the checkpointing!
				env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.setParallelism(parallelism);

		// the readSequence operation sleeps for 20 ms between each record.
		// setting a delay of 25*20 = 500 for the commit interval makes
		// sure that we commit roughly 3-4 times while reading, however
		// at least once.
		Properties readProps = new Properties();
		readProps.putAll(standardProps);

		// make sure that auto commit is enabled in the properties
		readProps.setProperty("auto.commit.enable", "true");
		readProps.setProperty("auto.commit.interval.ms", "500");

		// read so that the offset can be committed to ZK
		readSequence(env, StartupMode.GROUP_OFFSETS, null, null, readProps, parallelism, topicName, 100, 0);

		final Long o1, o2, o3;
		// get the offset
		try (CuratorFramework curatorFramework = ((KafkaTestEnvironmentImpl) kafkaServer).createCuratorClient()) {
			o1 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, standardProps.getProperty("group.id"), topicName, 0);
			o2 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, standardProps.getProperty("group.id"), topicName, 1);
			o3 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, standardProps.getProperty("group.id"), topicName, 2);
		}

		LOG.info("Got final offsets from zookeeper o1={}, o2={}, o3={}", o1, o2, o3);

		// ensure that the offset has been committed
		boolean atLeastOneOffsetSet = (o1 != null && o1 > 0 && o1 <= 100) ||
			(o2 != null && o2 > 0 && o2 <= 100) ||
			(o3 != null && o3 > 0 && o3 <= 100);
		assertTrue("Expecting at least one offset to be set o1=" + o1 + " o2=" + o2 + " o3=" + o3, atLeastOneOffsetSet);

		deleteTestTopic(topicName);
	}

	// --- special executions ---

	@Test(timeout = 60000)
	public void testBigRecordJob() throws Exception {
		runBigRecordTestTopology();
	}

	@Test(timeout = 60000)
	public void testMultipleTopics() throws Exception {
		runProduceConsumeMultipleTopics(true);
	}

	@Test(timeout = 60000)
	public void testAllDeletes() throws Exception {
		runAllDeletesTest();
	}

	@Test(timeout = 60000)
	public void testEndOfStream() throws Exception {
		runEndOfStreamTest();
	}

	@Test(timeout = 60000)
	public void testMetrics() throws Throwable {
		runMetricsTest();
	}
}
