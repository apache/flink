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

import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.ZookeeperOffsetHandler;
import org.junit.Test;

import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.assertTrue;


public class Kafka08ITCase extends KafkaConsumerTestBase {

	// ------------------------------------------------------------------------
	//  Suite of Tests
	// ------------------------------------------------------------------------

	@Test(timeout = 60000)
	public void testCheckpointing() throws Exception {
		runCheckpointingTest();
	}

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
	public void testFailOnDeploy() throws Exception {
		runFailOnDeployTest();
	}

	@Test(timeout = 60000)
	public void testInvalidOffset() throws Exception {
		final String topic = "invalidOffsetTopic";
		final int parallelism = 1;

		// create topic
		createTestTopic(topic, parallelism, 1);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);

		// write 20 messages into topic:
		writeSequence(env, topic, 20, parallelism);

		// set invalid offset:
		CuratorFramework curatorClient = ((KafkaTestEnvironmentImpl)kafkaServer).createCuratorClient();
		ZookeeperOffsetHandler.setOffsetInZooKeeper(curatorClient, standardCC.groupId(), topic, 0, 1234);
		curatorClient.close();

		// read from topic
		final int valuesCount = 20;
		final int startFrom = 0;
		readSequence(env, standardCC.props().props(), parallelism, topic, valuesCount, startFrom);

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

	// --- special executions ---

	@Test(timeout = 60000)
	public void testBigRecordJob() throws Exception {
		runBigRecordTestTopology();
	}

	@Test(timeout = 60000)
	public void testMultipleTopics() throws Exception {
		runConsumeMultipleTopics();
	}

	@Test(timeout = 60000)
	public void testAllDeletes() throws Exception {
		runAllDeletesTest();
	}

	@Test(timeout=60000)
	public void testMetricsAndEndOfStream() throws Exception {
		runMetricsAndEndOfStreamTest();
	}


	/**
	 * Tests that offsets are properly committed to ZooKeeper and initial offsets are read from ZooKeeper.
	 *
	 * This test is only applicable if the Flink Kafka Consumer uses the ZooKeeperOffsetHandler.
	 */
	@Test(timeout = 60000)
	public void testOffsetInZookeeper() throws Exception {
		final String topicName = "testOffsetInZK";
		final int parallelism = 3;

		createTestTopic(topicName, parallelism, 1);

		StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env1.getConfig().disableSysoutLogging();
		env1.enableCheckpointing(50);
		env1.setNumberOfExecutionRetries(0);
		env1.setParallelism(parallelism);

		StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env2.getConfig().disableSysoutLogging();
		env2.enableCheckpointing(50);
		env2.setNumberOfExecutionRetries(0);
		env2.setParallelism(parallelism);

		StreamExecutionEnvironment env3 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env3.getConfig().disableSysoutLogging();
		env3.enableCheckpointing(50);
		env3.setNumberOfExecutionRetries(0);
		env3.setParallelism(parallelism);

		// write a sequence from 0 to 99 to each of the 3 partitions.
		writeSequence(env1, topicName, 100, parallelism);

		readSequence(env2, standardProps, parallelism, topicName, 100, 0);

		CuratorFramework curatorClient = ((KafkaTestEnvironmentImpl)kafkaServer).createCuratorClient();

		long o1 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorClient, standardCC.groupId(), topicName, 0);
		long o2 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorClient, standardCC.groupId(), topicName, 1);
		long o3 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorClient, standardCC.groupId(), topicName, 2);

		LOG.info("Got final offsets from zookeeper o1={}, o2={}, o3={}", o1, o2, o3);

		assertTrue(o1 == FlinkKafkaConsumer08.OFFSET_NOT_SET || (o1 >= 0 && o1 <= 100));
		assertTrue(o2 == FlinkKafkaConsumer08.OFFSET_NOT_SET || (o2 >= 0 && o2 <= 100));
		assertTrue(o3 == FlinkKafkaConsumer08.OFFSET_NOT_SET || (o3 >= 0 && o3 <= 100));

		LOG.info("Manipulating offsets");

		// set the offset to 50 for the three partitions
		ZookeeperOffsetHandler.setOffsetInZooKeeper(curatorClient, standardCC.groupId(), topicName, 0, 49);
		ZookeeperOffsetHandler.setOffsetInZooKeeper(curatorClient, standardCC.groupId(), topicName, 1, 49);
		ZookeeperOffsetHandler.setOffsetInZooKeeper(curatorClient, standardCC.groupId(), topicName, 2, 49);

		curatorClient.close();

		// create new env
		readSequence(env3, standardProps, parallelism, topicName, 50, 50);

		deleteTestTopic(topicName);
	}

	@Test(timeout = 60000)
	public void testOffsetAutocommitTest() throws Exception {
		final String topicName = "testOffsetAutocommit";
		final int parallelism = 3;

		createTestTopic(topicName, parallelism, 1);

		StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env1.getConfig().disableSysoutLogging();
		env1.setNumberOfExecutionRetries(0);
		env1.setParallelism(parallelism);

		StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		// NOTE: We are not enabling the checkpointing!
		env2.getConfig().disableSysoutLogging();
		env2.setNumberOfExecutionRetries(0);
		env2.setParallelism(parallelism);


		// write a sequence from 0 to 99 to each of the 3 partitions.
		writeSequence(env1, topicName, 100, parallelism);


		// the readSequence operation sleeps for 20 ms between each record.
		// setting a delay of 25*20 = 500 for the commit interval makes
		// sure that we commit roughly 3-4 times while reading, however
		// at least once.
		Properties readProps = new Properties();
		readProps.putAll(standardProps);
		readProps.setProperty("auto.commit.interval.ms", "500");

		// read so that the offset can be committed to ZK
		readSequence(env2, readProps, parallelism, topicName, 100, 0);

		// get the offset
		CuratorFramework curatorFramework = ((KafkaTestEnvironmentImpl)kafkaServer).createCuratorClient();

		long o1 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, standardCC.groupId(), topicName, 0);
		long o2 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, standardCC.groupId(), topicName, 1);
		long o3 = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, standardCC.groupId(), topicName, 2);

		LOG.info("Got final offsets from zookeeper o1={}, o2={}, o3={}", o1, o2, o3);

		// ensure that the offset has been committed
		boolean atLeastOneOffsetSet = (o1 > 0 && o1 <= 100) ||
				(o2 > 0 && o2 <= 100) ||
				(o3 > 0 && o3 <= 100);
		assertTrue("Expecting at least one offset to be set o1="+o1+" o2="+o2+" o3="+o3, atLeastOneOffsetSet);
		//assertTrue("Offset of o1=" + o1 + " was not in range", o1 > 0 && o1 <= 100);
		//assertTrue("Offset of o2=" + o2 + " was not in range", o2 > 0 && o2 <= 100);
		//assertTrue("Offset of o3=" + o3 + " was not in range", o3 > 0 && o3 <= 100);

		deleteTestTopic(topicName);
	}
}
