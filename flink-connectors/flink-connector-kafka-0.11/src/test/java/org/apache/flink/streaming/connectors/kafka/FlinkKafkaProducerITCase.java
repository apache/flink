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

import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Tests for our own {@link FlinkKafkaProducer}.
 */
@SuppressWarnings("serial")
public class FlinkKafkaProducerITCase extends KafkaTestBase {
	protected String transactionalId;
	protected Properties extraProperties;

	@BeforeClass
	public static void prepare() throws Exception {
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting KafkaTestBase ");
		LOG.info("-------------------------------------------------------------------------");

		Properties serverProperties = new Properties();
		serverProperties.put("transaction.state.log.num.partitions", Integer.toString(1));
		serverProperties.put("auto.leader.rebalance.enable", Boolean.toString(false));
		startClusters(KafkaTestEnvironment.createConfig()
			.setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS)
			.setSecureMode(false)
			.setHideKafkaBehindProxy(true)
			.setKafkaServerProperties(serverProperties));
	}

	@Before
	public void before() {
		transactionalId = UUID.randomUUID().toString();
		extraProperties = new Properties();
		extraProperties.putAll(standardProps);
		extraProperties.put("transactional.id", transactionalId);
		extraProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		extraProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		extraProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		extraProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		extraProperties.put("isolation.level", "read_committed");
	}

	@Test(timeout = 30000L)
	public void testHappyPath() throws IOException {
		String topicName = "flink-kafka-producer-happy-path";
		try (Producer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties)) {
			kafkaProducer.initTransactions();
			kafkaProducer.beginTransaction();
			kafkaProducer.send(new ProducerRecord<>(topicName, "42", "42"));
			kafkaProducer.commitTransaction();
		}
		assertRecord(topicName, "42", "42");
		deleteTestTopic(topicName);
	}

	@Test(timeout = 30000L)
	public void testResumeTransaction() throws IOException {
		String topicName = "flink-kafka-producer-resume-transaction";
		try (FlinkKafkaProducer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties)) {
			kafkaProducer.initTransactions();
			kafkaProducer.beginTransaction();
			kafkaProducer.send(new ProducerRecord<>(topicName, "42", "42"));
			kafkaProducer.flush();
			long producerId = kafkaProducer.getProducerId();
			short epoch = kafkaProducer.getEpoch();

			try (FlinkKafkaProducer<String, String> resumeProducer = new FlinkKafkaProducer<>(extraProperties)) {
				resumeProducer.resumeTransaction(producerId, epoch);
				resumeProducer.commitTransaction();
			}

			assertRecord(topicName, "42", "42");

			// this shouldn't throw - in case of network split, old producer might attempt to commit it's transaction
			kafkaProducer.commitTransaction();

			// this shouldn't fail also, for same reason as above
			try (FlinkKafkaProducer<String, String> resumeProducer = new FlinkKafkaProducer<>(extraProperties)) {
				resumeProducer.resumeTransaction(producerId, epoch);
				resumeProducer.commitTransaction();
			}
		}
		deleteTestTopic(topicName);
	}

	@Test(timeout = 30000L, expected = IllegalStateException.class)
	public void testPartitionsForAfterClosed() {
		FlinkKafkaProducer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties);
		kafkaProducer.close();
		kafkaProducer.partitionsFor("Topic");
	}

	@Test(timeout = 30000L, expected = IllegalStateException.class)
	public void testInitTransactionsAfterClosed() {
		FlinkKafkaProducer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties);
		kafkaProducer.close();
		kafkaProducer.initTransactions();
	}

	@Test(timeout = 30000L, expected = IllegalStateException.class)
	public void testBeginTransactionAfterClosed() {
		FlinkKafkaProducer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties);
		kafkaProducer.initTransactions();
		kafkaProducer.close();
		kafkaProducer.beginTransaction();
	}

	@Test(timeout = 30000L, expected = IllegalStateException.class)
	public void testCommitTransactionAfterClosed() {
		String topicName = "testCommitTransactionAfterClosed";
		FlinkKafkaProducer<String, String> kafkaProducer = getClosedProducer(topicName);
		kafkaProducer.commitTransaction();
	}

	@Test(timeout = 30000L, expected = IllegalStateException.class)
	public void testResumeTransactionAfterClosed() {
		String topicName = "testAbortTransactionAfterClosed";
		FlinkKafkaProducer<String, String> kafkaProducer = getClosedProducer(topicName);
		kafkaProducer.resumeTransaction(0L, (short) 1);
	}

	@Test(timeout = 30000L, expected = IllegalStateException.class)
	public void testAbortTransactionAfterClosed() {
		String topicName = "testAbortTransactionAfterClosed";
		FlinkKafkaProducer<String, String> kafkaProducer = getClosedProducer(topicName);
		kafkaProducer.abortTransaction();
		kafkaProducer.resumeTransaction(0L, (short) 1);
	}

	@Test(timeout = 30000L, expected = IllegalStateException.class)
	public void testFlushAfterClosed() {
		String topicName = "testCommitTransactionAfterClosed";
		FlinkKafkaProducer<String, String> kafkaProducer = getClosedProducer(topicName);
		kafkaProducer.flush();
	}

	@Test(timeout = 30000L)
	public void testProducerWhenCommitEmptyPartitionsToOutdatedTxnCoordinator() throws Exception {
		String topic = "flink-kafka-producer-txn-coordinator-changed";
		createTestTopic(topic, 1, 2);
		try (Producer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties)) {
			kafkaProducer.initTransactions();
			kafkaProducer.beginTransaction();
			restartBroker(kafkaServer.getLeaderToShutDown("__transaction_state"));
			kafkaProducer.flush();
			kafkaProducer.commitTransaction();
		}
		deleteTestTopic(topic);
	}

	private FlinkKafkaProducer<String, String> getClosedProducer(String topicName) {
		FlinkKafkaProducer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties);
		kafkaProducer.initTransactions();
		kafkaProducer.beginTransaction();
		kafkaProducer.send(new ProducerRecord<>(topicName, "42", "42"));
		kafkaProducer.close();
		return kafkaProducer;
	}

	private void assertRecord(String topicName, String expectedKey, String expectedValue) {
		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(extraProperties)) {
			kafkaConsumer.subscribe(Collections.singletonList(topicName));
			ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);

			ConsumerRecord<String, String> record = Iterables.getOnlyElement(records);
			assertEquals(expectedKey, record.key());
			assertEquals(expectedValue, record.value());
		}
	}

	private void restartBroker(int brokerId) {
		KafkaServer toRestart = null;
		for (KafkaServer server : kafkaServer.getBrokers()) {
			if (kafkaServer.getBrokerId(server) == brokerId) {
				toRestart = server;
			}
		}

		if (toRestart == null) {
			StringBuilder listOfBrokers = new StringBuilder();
			for (KafkaServer server : kafkaServer.getBrokers()) {
				listOfBrokers.append(kafkaServer.getBrokerId(server));
				listOfBrokers.append(" ; ");
			}

			throw new IllegalArgumentException("Cannot find broker to restart: " + brokerId
				+ " ; available brokers: " + listOfBrokers.toString());
		} else {
			toRestart.shutdown();
			toRestart.awaitShutdown();
			toRestart.startup();
		}
	}
}
