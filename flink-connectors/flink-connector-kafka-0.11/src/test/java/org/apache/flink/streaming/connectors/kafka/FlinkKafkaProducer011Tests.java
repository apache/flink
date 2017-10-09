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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * IT cases for the {@link FlinkKafkaProducer011}.
 */
@SuppressWarnings("serial")
public class FlinkKafkaProducer011Tests extends KafkaTestBase {
	protected String transactionalId;
	protected Properties extraProperties;

	protected TypeInformationSerializationSchema<Integer> integerSerializationSchema =
			new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
	protected KeyedSerializationSchema<Integer> integerKeyedSerializationSchema =
			new KeyedSerializationSchemaWrapper(integerSerializationSchema);

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

	@Test(timeout = 120_000L)
	public void testFlinkKafkaProducer011FailBeforeNotify() throws Exception {
		String topic = "flink-kafka-producer-fail-before-notify";

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic);

		testHarness.setup();
		testHarness.open();
		testHarness.initializeState(null);
		testHarness.processElement(42, 0);
		testHarness.snapshot(0, 1);
		testHarness.processElement(43, 2);
		OperatorStateHandles snapshot = testHarness.snapshot(1, 3);

		int leaderId = kafkaServer.getLeaderToShutDown(topic);
		failBroker(leaderId);

		try {
			testHarness.processElement(44, 4);
			testHarness.snapshot(2, 5);
			assertFalse(true);
		}
		catch (Exception ex) {
			// expected
		}
		try {
			testHarness.close();
		}
		catch (Exception ex) {
		}

		kafkaServer.restartBroker(leaderId);

		testHarness = createTestHarness(topic);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.close();

		assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42, 43), 30_000L);

		deleteTestTopic(topic);
	}

	@Test(timeout = 120_000L)
	public void testFlinkKafkaProducer011FailTransactionCoordinatorBeforeNotify() throws Exception {
		String topic = "flink-kafka-producer-fail-transaction-coordinator-before-notify";

		Properties properties = createProperties();

		FlinkKafkaProducer011<Integer> kafkaProducer = new FlinkKafkaProducer011<>(
			topic,
			integerKeyedSerializationSchema,
			properties,
			FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness1 = new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer),
			IntSerializer.INSTANCE);

		testHarness1.setup();
		testHarness1.open();
		testHarness1.initializeState(null);
		testHarness1.processElement(42, 0);
		testHarness1.snapshot(0, 1);
		testHarness1.processElement(43, 2);
		int transactionCoordinatorId = kafkaProducer.getTransactionCoordinatorId();
		OperatorStateHandles snapshot = testHarness1.snapshot(1, 3);

		failBroker(transactionCoordinatorId);

		try {
			testHarness1.processElement(44, 4);
			testHarness1.notifyOfCompletedCheckpoint(1);
			testHarness1.close();
		}
		catch (Exception ex) {
			// Expected... some random exception could be thrown by any of the above operations.
		}
		finally {
			kafkaServer.restartBroker(transactionCoordinatorId);
		}

		try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness2 = createTestHarness(topic)) {
			testHarness2.setup();
			testHarness2.initializeState(snapshot);
			testHarness2.open();
		}

		assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42, 43), 30_000L);

		deleteTestTopic(topic);
	}

	/**
	 * This tests checks whether FlinkKafkaProducer011 correctly aborts lingering transactions after a failure.
	 * If such transactions were left alone lingering it consumers would be unable to read committed records
	 * that were created after this lingering transaction.
	 */
	@Test(timeout = 120_000L)
	public void testFailBeforeNotifyAndResumeWorkAfterwards() throws Exception {
		String topic = "flink-kafka-producer-fail-before-notify";

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic);

		testHarness.setup();
		testHarness.open();
		testHarness.processElement(42, 0);
		testHarness.snapshot(0, 1);
		testHarness.processElement(43, 2);
		OperatorStateHandles snapshot1 = testHarness.snapshot(1, 3);

		testHarness.processElement(44, 4);
		testHarness.snapshot(2, 5);
		testHarness.processElement(45, 6);

		// do not close previous testHarness to make sure that closing do not clean up something (in case of failure
		// there might not be any close)
		testHarness = createTestHarness(topic);
		testHarness.setup();
		// restore from snapshot1, transactions with records 44 and 45 should be aborted
		testHarness.initializeState(snapshot1);
		testHarness.open();

		// write and commit more records, after potentially lingering transactions
		testHarness.processElement(46, 7);
		testHarness.snapshot(4, 8);
		testHarness.processElement(47, 9);
		testHarness.notifyOfCompletedCheckpoint(4);

		//now we should have:
		// - records 42 and 43 in committed transactions
		// - aborted transactions with records 44 and 45
		// - committed transaction with record 46
		// - pending transaction with record 47
		assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42, 43, 46), 30_000L);

		testHarness.close();
		deleteTestTopic(topic);
	}

	private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness(String topic) throws Exception {
		Properties properties = createProperties();

		FlinkKafkaProducer011<Integer> kafkaProducer = new FlinkKafkaProducer011<>(
			topic,
			integerKeyedSerializationSchema,
			properties,
			FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

		return new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer),
			IntSerializer.INSTANCE);
	}

	private Properties createProperties() {
		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		properties.put(FlinkKafkaProducer011.KEY_DISABLE_METRICS, "true");
		return properties;
	}

	@Test
	public void testRecoverCommittedTransaction() throws Exception {
		String topic = "flink-kafka-producer-recover-committed-transaction";

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic);

		testHarness.setup();
		testHarness.open(); // producerA - start transaction (txn) 0
		testHarness.processElement(42, 0); // producerA - write 42 in txn 0
		OperatorStateHandles checkpoint0 = testHarness.snapshot(0, 1); // producerA - pre commit txn 0, producerB - start txn 1
		testHarness.processElement(43, 2); // producerB - write 43 in txn 1
		testHarness.notifyOfCompletedCheckpoint(0); // producerA - commit txn 0 and return to the pool
		testHarness.snapshot(1, 3); // producerB - pre txn 1,  producerA - start txn 2
		testHarness.processElement(44, 4); // producerA - write 44 in txn 2
		testHarness.close(); // producerA - abort txn 2

		testHarness = createTestHarness(topic);
		testHarness.initializeState(checkpoint0); // recover state 0 - producerA recover and commit txn 0
		testHarness.close();

		assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42), 30_000L);

		deleteTestTopic(topic);
	}

	@Test
	public void testRunOutOfProducersInThePool() throws Exception {
		String topic = "flink-kafka-run-out-of-producers";

		try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic)) {

			testHarness.setup();
			testHarness.open();

			for (int i = 0; i < FlinkKafkaProducer011.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE * 2; i++) {
				testHarness.processElement(i, i * 2);
				testHarness.snapshot(i, i * 2 + 1);
			}
		}
		catch (Exception ex) {
			if (!ex.getCause().getMessage().startsWith("Too many ongoing")) {
				throw ex;
			}
		}
		deleteTestTopic(topic);
	}

	// shut down a Kafka broker
	private void failBroker(int brokerId) {
		KafkaServer toShutDown = null;
		for (KafkaServer server : kafkaServer.getBrokers()) {

			if (kafkaServer.getBrokerId(server) == brokerId) {
				toShutDown = server;
				break;
			}
		}

		if (toShutDown == null) {
			StringBuilder listOfBrokers = new StringBuilder();
			for (KafkaServer server : kafkaServer.getBrokers()) {
				listOfBrokers.append(kafkaServer.getBrokerId(server));
				listOfBrokers.append(" ; ");
			}

			throw new IllegalArgumentException("Cannot find broker to shut down: " + brokerId
				+ " ; available brokers: " + listOfBrokers.toString());
		} else {
			toShutDown.shutdown();
			toShutDown.awaitShutdown();
		}
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
}
