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
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import kafka.server.KafkaServer;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * IT cases for the {@link FlinkKafkaProducer011}.
 */
@SuppressWarnings("serial")
public class FlinkKafkaProducer011ITCase extends KafkaTestBase {
	protected String transactionalId;
	protected Properties extraProperties;

	protected TypeInformationSerializationSchema<Integer> integerSerializationSchema =
			new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
	protected KeyedSerializationSchema<Integer> integerKeyedSerializationSchema =
			new KeyedSerializationSchemaWrapper<>(integerSerializationSchema);

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

	@Test
	public void resourceCleanUpNone() throws Exception {
		resourceCleanUp(Semantic.NONE);
	}

	@Test
	public void resourceCleanUpAtLeastOnce() throws Exception {
		resourceCleanUp(Semantic.AT_LEAST_ONCE);
	}

	/**
	 * This tests checks whether there is some resource leak in form of growing threads number.
	 */
	public void resourceCleanUp(Semantic semantic) throws Exception {
		String topic = "flink-kafka-producer-resource-cleanup-" + semantic;

		final int allowedEpsilonThreadCountGrow = 50;

		Optional<Integer> initialActiveThreads = Optional.empty();
		for (int i = 0; i < allowedEpsilonThreadCountGrow * 2; i++) {
			try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness1 =
					createTestHarness(topic, 1, 1, 0, semantic)) {
				testHarness1.setup();
				testHarness1.open();
			}

			if (initialActiveThreads.isPresent()) {
				assertThat("active threads count",
					Thread.activeCount(),
					lessThan(initialActiveThreads.get() + allowedEpsilonThreadCountGrow));
			}
			else {
				initialActiveThreads = Optional.of(Thread.activeCount());
			}
		}
	}

	/**
	 * This test ensures that transactions reusing transactional.ids (after returning to the pool) will not clash
	 * with previous transactions using same transactional.ids.
	 */
	@Test
	public void testRestoreToCheckpointAfterExceedingProducersPool() throws Exception {
		String topic = "flink-kafka-producer-fail-before-notify";

		try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness1 = createTestHarness(topic)) {
			testHarness1.setup();
			testHarness1.open();
			testHarness1.processElement(42, 0);
			OperatorSubtaskState snapshot = testHarness1.snapshot(0, 0);
			testHarness1.processElement(43, 0);
			testHarness1.notifyOfCompletedCheckpoint(0);
			try {
				for (int i = 0; i < FlinkKafkaProducer011.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE; i++) {
					testHarness1.snapshot(i + 1, 0);
					testHarness1.processElement(i, 0);
				}
				throw new IllegalStateException("This should not be reached.");
			}
			catch (Exception ex) {
				if (!isCausedBy(FlinkKafka011ErrorCode.PRODUCERS_POOL_EMPTY, ex)) {
					throw ex;
				}
			}

			// Resume transactions before testHarness1 is being closed (in case of failures close() might not be called)
			try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness2 = createTestHarness(topic)) {
				testHarness2.setup();
				// restore from snapshot1, transactions with records 43 and 44 should be aborted
				testHarness2.initializeState(snapshot);
				testHarness2.open();
			}

			assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42), 30_000L);
			deleteTestTopic(topic);
		}
		catch (Exception ex) {
			// testHarness1 will be fenced off after creating and closing testHarness2
			if (!findThrowable(ex, ProducerFencedException.class).isPresent()) {
				throw ex;
			}
		}
	}

	@Test
	public void testFlinkKafkaProducer011FailBeforeNotify() throws Exception {
		String topic = "flink-kafka-producer-fail-before-notify";

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic);

		testHarness.setup();
		testHarness.open();
		testHarness.initializeState(null);
		testHarness.processElement(42, 0);
		testHarness.snapshot(0, 1);
		testHarness.processElement(43, 2);
		OperatorSubtaskState snapshot = testHarness.snapshot(1, 3);

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

	@Test
	public void testFlinkKafkaProducer011FailTransactionCoordinatorBeforeNotify() throws Exception {
		String topic = "flink-kafka-producer-fail-transaction-coordinator-before-notify";

		Properties properties = createProperties();

		FlinkKafkaProducer011<Integer> kafkaProducer = new FlinkKafkaProducer011<>(
			topic,
			integerKeyedSerializationSchema,
			properties,
			Semantic.EXACTLY_ONCE);

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
		OperatorSubtaskState snapshot = testHarness1.snapshot(1, 3);

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
	@Test
	public void testFailBeforeNotifyAndResumeWorkAfterwards() throws Exception {
		String topic = "flink-kafka-producer-fail-before-notify";

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic);

		testHarness.setup();
		testHarness.open();
		testHarness.processElement(42, 0);
		testHarness.snapshot(0, 1);
		testHarness.processElement(43, 2);
		OperatorSubtaskState snapshot1 = testHarness.snapshot(1, 3);

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

	@Test
	public void testFailAndRecoverSameCheckpointTwice() throws Exception {
		String topic = "flink-kafka-producer-fail-and-recover-same-checkpoint-twice";

		OperatorSubtaskState snapshot1;
		try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic)) {
			testHarness.setup();
			testHarness.open();
			testHarness.processElement(42, 0);
			testHarness.snapshot(0, 1);
			testHarness.processElement(43, 2);
			snapshot1 = testHarness.snapshot(1, 3);

			testHarness.processElement(44, 4);
		}

		try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic)) {
			testHarness.setup();
			// restore from snapshot1, transactions with records 44 and 45 should be aborted
			testHarness.initializeState(snapshot1);
			testHarness.open();

			// write and commit more records, after potentially lingering transactions
			testHarness.processElement(44, 7);
			testHarness.snapshot(2, 8);
			testHarness.processElement(45, 9);
		}

		try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic)) {
			testHarness.setup();
			// restore from snapshot1, transactions with records 44 and 45 should be aborted
			testHarness.initializeState(snapshot1);
			testHarness.open();

			// write and commit more records, after potentially lingering transactions
			testHarness.processElement(44, 7);
			testHarness.snapshot(3, 8);
			testHarness.processElement(45, 9);
		}

		//now we should have:
		// - records 42 and 43 in committed transactions
		// - aborted transactions with records 44 and 45
		assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42, 43), 30_000L);
		deleteTestTopic(topic);
	}

	/**
	 * This tests checks whether FlinkKafkaProducer011 correctly aborts lingering transactions after a failure,
	 * which happened before first checkpoint and was followed up by reducing the parallelism.
	 * If such transactions were left alone lingering it consumers would be unable to read committed records
	 * that were created after this lingering transaction.
	 */
	@Test
	public void testScaleDownBeforeFirstCheckpoint() throws Exception {
		String topic = "scale-down-before-first-checkpoint";

		List<AutoCloseable> operatorsToClose = new ArrayList<>();
		int preScaleDownParallelism = Math.max(2, FlinkKafkaProducer011.SAFE_SCALE_DOWN_FACTOR);
		for (int subtaskIndex = 0; subtaskIndex < preScaleDownParallelism; subtaskIndex++) {
			OneInputStreamOperatorTestHarness<Integer, Object> preScaleDownOperator = createTestHarness(
				topic,
				preScaleDownParallelism,
				preScaleDownParallelism,
				subtaskIndex,
				Semantic.EXACTLY_ONCE);

			preScaleDownOperator.setup();
			preScaleDownOperator.open();
			preScaleDownOperator.processElement(subtaskIndex * 2, 0);
			preScaleDownOperator.snapshot(0, 1);
			preScaleDownOperator.processElement(subtaskIndex * 2 + 1, 2);

			operatorsToClose.add(preScaleDownOperator);
		}

		// do not close previous testHarnesses to make sure that closing do not clean up something (in case of failure
		// there might not be any close)

		// After previous failure simulate restarting application with smaller parallelism
		OneInputStreamOperatorTestHarness<Integer, Object> postScaleDownOperator1 = createTestHarness(topic, 1, 1, 0, Semantic.EXACTLY_ONCE);

		postScaleDownOperator1.setup();
		postScaleDownOperator1.open();

		// write and commit more records, after potentially lingering transactions
		postScaleDownOperator1.processElement(46, 7);
		postScaleDownOperator1.snapshot(4, 8);
		postScaleDownOperator1.processElement(47, 9);
		postScaleDownOperator1.notifyOfCompletedCheckpoint(4);

		//now we should have:
		// - records 42, 43, 44 and 45 in aborted transactions
		// - committed transaction with record 46
		// - pending transaction with record 47
		assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(46), 30_000L);

		postScaleDownOperator1.close();
		// ignore ProducerFencedExceptions, because postScaleDownOperator1 could reuse transactional ids.
		for (AutoCloseable operatorToClose : operatorsToClose) {
			closeIgnoringProducerFenced(operatorToClose);
		}
		deleteTestTopic(topic);
	}

	/**
	 * Each instance of FlinkKafkaProducer011 uses it's own pool of transactional ids. After the restore from checkpoint
	 * transactional ids are redistributed across the subtasks. In case of scale down, the surplus transactional ids
	 * are dropped. In case of scale up, new one are generated (for the new subtasks). This test make sure that sequence
	 * of scaling down and up again works fine. Especially it checks whether the newly generated ids in scaling up
	 * do not overlap with ids that were used before scaling down. For example we start with 4 ids and parallelism 4:
	 * [1], [2], [3], [4] - one assigned per each subtask
	 * we scale down to parallelism 2:
	 * [1, 2], [3, 4] - first subtask got id 1 and 2, second got ids 3 and 4
	 * surplus ids are dropped from the pools and we scale up to parallelism 3:
	 * [1 or 2], [3 or 4], [???]
	 * new subtask have to generate new id(s), but he can not use ids that are potentially in use, so it has to generate
	 * new ones that are greater then 4.
	 */
	@Test
	public void testScaleUpAfterScalingDown() throws Exception {
		String topic = "scale-down-before-first-checkpoint";

		final int parallelism1 = 4;
		final int parallelism2 = 2;
		final int parallelism3 = 3;
		final int maxParallelism = Math.max(parallelism1, Math.max(parallelism2, parallelism3));

		List<OperatorStateHandle> operatorSubtaskState = repartitionAndExecute(
			topic,
			Collections.emptyList(),
			parallelism1,
			maxParallelism,
			IntStream.range(0, parallelism1).boxed().iterator());

		operatorSubtaskState = repartitionAndExecute(
			topic,
			operatorSubtaskState,
			parallelism2,
			maxParallelism,
			IntStream.range(parallelism1,  parallelism1 + parallelism2).boxed().iterator());

		operatorSubtaskState = repartitionAndExecute(
			topic,
			operatorSubtaskState,
			parallelism3,
			maxParallelism,
			IntStream.range(parallelism1 + parallelism2,  parallelism1 + parallelism2 + parallelism3).boxed().iterator());

		// After each previous repartitionAndExecute call, we are left with some lingering transactions, that would
		// not allow us to read all committed messages from the topic. Thus we initialize operators from
		// OperatorSubtaskState once more, but without any new data. This should terminate all ongoing transactions.

		operatorSubtaskState = repartitionAndExecute(
			topic,
			operatorSubtaskState,
			1,
			maxParallelism,
			Collections.emptyIterator());

		assertExactlyOnceForTopic(
			createProperties(),
			topic,
			0,
			IntStream.range(0, parallelism1 + parallelism2 + parallelism3).boxed().collect(Collectors.toList()),
			30_000L);
		deleteTestTopic(topic);
	}

	private List<OperatorStateHandle> repartitionAndExecute(
			String topic,
			List<OperatorStateHandle> inputStates,
			int parallelism,
			int maxParallelism,
			Iterator<Integer> inputData) throws Exception {

		List<OperatorStateHandle> outputStates = new ArrayList<>();
		List<OneInputStreamOperatorTestHarness<Integer, Object>> testHarnesses = new ArrayList<>();

		for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
			OneInputStreamOperatorTestHarness<Integer, Object> testHarness =
				createTestHarness(topic, maxParallelism, parallelism, subtaskIndex, Semantic.EXACTLY_ONCE);
			testHarnesses.add(testHarness);

			testHarness.setup();

			testHarness.initializeState(new OperatorSubtaskState(
				new StateObjectCollection<>(inputStates),
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.empty()));
			testHarness.open();

			if (inputData.hasNext()) {
				int nextValue = inputData.next();
				testHarness.processElement(nextValue, 0);
				OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

				outputStates.addAll(snapshot.getManagedOperatorState());
				checkState(snapshot.getRawOperatorState().isEmpty(), "Unexpected raw operator state");
				checkState(snapshot.getManagedKeyedState().isEmpty(), "Unexpected managed keyed state");
				checkState(snapshot.getRawKeyedState().isEmpty(), "Unexpected raw keyed state");

				for (int i = 1; i < FlinkKafkaProducer011.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE - 1; i++) {
					testHarness.processElement(-nextValue, 0);
					testHarness.snapshot(i, 0);
				}
			}
		}

		for (OneInputStreamOperatorTestHarness<Integer, Object> testHarness : testHarnesses) {
			testHarness.close();
		}

		return outputStates;
	}

	@Test
	public void testRecoverCommittedTransaction() throws Exception {
		String topic = "flink-kafka-producer-recover-committed-transaction";

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topic);

		testHarness.setup();
		testHarness.open(); // producerA - start transaction (txn) 0
		testHarness.processElement(42, 0); // producerA - write 42 in txn 0
		OperatorSubtaskState checkpoint0 = testHarness.snapshot(0, 1); // producerA - pre commit txn 0, producerB - start txn 1
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

	private void closeIgnoringProducerFenced(AutoCloseable autoCloseable) throws Exception {
		try {
			autoCloseable.close();
		}
		catch (Exception ex) {
			if (!(ex.getCause() instanceof ProducerFencedException)) {
				throw ex;
			}
		}
	}

	private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness(String topic) throws Exception {
		return createTestHarness(topic, 1, 1, 0, Semantic.EXACTLY_ONCE);
	}

	private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness(
			String topic,
			int maxParallelism,
			int parallelism,
			int subtaskIndex,
			Semantic semantic) throws Exception {
		Properties properties = createProperties();

		FlinkKafkaProducer011<Integer> kafkaProducer = new FlinkKafkaProducer011<>(
			topic,
			integerKeyedSerializationSchema,
			properties,
			semantic);

		return new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer),
			maxParallelism,
			parallelism,
			subtaskIndex,
			IntSerializer.INSTANCE,
			new OperatorID(42, 44));
	}

	private Properties createProperties() {
		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		properties.put(FlinkKafkaProducer011.KEY_DISABLE_METRICS, "true");
		return properties;
	}

	private boolean isCausedBy(FlinkKafka011ErrorCode expectedErrorCode, Throwable ex) {
		Optional<FlinkKafka011Exception> cause = findThrowable(ex, FlinkKafka011Exception.class);
		if (cause.isPresent()) {
			return cause.get().getErrorCode().equals(expectedErrorCode);
		}
		return false;
	}
}
