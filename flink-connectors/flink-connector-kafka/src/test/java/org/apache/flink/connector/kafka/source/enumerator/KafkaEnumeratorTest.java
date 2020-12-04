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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.KafkaSourceTestEnv;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link KafkaSourceEnumerator}.
 */
public class KafkaEnumeratorTest {
	private static final int NUM_SUBTASKS = 3;
	private static final String DYNAMIC_TOPIC_NAME = "dynamic_topic";
	private static final int NUM_PARTITIONS_DYNAMIC_TOPIC = 4;

	private static final String TOPIC1 = "topic";
	private static final String TOPIC2 = "pattern-topic";

	private static final int READER0 = 0;
	private static final int READER1 = 1;
	private static final Set<String> PRE_EXISTING_TOPICS =
			new HashSet<>(Arrays.asList(TOPIC1, TOPIC2));
	private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
	private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
	private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;
	private static final boolean INCLUDE_DYNAMIC_TOPIC = true;
	private static final boolean EXCLUDE_DYNAMIC_TOPIC = false;

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
	public void testStartWithDiscoverPartitionsOnce() throws IOException {
		MockSplitEnumeratorContext<KafkaPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		try (KafkaSourceEnumerator enumerator = createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

			// Start the enumerator and it should schedule a one time task to discover and assign partitions.
			enumerator.start();
			assertTrue(context.getPeriodicCallables().isEmpty());
			assertEquals("A one time partition discovery callable should have been scheduled",
					1, context.getOneTimeCallables().size());
		}
	}

	@Test
	public void testStartWithPeriodicPartitionDiscovery() throws IOException {
		MockSplitEnumeratorContext<KafkaPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		try (KafkaSourceEnumerator enumerator = createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

			// Start the enumerator and it should schedule a one time task to discover and assign partitions.
			enumerator.start();
			assertTrue(context.getOneTimeCallables().isEmpty());
			assertEquals("A periodic partition discovery callable should have been scheduled",
					1, context.getPeriodicCallables().size());
		}
	}

	@Test
	public void testDiscoverPartitionsTriggersAssignments() throws Throwable {
		MockSplitEnumeratorContext<KafkaPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		try (KafkaSourceEnumerator enumerator = createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

			// Start the enumerator and it should schedule a one time task to discover and assign partitions.
			enumerator.start();

			// register reader 0.
			registerReader(context, enumerator, READER0);
			registerReader(context, enumerator, READER1);
			assertTrue(context.getSplitsAssignmentSequence().isEmpty());

			// Run the partition discover callable and check the partition assignment.
			context.runNextOneTimeCallable();

			// Verify assignments for reader 0.
			verifyLastReadersAssignments(context, Arrays.asList(READER0, READER1), PRE_EXISTING_TOPICS, 1);
		}
	}

	@Test
	public void testReaderRegistrationTriggersAssignments() throws Throwable {
		MockSplitEnumeratorContext<KafkaPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		try (KafkaSourceEnumerator enumerator = createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

			// Start the enumerator and it should schedule a one time task to discover and assign partitions.
			enumerator.start();
			context.runNextOneTimeCallable();
			assertTrue(context.getSplitsAssignmentSequence().isEmpty());

			registerReader(context, enumerator, READER0);
			verifyLastReadersAssignments(context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

			registerReader(context, enumerator, READER1);
			verifyLastReadersAssignments(context, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 2);
		}
	}

	@Test(timeout = 30000L)
	public void testDiscoverPartitionsPeriodically() throws Throwable {
		MockSplitEnumeratorContext<KafkaPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		try (KafkaSourceEnumerator enumerator =
					createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY, INCLUDE_DYNAMIC_TOPIC);
				AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {

			startEnumeratorAndRegisterReaders(context, enumerator);

			// invoke partition discovery callable again and there should be no new assignments.
			context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
			assertEquals("No assignments should be made because there is no partition change",
					2, context.getSplitsAssignmentSequence().size());

			// create the dynamic topic.
			adminClient
					.createTopics(Collections.singleton(
							new NewTopic(DYNAMIC_TOPIC_NAME, NUM_PARTITIONS_DYNAMIC_TOPIC, (short) 1)))
					.all().get();

			// invoke partition discovery callable again.
			while (true) {
				context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
				if (context.getSplitsAssignmentSequence().size() < 3) {
					Thread.sleep(10);
				} else {
					break;
				}
			}
			verifyLastReadersAssignments(
					context,
					Arrays.asList(READER0, READER1),
					Collections.singleton(DYNAMIC_TOPIC_NAME),
					3);
		} finally {
			try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
				adminClient.deleteTopics(Collections.singleton(DYNAMIC_TOPIC_NAME)).all().get();
			} catch (Exception e) {
				// Let it go.
			}
		}
	}

	@Test
	public void testAddSplitsBack() throws Throwable {
		MockSplitEnumeratorContext<KafkaPartitionSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		try (KafkaSourceEnumerator enumerator = createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

			startEnumeratorAndRegisterReaders(context, enumerator);

			// Simulate a reader failure.
			context.unregisterReader(READER0);
			enumerator.addSplitsBack(
					context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
					READER0);
			assertEquals("The added back splits should have not been assigned",
					2, context.getSplitsAssignmentSequence().size());

			// Simulate a reader recovery.
			registerReader(context, enumerator, READER0);
			verifyLastReadersAssignments(context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 3);
		}
	}

	@Test
	public void testWorkWithPreexistingAssignments() throws Throwable {
		final MockSplitEnumeratorContext<KafkaPartitionSplit> context1 = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		Map<Integer, Set<KafkaPartitionSplit>> preexistingAssignments;
		try (KafkaSourceEnumerator enumerator = createEnumerator(context1, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {
			startEnumeratorAndRegisterReaders(context1, enumerator);
			preexistingAssignments = asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
		}

		final MockSplitEnumeratorContext<KafkaPartitionSplit> context2 = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
		try (KafkaSourceEnumerator enumerator =
					createEnumerator(context2, ENABLE_PERIODIC_PARTITION_DISCOVERY, PRE_EXISTING_TOPICS, preexistingAssignments)) {
			enumerator.start();
			context2.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);

			registerReader(context2, enumerator, READER0);
			assertTrue(context2.getSplitsAssignmentSequence().isEmpty());

			registerReader(context2, enumerator, READER1);
			verifyLastReadersAssignments(context2, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 1);
		}
	}

	// -------------- some common startup sequence ---------------

	private void startEnumeratorAndRegisterReaders(
			MockSplitEnumeratorContext<KafkaPartitionSplit> context,
			KafkaSourceEnumerator enumerator) throws Throwable {
		// Start the enumerator and it should schedule a one time task to discover and assign partitions.
		enumerator.start();

		// register reader 0 before the partition discovery.
		registerReader(context, enumerator, READER0);
		assertTrue(context.getSplitsAssignmentSequence().isEmpty());

		// Run the partition discover callable and check the partition assignment.
		context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
		verifyLastReadersAssignments(context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

		// Register reader 1 after first partition discovery.
		registerReader(context, enumerator, READER1);
		verifyLastReadersAssignments(context, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 2);

	}

	// ----------------------------------------

	private KafkaSourceEnumerator createEnumerator(
			MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
			boolean enablePeriodicPartitionDiscovery) {
		return createEnumerator(enumContext, enablePeriodicPartitionDiscovery, EXCLUDE_DYNAMIC_TOPIC);
	}

	private KafkaSourceEnumerator createEnumerator(
			MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
			boolean enablePeriodicPartitionDiscovery,
			boolean includeDynamicTopic) {
		List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
		if (includeDynamicTopic) {
			topics.add(DYNAMIC_TOPIC_NAME);
		}
		return createEnumerator(enumContext, enablePeriodicPartitionDiscovery, topics, Collections.emptyMap());
	}

	/**
	 * Create the enumerator. For the purpose of the tests in this class we don't care about
	 * the subscriber and offsets initializer, so just use arbitrary settings.
	 */
	private KafkaSourceEnumerator createEnumerator(
			MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
			boolean enablePeriodicPartitionDiscovery,
			Collection<String> topicsToSubscribe,
			Map<Integer, Set<KafkaPartitionSplit>> currentAssignments) {
		// Use a TopicPatternSubscriber so that no exception if a subscribed topic hasn't been created yet.
		StringJoiner topicNameJoiner = new StringJoiner("|");
		topicsToSubscribe.forEach(topicNameJoiner::add);
		Pattern topicPattern = Pattern.compile(topicNameJoiner.toString());
		KafkaSubscriber subscriber = KafkaSubscriber.getTopicPatternSubscriber(topicPattern);

		OffsetsInitializer startingOffsetsInitializer = OffsetsInitializer.earliest();
		OffsetsInitializer stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();

		Properties props = new Properties(KafkaSourceTestEnv.getConsumerProperties(StringDeserializer.class));
		String partitionDiscoverInterval = enablePeriodicPartitionDiscovery ? "1" : "-1";
		props.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), partitionDiscoverInterval);

		return new KafkaSourceEnumerator(
				subscriber,
				startingOffsetsInitializer,
				stoppingOffsetsInitializer,
				props,
				enumContext,
				currentAssignments);
	}

	// ---------------------

	private void registerReader(
			MockSplitEnumeratorContext<KafkaPartitionSplit> context,
			KafkaSourceEnumerator enumerator,
			int reader) {
		context.registerReader(new ReaderInfo(reader, "location 0"));
		enumerator.addReader(reader);
	}

	private void verifyLastReadersAssignments(
		MockSplitEnumeratorContext<KafkaPartitionSplit> context,
		Collection<Integer> readers,
		Set<String> topics,
		int expectedAssignmentSeqSize) {
		verifyAssignments(
				getExpectedAssignments(new HashSet<>(readers), topics),
				context.getSplitsAssignmentSequence().get(expectedAssignmentSeqSize - 1).assignment());
	}

	private void verifyAssignments(
			Map<Integer, Set<TopicPartition>> expectedAssignments,
			Map<Integer, List<KafkaPartitionSplit>> actualAssignments) {
		actualAssignments.forEach((reader, splits) -> {
			Set<TopicPartition> expectedAssignmentsForReader = expectedAssignments.get(reader);
			assertNotNull(expectedAssignmentsForReader);
			assertEquals(expectedAssignmentsForReader.size(), splits.size());
			for (KafkaPartitionSplit split : splits) {
				assertTrue(expectedAssignmentsForReader.contains(split.getTopicPartition()));
			}
		});
	}

	private Map<Integer, Set<TopicPartition>> getExpectedAssignments(
			Set<Integer> readers,
			Set<String> topics) {
		Map<Integer, Set<TopicPartition>> expectedAssignments = new HashMap<>();
		Set<TopicPartition> allPartitions = new HashSet<>();

		if (topics.contains(DYNAMIC_TOPIC_NAME)) {
			for (int i = 0; i < NUM_PARTITIONS_DYNAMIC_TOPIC; i++) {
				allPartitions.add(new TopicPartition(DYNAMIC_TOPIC_NAME, i));
			}
		}

		for (TopicPartition tp : KafkaSourceTestEnv.getPartitionsForTopics(PRE_EXISTING_TOPICS)) {
			if (topics.contains(tp.topic())) {
				allPartitions.add(tp);
			}
		}

		for (TopicPartition tp : allPartitions) {
			int ownerReader = KafkaSourceEnumerator.getSplitOwner(tp, NUM_SUBTASKS);
			if (readers.contains(ownerReader)) {
				expectedAssignments.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(tp);
			}
		}
		return expectedAssignments;
	}

	private Map<Integer, Set<KafkaPartitionSplit>> asEnumState(Map<Integer, List<KafkaPartitionSplit>> assignments) {
		Map<Integer, Set<KafkaPartitionSplit>> enumState = new HashMap<>();
		assignments.forEach((reader, assignment) -> enumState.put(reader, new HashSet<>(assignment)));
		return enumState;
	}

	// -------------- private class ----------------

	private static class BlockingClosingContext extends MockSplitEnumeratorContext<KafkaPartitionSplit> {

		public BlockingClosingContext(int parallelism) {
			super(parallelism);
		}

		@Override
		public void close() {
			try {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e) {
				// let it go.
			}
		}
	}
}
