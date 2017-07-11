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

import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that the partition assignment is deterministic and stable.
 */
public class KafkaConsumerPartitionAssignmentTest {

	@Test
	public void testPartitionsEqualConsumers() {
		try {
			List<KafkaTopicPartition> inPartitions = Arrays.asList(
				new KafkaTopicPartition("test-topic", 0),
				new KafkaTopicPartition("test-topic", 1),
				new KafkaTopicPartition("test-topic", 2),
				new KafkaTopicPartition("test-topic", 3));

			for (int subtaskIndex = 0; subtaskIndex < inPartitions.size(); subtaskIndex++) {
				Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets = new HashMap<>();
				FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
					subscribedPartitionsToStartOffsets,
					inPartitions,
					subtaskIndex,
					inPartitions.size(),
					StartupMode.GROUP_OFFSETS,
					null);

				List<KafkaTopicPartition> subscribedPartitions = new ArrayList<>(subscribedPartitionsToStartOffsets.keySet());

				assertEquals(1, subscribedPartitions.size());
				assertTrue(contains(inPartitions, subscribedPartitions.get(0).getPartition()));
				assertEquals(getExpectedSubtaskIndex(subscribedPartitions.get(0), 4), subtaskIndex);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultiplePartitionsPerConsumers() {
		try {
			final int[] partitionIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

			final List<KafkaTopicPartition> partitions = new ArrayList<>();
			final Set<KafkaTopicPartition> allPartitions = new HashSet<>();

			for (int p : partitionIDs) {
				KafkaTopicPartition part = new KafkaTopicPartition("test-topic", p);
				partitions.add(part);
				allPartitions.add(part);
			}

			final int numConsumers = 3;
			final int minPartitionsPerConsumer = partitions.size() / numConsumers;
			final int maxPartitionsPerConsumer = partitions.size() / numConsumers + 1;

			for (int subtaskIndex = 0; subtaskIndex < numConsumers; subtaskIndex++) {
				Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets = new HashMap<>();
				FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
					subscribedPartitionsToStartOffsets,
					partitions,
					subtaskIndex,
					numConsumers,
					StartupMode.GROUP_OFFSETS,
					null);

				List<KafkaTopicPartition> subscribedPartitions = new ArrayList<>(subscribedPartitionsToStartOffsets.keySet());

				assertTrue(subscribedPartitions.size() >= minPartitionsPerConsumer);
				assertTrue(subscribedPartitions.size() <= maxPartitionsPerConsumer);
				for (KafkaTopicPartition subscribedPartition : subscribedPartitions) {
					assertEquals(getExpectedSubtaskIndex(subscribedPartition, numConsumers), subtaskIndex);
				}

				for (KafkaTopicPartition p : subscribedPartitions) {
					// check that the element was actually contained
					assertTrue(allPartitions.remove(p));
				}
			}

			// all partitions must have been assigned
			assertTrue(allPartitions.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPartitionsFewerThanConsumers() {
		try {
			List<KafkaTopicPartition> inPartitions = Arrays.asList(
					new KafkaTopicPartition("test-topic", 4),
					new KafkaTopicPartition("test-topic", 52),
					new KafkaTopicPartition("test-topic", 17),
					new KafkaTopicPartition("test-topic", 1));

			final Set<KafkaTopicPartition> allPartitions = new HashSet<>();
			allPartitions.addAll(inPartitions);

			final int numConsumers = 2 * inPartitions.size() + 3;

			for (int subtaskIndex = 0; subtaskIndex < numConsumers; subtaskIndex++) {
				Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets = new HashMap<>();
				FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
					subscribedPartitionsToStartOffsets,
					inPartitions,
					subtaskIndex,
					numConsumers,
					StartupMode.GROUP_OFFSETS,
					null);

				List<KafkaTopicPartition> subscribedPartitions = new ArrayList<>(subscribedPartitionsToStartOffsets.keySet());

				assertTrue(subscribedPartitions.size() <= 1);
				for (KafkaTopicPartition subscribedPartition : subscribedPartitions) {
					assertEquals(getExpectedSubtaskIndex(subscribedPartition, numConsumers), subtaskIndex);
				}

				for (KafkaTopicPartition p : subscribedPartitions) {
					// check that the element was actually contained
					assertTrue(allPartitions.remove(p));
				}
			}

			// all partitions must have been assigned
			assertTrue(allPartitions.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAssignEmptyPartitions() {
		try {
			List<KafkaTopicPartition> ep = new ArrayList<>();
			Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets = new HashMap<>();
			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets,
				ep,
				2,
				4,
				StartupMode.GROUP_OFFSETS,
				null);
			assertTrue(subscribedPartitionsToStartOffsets.entrySet().isEmpty());

			subscribedPartitionsToStartOffsets = new HashMap<>();
			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets,
				ep,
				0,
				1,
				StartupMode.GROUP_OFFSETS,
				null);
			assertTrue(subscribedPartitionsToStartOffsets.entrySet().isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGrowingPartitionsRemainsStable() {
		try {
			final int[] newPartitionIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
			List<KafkaTopicPartition> newPartitions = new ArrayList<>();

			for (int p : newPartitionIDs) {
				KafkaTopicPartition part = new KafkaTopicPartition("test-topic", p);
				newPartitions.add(part);
			}

			List<KafkaTopicPartition> initialPartitions = newPartitions.subList(0, 7);

			final Set<KafkaTopicPartition> allNewPartitions = new HashSet<>(newPartitions);
			final Set<KafkaTopicPartition> allInitialPartitions = new HashSet<>(initialPartitions);

			final int numConsumers = 3;
			final int minInitialPartitionsPerConsumer = initialPartitions.size() / numConsumers;
			final int maxInitialPartitionsPerConsumer = initialPartitions.size() / numConsumers + 1;
			final int minNewPartitionsPerConsumer = newPartitions.size() / numConsumers;
			final int maxNewPartitionsPerConsumer = newPartitions.size() / numConsumers + 1;

			Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets1 = new HashMap<>();
			Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets2 = new HashMap<>();
			Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets3 = new HashMap<>();

			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets1,
				initialPartitions,
				0,
				numConsumers,
				StartupMode.GROUP_OFFSETS,
				null);

			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets2,
				initialPartitions,
				1,
				numConsumers,
				StartupMode.GROUP_OFFSETS,
				null);

			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets3,
				initialPartitions,
				2,
				numConsumers,
				StartupMode.GROUP_OFFSETS,
				null);

			List<KafkaTopicPartition> subscribedPartitions1 = new ArrayList<>(subscribedPartitionsToStartOffsets1.keySet());
			List<KafkaTopicPartition> subscribedPartitions2 = new ArrayList<>(subscribedPartitionsToStartOffsets2.keySet());
			List<KafkaTopicPartition> subscribedPartitions3 = new ArrayList<>(subscribedPartitionsToStartOffsets3.keySet());

			assertTrue(subscribedPartitions1.size() >= minInitialPartitionsPerConsumer);
			assertTrue(subscribedPartitions1.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(subscribedPartitions2.size() >= minInitialPartitionsPerConsumer);
			assertTrue(subscribedPartitions2.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(subscribedPartitions3.size() >= minInitialPartitionsPerConsumer);
			assertTrue(subscribedPartitions3.size() <= maxInitialPartitionsPerConsumer);

			for (KafkaTopicPartition p : subscribedPartitions1) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
				assertEquals(getExpectedSubtaskIndex(p, numConsumers), 0);
			}

			for (KafkaTopicPartition p : subscribedPartitions2) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
				assertEquals(getExpectedSubtaskIndex(p, numConsumers), 1);
			}

			for (KafkaTopicPartition p : subscribedPartitions3) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
				assertEquals(getExpectedSubtaskIndex(p, numConsumers), 2);
			}

			// all partitions must have been assigned
			assertTrue(allInitialPartitions.isEmpty());

			// grow the set of partitions and distribute anew

			subscribedPartitionsToStartOffsets1 = new HashMap<>();
			subscribedPartitionsToStartOffsets2 = new HashMap<>();
			subscribedPartitionsToStartOffsets3 = new HashMap<>();

			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets1,
				newPartitions,
				0,
				numConsumers,
				StartupMode.GROUP_OFFSETS,
				null);

			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets2,
				newPartitions,
				1,
				numConsumers,
				StartupMode.GROUP_OFFSETS,
				null);

			FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
				subscribedPartitionsToStartOffsets3,
				newPartitions,
				2,
				numConsumers,
				StartupMode.GROUP_OFFSETS,
				null);

			List<KafkaTopicPartition> subscribedPartitions1New = new ArrayList<>(subscribedPartitionsToStartOffsets1.keySet());
			List<KafkaTopicPartition> subscribedPartitions2New = new ArrayList<>(subscribedPartitionsToStartOffsets2.keySet());
			List<KafkaTopicPartition> subscribedPartitions3New = new ArrayList<>(subscribedPartitionsToStartOffsets3.keySet());

			// new partitions must include all old partitions

			assertTrue(subscribedPartitions1New.size() > subscribedPartitions1.size());
			assertTrue(subscribedPartitions2New.size() > subscribedPartitions2.size());
			assertTrue(subscribedPartitions3New.size() > subscribedPartitions3.size());

			assertTrue(subscribedPartitions1New.containsAll(subscribedPartitions1));
			assertTrue(subscribedPartitions2New.containsAll(subscribedPartitions2));
			assertTrue(subscribedPartitions3New.containsAll(subscribedPartitions3));

			assertTrue(subscribedPartitions1New.size() >= minNewPartitionsPerConsumer);
			assertTrue(subscribedPartitions1New.size() <= maxNewPartitionsPerConsumer);
			assertTrue(subscribedPartitions2New.size() >= minNewPartitionsPerConsumer);
			assertTrue(subscribedPartitions2New.size() <= maxNewPartitionsPerConsumer);
			assertTrue(subscribedPartitions3New.size() >= minNewPartitionsPerConsumer);
			assertTrue(subscribedPartitions3New.size() <= maxNewPartitionsPerConsumer);

			for (KafkaTopicPartition p : subscribedPartitions1New) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
				assertEquals(getExpectedSubtaskIndex(p, numConsumers), 0);
			}
			for (KafkaTopicPartition p : subscribedPartitions2New) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
				assertEquals(getExpectedSubtaskIndex(p, numConsumers), 1);
			}
			for (KafkaTopicPartition p : subscribedPartitions3New) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
				assertEquals(getExpectedSubtaskIndex(p, numConsumers), 2);
			}

			// all partitions must have been assigned
			assertTrue(allNewPartitions.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDeterministicAssignmentWithDifferentFetchedPartitionOrdering() {
		final int numSubtasks = 4;

		try {
			List<KafkaTopicPartition> inPartitions = Arrays.asList(
				new KafkaTopicPartition("test-topic", 0),
				new KafkaTopicPartition("test-topic", 1),
				new KafkaTopicPartition("test-topic", 2),
				new KafkaTopicPartition("test-topic", 3),
				new KafkaTopicPartition("test-topic2", 0),
				new KafkaTopicPartition("test-topic2", 1));

			List<KafkaTopicPartition> inPartitionsOutOfOrder = Arrays.asList(
				new KafkaTopicPartition("test-topic", 3),
				new KafkaTopicPartition("test-topic", 1),
				new KafkaTopicPartition("test-topic2", 1),
				new KafkaTopicPartition("test-topic", 0),
				new KafkaTopicPartition("test-topic2", 0),
				new KafkaTopicPartition("test-topic", 2));

			for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
				Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets = new HashMap<>();
				Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsetsOutOfOrder = new HashMap<>();

				FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
					subscribedPartitionsToStartOffsets,
					inPartitions,
					subtaskIndex,
					inPartitions.size(),
					StartupMode.GROUP_OFFSETS,
					null);

				FlinkKafkaConsumerBase.initializeSubscribedPartitionsToStartOffsets(
					subscribedPartitionsToStartOffsetsOutOfOrder,
					inPartitionsOutOfOrder,
					subtaskIndex,
					inPartitions.size(),
					StartupMode.GROUP_OFFSETS,
					null);

				// the subscribed partitions should be identical, regardless of the input partition ordering
				assertEquals(subscribedPartitionsToStartOffsets, subscribedPartitionsToStartOffsetsOutOfOrder);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static int getExpectedSubtaskIndex(KafkaTopicPartition partition, int numTasks) {
		return Math.abs(partition.hashCode() % numTasks);
	}

	private boolean contains(List<KafkaTopicPartition> inPartitions, int partition) {
		for (KafkaTopicPartition ktp : inPartitions) {
			if (ktp.getPartition() == partition) {
				return true;
			}
		}
		return false;
	}

}
