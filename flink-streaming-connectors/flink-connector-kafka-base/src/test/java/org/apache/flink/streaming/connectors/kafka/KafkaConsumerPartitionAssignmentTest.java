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

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests that the partition assignment is deterministic and stable.
 */
public class KafkaConsumerPartitionAssignmentTest {

	@Test
	public void testPartitionsEqualConsumers() {
		try {
			List<KafkaTopicPartition> inPartitions = Arrays.asList(
					new KafkaTopicPartition("test-topic", 4),
					new KafkaTopicPartition("test-topic", 52),
					new KafkaTopicPartition("test-topic", 17),
					new KafkaTopicPartition("test-topic", 1));

			for (int i = 0; i < inPartitions.size(); i++) {
				List<KafkaTopicPartition> parts = 
						FlinkKafkaConsumerBase.assignPartitions(inPartitions, inPartitions.size(), i);

				assertNotNull(parts);
				assertEquals(1, parts.size());
				assertTrue(contains(inPartitions, parts.get(0).getPartition()));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private boolean contains(List<KafkaTopicPartition> inPartitions, int partition) {
		for (KafkaTopicPartition ktp : inPartitions) {
			if (ktp.getPartition() == partition) {
				return true;
			}
		}
		return false;
	}

	@Test
	public void testMultiplePartitionsPerConsumers() {
		try {
			final int[] partitionIDs = {4, 52, 17, 1, 2, 3, 89, 42, 31, 127, 14};

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

			for (int i = 0; i < numConsumers; i++) {
				List<KafkaTopicPartition> parts = 
						FlinkKafkaConsumerBase.assignPartitions(partitions, numConsumers, i);

				assertNotNull(parts);
				assertTrue(parts.size() >= minPartitionsPerConsumer);
				assertTrue(parts.size() <= maxPartitionsPerConsumer);

				for (KafkaTopicPartition p : parts) {
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

			for (int i = 0; i < numConsumers; i++) {
				List<KafkaTopicPartition> parts = FlinkKafkaConsumerBase.assignPartitions(inPartitions, numConsumers, i);

				assertNotNull(parts);
				assertTrue(parts.size() <= 1);

				for (KafkaTopicPartition p : parts) {
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
			List<KafkaTopicPartition> parts1 = FlinkKafkaConsumerBase.assignPartitions(ep, 4, 2);
			assertNotNull(parts1);
			assertTrue(parts1.isEmpty());

			List<KafkaTopicPartition> parts2 = FlinkKafkaConsumerBase.assignPartitions(ep, 1, 0);
			assertNotNull(parts2);
			assertTrue(parts2.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGrowingPartitionsRemainsStable() {
		try {
			final int[] newPartitionIDs = {4, 52, 17, 1, 2, 3, 89, 42, 31, 127, 14};
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

			List<KafkaTopicPartition> parts1 = FlinkKafkaConsumerBase.assignPartitions(
					initialPartitions, numConsumers, 0);
			List<KafkaTopicPartition> parts2 = FlinkKafkaConsumerBase.assignPartitions(
					initialPartitions, numConsumers, 1);
			List<KafkaTopicPartition> parts3 = FlinkKafkaConsumerBase.assignPartitions(
					initialPartitions, numConsumers, 2);

			assertNotNull(parts1);
			assertNotNull(parts2);
			assertNotNull(parts3);

			assertTrue(parts1.size() >= minInitialPartitionsPerConsumer);
			assertTrue(parts1.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(parts2.size() >= minInitialPartitionsPerConsumer);
			assertTrue(parts2.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(parts3.size() >= minInitialPartitionsPerConsumer);
			assertTrue(parts3.size() <= maxInitialPartitionsPerConsumer);

			for (KafkaTopicPartition p : parts1) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}
			for (KafkaTopicPartition p : parts2) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}
			for (KafkaTopicPartition p : parts3) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}

			// all partitions must have been assigned
			assertTrue(allInitialPartitions.isEmpty());

			// grow the set of partitions and distribute anew

			List<KafkaTopicPartition> parts1new = FlinkKafkaConsumerBase.assignPartitions(
					newPartitions, numConsumers, 0);
			List<KafkaTopicPartition> parts2new = FlinkKafkaConsumerBase.assignPartitions(
					newPartitions, numConsumers, 1);
			List<KafkaTopicPartition> parts3new = FlinkKafkaConsumerBase.assignPartitions(
					newPartitions, numConsumers, 2);

			// new partitions must include all old partitions

			assertTrue(parts1new.size() > parts1.size());
			assertTrue(parts2new.size() > parts2.size());
			assertTrue(parts3new.size() > parts3.size());

			assertTrue(parts1new.containsAll(parts1));
			assertTrue(parts2new.containsAll(parts2));
			assertTrue(parts3new.containsAll(parts3));

			assertTrue(parts1new.size() >= minNewPartitionsPerConsumer);
			assertTrue(parts1new.size() <= maxNewPartitionsPerConsumer);
			assertTrue(parts2new.size() >= minNewPartitionsPerConsumer);
			assertTrue(parts2new.size() <= maxNewPartitionsPerConsumer);
			assertTrue(parts3new.size() >= minNewPartitionsPerConsumer);
			assertTrue(parts3new.size() <= maxNewPartitionsPerConsumer);

			for (KafkaTopicPartition p : parts1new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
			}
			for (KafkaTopicPartition p : parts2new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
			}
			for (KafkaTopicPartition p : parts3new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
			}

			// all partitions must have been assigned
			assertTrue(allNewPartitions.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
