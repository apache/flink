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
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionLeader;
import org.apache.kafka.common.Node;
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

	private final Node fake = new Node(1337, "localhost", 1337);

	@Test
	public void testPartitionsEqualConsumers() {
		try {
			List<KafkaTopicPartitionLeader> inPartitions = new ArrayList<>();
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 4), fake));
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 52), fake));
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 17), fake));
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 1), fake));

			for (int i = 0; i < inPartitions.size(); i++) {
				List<KafkaTopicPartitionLeader> parts = FlinkKafkaConsumer.assignPartitions(
						inPartitions, inPartitions.size(), i);

				assertNotNull(parts);
				assertEquals(1, parts.size());
				assertTrue(contains(inPartitions, parts.get(0).getTopicPartition().getPartition()));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private boolean contains(List<KafkaTopicPartitionLeader> inPartitions, int partition) {
		for (KafkaTopicPartitionLeader ktp: inPartitions) {
			if (ktp.getTopicPartition().getPartition() == partition) {
				return true;
			}
		}
		return false;
	}

	@Test
	public void testMultiplePartitionsPerConsumers() {
		try {
			final int[] partitionIDs = {4, 52, 17, 1, 2, 3, 89, 42, 31, 127, 14};

			final List<KafkaTopicPartitionLeader> partitions = new ArrayList<>();
			final Set<KafkaTopicPartitionLeader> allPartitions = new HashSet<>();

			for (int p : partitionIDs) {
				KafkaTopicPartitionLeader part = new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", p), fake);
				partitions.add(part);
				allPartitions.add(part);
			}

			final int numConsumers = 3;
			final int minPartitionsPerConsumer = partitions.size() / numConsumers;
			final int maxPartitionsPerConsumer = partitions.size() / numConsumers + 1;

			for (int i = 0; i < numConsumers; i++) {
				List<KafkaTopicPartitionLeader> parts = FlinkKafkaConsumer.assignPartitions(partitions, numConsumers, i);

				assertNotNull(parts);
				assertTrue(parts.size() >= minPartitionsPerConsumer);
				assertTrue(parts.size() <= maxPartitionsPerConsumer);

				for (KafkaTopicPartitionLeader p : parts) {
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
			List<KafkaTopicPartitionLeader> inPartitions = new ArrayList<>();
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 4), fake));
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 52), fake));
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 17), fake));
			inPartitions.add(new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", 1), fake));

			final Set<KafkaTopicPartitionLeader> allPartitions = new HashSet<>();
			allPartitions.addAll(inPartitions);

			final int numConsumers = 2 * inPartitions.size() + 3;

			for (int i = 0; i < numConsumers; i++) {
				List<KafkaTopicPartitionLeader> parts = FlinkKafkaConsumer.assignPartitions(inPartitions, numConsumers, i);

				assertNotNull(parts);
				assertTrue(parts.size() <= 1);

				for (KafkaTopicPartitionLeader p : parts) {
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
			List<KafkaTopicPartitionLeader> ep = new ArrayList<>();
			List<KafkaTopicPartitionLeader> parts1 = FlinkKafkaConsumer.assignPartitions(ep, 4, 2);
			assertNotNull(parts1);
			assertTrue(parts1.isEmpty());

			List<KafkaTopicPartitionLeader> parts2 = FlinkKafkaConsumer.assignPartitions(ep, 1, 0);
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
			List<KafkaTopicPartitionLeader> newPartitions = new ArrayList<>();

			for (int p : newPartitionIDs) {
				KafkaTopicPartitionLeader part = new KafkaTopicPartitionLeader(new KafkaTopicPartition("test-topic", p), fake);
				newPartitions.add(part);
			}

			List<KafkaTopicPartitionLeader> initialPartitions = newPartitions.subList(0, 7);

			final Set<KafkaTopicPartitionLeader> allNewPartitions = new HashSet<>(newPartitions);
			final Set<KafkaTopicPartitionLeader> allInitialPartitions = new HashSet<>(initialPartitions);

			final int numConsumers = 3;
			final int minInitialPartitionsPerConsumer = initialPartitions.size() / numConsumers;
			final int maxInitialPartitionsPerConsumer = initialPartitions.size() / numConsumers + 1;
			final int minNewPartitionsPerConsumer = newPartitions.size() / numConsumers;
			final int maxNewPartitionsPerConsumer = newPartitions.size() / numConsumers + 1;

			List<KafkaTopicPartitionLeader> parts1 = FlinkKafkaConsumer.assignPartitions(
					initialPartitions, numConsumers, 0);
			List<KafkaTopicPartitionLeader> parts2 = FlinkKafkaConsumer.assignPartitions(
					initialPartitions, numConsumers, 1);
			List<KafkaTopicPartitionLeader> parts3 = FlinkKafkaConsumer.assignPartitions(
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

			for (KafkaTopicPartitionLeader p : parts1) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}
			for (KafkaTopicPartitionLeader p : parts2) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}
			for (KafkaTopicPartitionLeader p : parts3) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}

			// all partitions must have been assigned
			assertTrue(allInitialPartitions.isEmpty());

			// grow the set of partitions and distribute anew

			List<KafkaTopicPartitionLeader> parts1new = FlinkKafkaConsumer.assignPartitions(
					newPartitions, numConsumers, 0);
			List<KafkaTopicPartitionLeader> parts2new = FlinkKafkaConsumer.assignPartitions(
					newPartitions, numConsumers, 1);
			List<KafkaTopicPartitionLeader> parts3new = FlinkKafkaConsumer.assignPartitions(
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

			for (KafkaTopicPartitionLeader p : parts1new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
			}
			for (KafkaTopicPartitionLeader p : parts2new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p));
			}
			for (KafkaTopicPartitionLeader p : parts3new) {
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
