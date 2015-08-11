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

package org.apache.flink.streaming.connectors;

import org.apache.flink.kafka_backport.common.TopicPartition;

import org.junit.Test;

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
			int[] partitions = {4, 52, 17, 1};
			
			for (int i = 0; i < partitions.length; i++) {
				List<TopicPartition> parts = FlinkKafkaConsumer.assignPartitions(
						partitions, "test-topic", partitions.length, i);
				
				assertNotNull(parts);
				assertEquals(1, parts.size());
				assertTrue(contains(partitions, parts.get(0).partition()));
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
			final int[] partitions = {4, 52, 17, 1, 2, 3, 89, 42, 31, 127, 14};

			final Set<Integer> allPartitions = new HashSet<>();
			for (int i : partitions) {
				allPartitions.add(i);
			}
			
			final int numConsumers = 3;
			final int minPartitionsPerConsumer = partitions.length / numConsumers;
			final int maxPartitionsPerConsumer = partitions.length / numConsumers + 1;
			
			for (int i = 0; i < numConsumers; i++) {
				List<TopicPartition> parts = FlinkKafkaConsumer.assignPartitions(
						partitions, "test-topic", numConsumers, i);

				assertNotNull(parts);
				assertTrue(parts.size() >= minPartitionsPerConsumer);
				assertTrue(parts.size() <= maxPartitionsPerConsumer);

				for (TopicPartition p : parts) {
					// check that the element was actually contained
					assertTrue(allPartitions.remove(p.partition()));
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
			final int[] partitions = {4, 52, 17, 1};

			final Set<Integer> allPartitions = new HashSet<>();
			for (int i : partitions) {
				allPartitions.add(i);
			}

			final int numConsumers = 2 * partitions.length + 3;
			
			for (int i = 0; i < numConsumers; i++) {
				List<TopicPartition> parts = FlinkKafkaConsumer.assignPartitions(
						partitions, "test-topic", numConsumers, i);

				assertNotNull(parts);
				assertTrue(parts.size() <= 1);
				
				for (TopicPartition p : parts) {
					// check that the element was actually contained
					assertTrue(allPartitions.remove(p.partition()));
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
			List<TopicPartition> parts1 = FlinkKafkaConsumer.assignPartitions(new int[0], "test-topic", 4, 2);
			assertNotNull(parts1);
			assertTrue(parts1.isEmpty());

			List<TopicPartition> parts2 = FlinkKafkaConsumer.assignPartitions(new int[0], "test-topic", 1, 0);
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
			final int[] newPartitions = {4, 52, 17, 1, 2, 3, 89, 42, 31, 127, 14};
			final int[] initialPartitions = Arrays.copyOfRange(newPartitions, 0, 7);

			final Set<Integer> allNewPartitions = new HashSet<>();
			final Set<Integer> allInitialPartitions = new HashSet<>();
			for (int i : newPartitions) {
				allNewPartitions.add(i);
			}
			for (int i : initialPartitions) {
				allInitialPartitions.add(i);
			}

			final int numConsumers = 3;
			final int minInitialPartitionsPerConsumer = initialPartitions.length / numConsumers;
			final int maxInitialPartitionsPerConsumer = initialPartitions.length / numConsumers + 1;
			final int minNewPartitionsPerConsumer = newPartitions.length / numConsumers;
			final int maxNewPartitionsPerConsumer = newPartitions.length / numConsumers + 1;
			
			List<TopicPartition> parts1 = FlinkKafkaConsumer.assignPartitions(
					initialPartitions, "test-topic", numConsumers, 0);
			List<TopicPartition> parts2 = FlinkKafkaConsumer.assignPartitions(
					initialPartitions, "test-topic", numConsumers, 1);
			List<TopicPartition> parts3 = FlinkKafkaConsumer.assignPartitions(
					initialPartitions, "test-topic", numConsumers, 2);

			assertNotNull(parts1);
			assertNotNull(parts2);
			assertNotNull(parts3);
			
			assertTrue(parts1.size() >= minInitialPartitionsPerConsumer);
			assertTrue(parts1.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(parts2.size() >= minInitialPartitionsPerConsumer);
			assertTrue(parts2.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(parts3.size() >= minInitialPartitionsPerConsumer);
			assertTrue(parts3.size() <= maxInitialPartitionsPerConsumer);

			for (TopicPartition p : parts1) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p.partition()));
			}
			for (TopicPartition p : parts2) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p.partition()));
			}
			for (TopicPartition p : parts3) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p.partition()));
			}
			
			// all partitions must have been assigned
			assertTrue(allInitialPartitions.isEmpty());
			
			// grow the set of partitions and distribute anew
			
			List<TopicPartition> parts1new = FlinkKafkaConsumer.assignPartitions(
					newPartitions, "test-topic", numConsumers, 0);
			List<TopicPartition> parts2new = FlinkKafkaConsumer.assignPartitions(
					newPartitions, "test-topic", numConsumers, 1);
			List<TopicPartition> parts3new = FlinkKafkaConsumer.assignPartitions(
					newPartitions, "test-topic", numConsumers, 2);

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

			for (TopicPartition p : parts1new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p.partition()));
			}
			for (TopicPartition p : parts2new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p.partition()));
			}
			for (TopicPartition p : parts3new) {
				// check that the element was actually contained
				assertTrue(allNewPartitions.remove(p.partition()));
			}

			// all partitions must have been assigned
			assertTrue(allNewPartitions.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static boolean contains(int[] array, int value) {
		for (int i : array) {
			if (i == value) {
				return true;
			}
		}
		return false;
	}
}
