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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that the partition assignment in the partition discoverer is
 * deterministic and stable, with both fixed and growing partitions.
 */
@RunWith(Parameterized.class)
public class AbstractPartitionDiscovererTest {

	private static final String TEST_TOPIC = "test-topic";
	private static final String TEST_TOPIC_PATTERN = "^" + TEST_TOPIC + "[0-9]*$";

	private final KafkaTopicsDescriptor topicsDescriptor;

	public AbstractPartitionDiscovererTest(KafkaTopicsDescriptor topicsDescriptor) {
		this.topicsDescriptor = topicsDescriptor;
	}

	@Parameterized.Parameters(name = "KafkaTopicsDescriptor = {0}")
	@SuppressWarnings("unchecked")
	public static Collection<KafkaTopicsDescriptor[]> timeCharacteristic(){
		return Arrays.asList(
			new KafkaTopicsDescriptor[]{new KafkaTopicsDescriptor(Collections.singletonList(TEST_TOPIC), null)},
			new KafkaTopicsDescriptor[]{new KafkaTopicsDescriptor(null, Pattern.compile(TEST_TOPIC_PATTERN))});
	}

	@Test
	public void testPartitionsEqualConsumersFixedPartitions() throws Exception {
		List<KafkaTopicPartition> mockGetAllPartitionsForTopicsReturn = Arrays.asList(
			new KafkaTopicPartition(TEST_TOPIC, 0),
			new KafkaTopicPartition(TEST_TOPIC, 1),
			new KafkaTopicPartition(TEST_TOPIC, 2),
			new KafkaTopicPartition(TEST_TOPIC, 3));

		for (int i = 0; i < mockGetAllPartitionsForTopicsReturn.size(); i++) {
			TestPartitionDiscoverer partitionDiscoverer = new TestPartitionDiscoverer(
					topicsDescriptor,
					i,
					mockGetAllPartitionsForTopicsReturn.size(),
					createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList(TEST_TOPIC)),
					createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturn));
			partitionDiscoverer.open();

			List<KafkaTopicPartition> initialDiscovery = partitionDiscoverer.discoverPartitions();
			assertEquals(1, initialDiscovery.size());
			assertTrue(contains(mockGetAllPartitionsForTopicsReturn, initialDiscovery.get(0).getPartition()));

			// subsequent discoveries should not find anything
			List<KafkaTopicPartition> secondDiscovery = partitionDiscoverer.discoverPartitions();
			List<KafkaTopicPartition> thirdDiscovery = partitionDiscoverer.discoverPartitions();
			assertEquals(0, secondDiscovery.size());
			assertEquals(0, thirdDiscovery.size());
		}
	}

	@Test
	public void testMultiplePartitionsPerConsumersFixedPartitions() {
		try {
			final int[] partitionIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

			final List<KafkaTopicPartition> mockGetAllPartitionsForTopicsReturn = new ArrayList<>();
			final Set<KafkaTopicPartition> allPartitions = new HashSet<>();

			for (int p : partitionIDs) {
				KafkaTopicPartition part = new KafkaTopicPartition(TEST_TOPIC, p);
				mockGetAllPartitionsForTopicsReturn.add(part);
				allPartitions.add(part);
			}

			final int numConsumers = 3;
			final int minPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturn.size() / numConsumers;
			final int maxPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturn.size() / numConsumers + 1;

			for (int i = 0; i < numConsumers; i++) {
				TestPartitionDiscoverer partitionDiscoverer = new TestPartitionDiscoverer(
						topicsDescriptor,
						i,
						numConsumers,
						createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList(TEST_TOPIC)),
						createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturn));
				partitionDiscoverer.open();

				List<KafkaTopicPartition> initialDiscovery = partitionDiscoverer.discoverPartitions();
				assertTrue(initialDiscovery.size() >= minPartitionsPerConsumer);
				assertTrue(initialDiscovery.size() <= maxPartitionsPerConsumer);

				for (KafkaTopicPartition p : initialDiscovery) {
					// check that the element was actually contained
					assertTrue(allPartitions.remove(p));
				}

				// subsequent discoveries should not find anything
				List<KafkaTopicPartition> secondDiscovery = partitionDiscoverer.discoverPartitions();
				List<KafkaTopicPartition> thirdDiscovery = partitionDiscoverer.discoverPartitions();
				assertEquals(0, secondDiscovery.size());
				assertEquals(0, thirdDiscovery.size());
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
	public void testPartitionsFewerThanConsumersFixedPartitions() {
		try {
			List<KafkaTopicPartition> mockGetAllPartitionsForTopicsReturn = Arrays.asList(
				new KafkaTopicPartition(TEST_TOPIC, 0),
				new KafkaTopicPartition(TEST_TOPIC, 1),
				new KafkaTopicPartition(TEST_TOPIC, 2),
				new KafkaTopicPartition(TEST_TOPIC, 3));

			final Set<KafkaTopicPartition> allPartitions = new HashSet<>();
			allPartitions.addAll(mockGetAllPartitionsForTopicsReturn);

			final int numConsumers = 2 * mockGetAllPartitionsForTopicsReturn.size() + 3;

			for (int i = 0; i < numConsumers; i++) {
				TestPartitionDiscoverer partitionDiscoverer = new TestPartitionDiscoverer(
						topicsDescriptor,
						i,
						numConsumers,
						createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList(TEST_TOPIC)),
						createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturn));
				partitionDiscoverer.open();

				List<KafkaTopicPartition> initialDiscovery = partitionDiscoverer.discoverPartitions();
				assertTrue(initialDiscovery.size() <= 1);

				for (KafkaTopicPartition p : initialDiscovery) {
					// check that the element was actually contained
					assertTrue(allPartitions.remove(p));
				}

				// subsequent discoveries should not find anything
				List<KafkaTopicPartition> secondDiscovery = partitionDiscoverer.discoverPartitions();
				List<KafkaTopicPartition> thirdDiscovery = partitionDiscoverer.discoverPartitions();
				assertEquals(0, secondDiscovery.size());
				assertEquals(0, thirdDiscovery.size());
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
	public void testGrowingPartitions() {
		try {
			final int[] newPartitionIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
			List<KafkaTopicPartition> allPartitions = new ArrayList<>(11);

			for (int p : newPartitionIDs) {
				KafkaTopicPartition part = new KafkaTopicPartition(TEST_TOPIC, p);
				allPartitions.add(part);
			}

			// first discovery returns an initial subset of the partitions; second returns all partitions
			List<List<KafkaTopicPartition>> mockGetAllPartitionsForTopicsReturnSequence = Arrays.asList(
				new ArrayList<>(allPartitions.subList(0, 7)),
				allPartitions);

			final Set<KafkaTopicPartition> allNewPartitions = new HashSet<>(allPartitions);
			final Set<KafkaTopicPartition> allInitialPartitions = new HashSet<>(mockGetAllPartitionsForTopicsReturnSequence.get(0));

			final int numConsumers = 3;
			final int minInitialPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturnSequence.get(0).size() / numConsumers;
			final int maxInitialPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturnSequence.get(0).size() / numConsumers + 1;
			final int minNewPartitionsPerConsumer = allPartitions.size() / numConsumers;
			final int maxNewPartitionsPerConsumer = allPartitions.size() / numConsumers + 1;

			TestPartitionDiscoverer partitionDiscovererSubtask0 = new TestPartitionDiscoverer(
					topicsDescriptor,
					0,
					numConsumers,
					createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList(TEST_TOPIC)),
					deepClone(mockGetAllPartitionsForTopicsReturnSequence));
			partitionDiscovererSubtask0.open();

			TestPartitionDiscoverer partitionDiscovererSubtask1 = new TestPartitionDiscoverer(
					topicsDescriptor,
					1,
					numConsumers,
					createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList(TEST_TOPIC)),
					deepClone(mockGetAllPartitionsForTopicsReturnSequence));
			partitionDiscovererSubtask1.open();

			TestPartitionDiscoverer partitionDiscovererSubtask2 = new TestPartitionDiscoverer(
					topicsDescriptor,
					2,
					numConsumers,
					createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList(TEST_TOPIC)),
					deepClone(mockGetAllPartitionsForTopicsReturnSequence));
			partitionDiscovererSubtask2.open();

			List<KafkaTopicPartition> initialDiscoverySubtask0 = partitionDiscovererSubtask0.discoverPartitions();
			List<KafkaTopicPartition> initialDiscoverySubtask1 = partitionDiscovererSubtask1.discoverPartitions();
			List<KafkaTopicPartition> initialDiscoverySubtask2 = partitionDiscovererSubtask2.discoverPartitions();

			assertTrue(initialDiscoverySubtask0.size() >= minInitialPartitionsPerConsumer);
			assertTrue(initialDiscoverySubtask0.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(initialDiscoverySubtask1.size() >= minInitialPartitionsPerConsumer);
			assertTrue(initialDiscoverySubtask1.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(initialDiscoverySubtask2.size() >= minInitialPartitionsPerConsumer);
			assertTrue(initialDiscoverySubtask2.size() <= maxInitialPartitionsPerConsumer);

			for (KafkaTopicPartition p : initialDiscoverySubtask0) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}

			for (KafkaTopicPartition p : initialDiscoverySubtask1) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}

			for (KafkaTopicPartition p : initialDiscoverySubtask2) {
				// check that the element was actually contained
				assertTrue(allInitialPartitions.remove(p));
			}

			// all partitions must have been assigned
			assertTrue(allInitialPartitions.isEmpty());

			// now, execute discover again (should find the extra new partitions)
			List<KafkaTopicPartition> secondDiscoverySubtask0 = partitionDiscovererSubtask0.discoverPartitions();
			List<KafkaTopicPartition> secondDiscoverySubtask1 = partitionDiscovererSubtask1.discoverPartitions();
			List<KafkaTopicPartition> secondDiscoverySubtask2 = partitionDiscovererSubtask2.discoverPartitions();

			// new discovered partitions must not have been discovered before
			assertTrue(Collections.disjoint(secondDiscoverySubtask0, initialDiscoverySubtask0));
			assertTrue(Collections.disjoint(secondDiscoverySubtask1, initialDiscoverySubtask1));
			assertTrue(Collections.disjoint(secondDiscoverySubtask2, initialDiscoverySubtask2));

			assertTrue(secondDiscoverySubtask0.size() + initialDiscoverySubtask0.size() >= minNewPartitionsPerConsumer);
			assertTrue(secondDiscoverySubtask0.size() + initialDiscoverySubtask0.size() <= maxNewPartitionsPerConsumer);
			assertTrue(secondDiscoverySubtask1.size() + initialDiscoverySubtask1.size() >= minNewPartitionsPerConsumer);
			assertTrue(secondDiscoverySubtask1.size() + initialDiscoverySubtask1.size() <= maxNewPartitionsPerConsumer);
			assertTrue(secondDiscoverySubtask2.size() + initialDiscoverySubtask2.size() >= minNewPartitionsPerConsumer);
			assertTrue(secondDiscoverySubtask2.size() + initialDiscoverySubtask2.size() <= maxNewPartitionsPerConsumer);

			// check that the two discoveries combined form all partitions

			for (KafkaTopicPartition p : initialDiscoverySubtask0) {
				assertTrue(allNewPartitions.remove(p));
			}

			for (KafkaTopicPartition p : initialDiscoverySubtask1) {
				assertTrue(allNewPartitions.remove(p));
			}

			for (KafkaTopicPartition p : initialDiscoverySubtask2) {
				assertTrue(allNewPartitions.remove(p));
			}

			for (KafkaTopicPartition p : secondDiscoverySubtask0) {
				assertTrue(allNewPartitions.remove(p));
			}

			for (KafkaTopicPartition p : secondDiscoverySubtask1) {
				assertTrue(allNewPartitions.remove(p));
			}

			for (KafkaTopicPartition p : secondDiscoverySubtask2) {
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

	private static class TestPartitionDiscoverer extends AbstractPartitionDiscoverer {

		private final KafkaTopicsDescriptor topicsDescriptor;

		private final List<List<String>> mockGetAllTopicsReturnSequence;
		private final List<List<KafkaTopicPartition>> mockGetAllPartitionsForTopicsReturnSequence;

		private int getAllTopicsInvokeCount = 0;
		private int getAllPartitionsForTopicsInvokeCount = 0;

		public TestPartitionDiscoverer(
				KafkaTopicsDescriptor topicsDescriptor,
				int indexOfThisSubtask,
				int numParallelSubtasks,
				List<List<String>> mockGetAllTopicsReturnSequence,
				List<List<KafkaTopicPartition>> mockGetAllPartitionsForTopicsReturnSequence) {

			super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);

			this.topicsDescriptor = topicsDescriptor;
			this.mockGetAllTopicsReturnSequence = mockGetAllTopicsReturnSequence;
			this.mockGetAllPartitionsForTopicsReturnSequence = mockGetAllPartitionsForTopicsReturnSequence;
		}

		@Override
		protected List<String> getAllTopics() {
			assertTrue(topicsDescriptor.isTopicPattern());
			return mockGetAllTopicsReturnSequence.get(getAllTopicsInvokeCount++);
		}

		@Override
		protected List<KafkaTopicPartition> getAllPartitionsForTopics(List<String> topics) {
			if (topicsDescriptor.isFixedTopics()) {
				assertEquals(topicsDescriptor.getFixedTopics(), topics);
			} else {
				assertEquals(mockGetAllTopicsReturnSequence.get(getAllPartitionsForTopicsInvokeCount - 1), topics);
			}
			return mockGetAllPartitionsForTopicsReturnSequence.get(getAllPartitionsForTopicsInvokeCount++);
		}

		@Override
		protected void initializeConnections() {
			// nothing to do
		}

		@Override
		protected void wakeupConnections() {
			// nothing to do
		}

		@Override
		protected void closeConnections() {
			// nothing to do
		}
	}

	private static List<List<String>> createMockGetAllTopicsSequenceFromFixedReturn(final List<String> fixed) {
		@SuppressWarnings("unchecked")
		List<List<String>> mockSequence = mock(List.class);
		when(mockSequence.get(anyInt())).thenAnswer(new Answer<List<String>>() {
			@Override
			public List<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
				return new ArrayList<>(fixed);
			}
		});

		return mockSequence;
	}

	private static List<List<KafkaTopicPartition>> createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(final List<KafkaTopicPartition> fixed) {
		@SuppressWarnings("unchecked")
		List<List<KafkaTopicPartition>> mockSequence = mock(List.class);
		when(mockSequence.get(anyInt())).thenAnswer(new Answer<List<KafkaTopicPartition>>() {
			@Override
			public List<KafkaTopicPartition> answer(InvocationOnMock invocationOnMock) throws Throwable {
				return new ArrayList<>(fixed);
			}
		});

		return mockSequence;
	}

	private boolean contains(List<KafkaTopicPartition> partitions, int partition) {
		for (KafkaTopicPartition ktp : partitions) {
			if (ktp.getPartition() == partition) {
				return true;
			}
		}

		return false;
	}

	private List<List<KafkaTopicPartition>> deepClone(List<List<KafkaTopicPartition>> toClone) {
		List<List<KafkaTopicPartition>> clone = new ArrayList<>(toClone.size());
		for (List<KafkaTopicPartition> partitionsToClone : toClone) {
			List<KafkaTopicPartition> clonePartitions = new ArrayList<>(partitionsToClone.size());
			clonePartitions.addAll(partitionsToClone);

			clone.add(clonePartitions);
		}

		return clone;
	}
}
