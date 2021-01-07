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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.streaming.connectors.pulsar.testutils.TestMetadataReader;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.pulsar.common.naming.TopicName;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests on pulsar topic discoverer.
 */
@RunWith(Parameterized.class)
public class DiscovererTest extends TestLogger {

	private static final String TEST_TOPIC = "test-topic";
	private static final String TEST_TOPIC_PATTERN = "^" + TEST_TOPIC + "[0-9]*$";

	private final Map<String, String> params;

	public DiscovererTest(Map<String, String> params) {
		this.params = params;
	}

	@Parameterized.Parameters(name = "params = {0}")
	public static Collection<Map<String, String>[]> pattern() {
		return Arrays.asList(
			new Map[]{Collections.singletonMap("topic", TEST_TOPIC)},
			new Map[]{Collections.singletonMap("topicspattern", TEST_TOPIC_PATTERN)});
	}

	String topicName(String topic, int partition) {
		return TopicName.get(topic).getPartition(partition).toString();
	}

	@Test
	public void testPartitionEqualConsumerNumber() {
		try {
			Set<TopicRange> mockAllTopics = Sets.newHashSet(
				new TopicRange(topicName(TEST_TOPIC, 0)),
				new TopicRange(topicName(TEST_TOPIC, 1)),
				new TopicRange(topicName(TEST_TOPIC, 2)),
				new TopicRange(topicName(TEST_TOPIC, 3)));

			int numSubTasks = mockAllTopics.size();

			for (int i = 0; i < numSubTasks; i++) {
				TestMetadataReader discoverer = new TestMetadataReader(
					params, i, numSubTasks,
					TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(mockAllTopics));

				Set<TopicRange> initials = discoverer.discoverTopicChanges();
				assertEquals(1, initials.size());
				assertTrue(mockAllTopics.containsAll(initials));
				Assert.assertEquals(
					i,
					TestMetadataReader.getExpectedSubtaskIndex(
						initials.iterator().next(),
						numSubTasks));

				Set<TopicRange> second = discoverer.discoverTopicChanges();
				Set<TopicRange> third = discoverer.discoverTopicChanges();
				assertEquals(second.size(), 0);
				assertEquals(third.size(), 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPartitionGreaterThanConsumerNumber() {
		try {
			Set<TopicRange> mockAllTopics = new HashSet<>();
			Set<TopicRange> allTopics = new HashSet<>();
			for (int i = 0; i < 10; i++) {
				String topic = topicName(TEST_TOPIC, i);
				mockAllTopics.add(new TopicRange(topic));
				allTopics.add(new TopicRange(topic));
			}

			int numTasks = 3;
			int minPartitionsPerTask = mockAllTopics.size() / numTasks;
			int maxPartitionsPerTask = mockAllTopics.size() / numTasks + 1;

			for (int i = 0; i < numTasks; i++) {
				TestMetadataReader discoverer = new TestMetadataReader(
					params, i, numTasks,
					TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(mockAllTopics));

				Set<TopicRange> initials = discoverer.discoverTopicChanges();
				int isize = initials.size();
				assertTrue(isize >= minPartitionsPerTask && isize <= maxPartitionsPerTask);

				for (TopicRange initial : initials) {
					assertTrue(allTopics.contains(initial));
					Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(
						initial,
						numTasks), i);
					allTopics.remove(initial);
				}

				Set<TopicRange> second = discoverer.discoverTopicChanges();
				Set<TopicRange> third = discoverer.discoverTopicChanges();
				assertEquals(second.size(), 0);
				assertEquals(third.size(), 0);
			}
			assertTrue(allTopics.isEmpty());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPartitionLessThanConsumerNumber() throws Exception {
		try {
			Set<TopicRange> mockAllTopics = new HashSet<>();
			Set<TopicRange> allTopics = new HashSet<>();
			for (int i = 0; i <= 3; i++) {
				String topic = topicName(TEST_TOPIC, i);
				mockAllTopics.add(new TopicRange(topic));
				allTopics.add(new TopicRange(topic));
			}

			int numTasks = 2 * mockAllTopics.size();

			for (int i = 0; i < numTasks; i++) {
				TestMetadataReader discoverer = new TestMetadataReader(
					params, i, numTasks,
					TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(mockAllTopics));

				Set<TopicRange> initials = discoverer.discoverTopicChanges();
				int isize = initials.size();
				assertTrue(isize <= 1);

				for (TopicRange initial : initials) {
					assertTrue(allTopics.contains(initial));
					Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(
						initial,
						numTasks), i);
					allTopics.remove(initial);
				}

				Set<TopicRange> second = discoverer.discoverTopicChanges();
				Set<TopicRange> third = discoverer.discoverTopicChanges();
				assertEquals(second.size(), 0);
				assertEquals(third.size(), 0);
			}
			assertTrue(allTopics.isEmpty());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGrowingPartitions() {
		try {
			Set<TopicRange> mockAllTopics = new HashSet<>();
			Set<TopicRange> allTopics = new HashSet<>();
			for (int i = 0; i <= 10; i++) {
				String topic = topicName(TEST_TOPIC, i);
				mockAllTopics.add(new TopicRange(topic));
				allTopics.add(new TopicRange(topic));
			}

			Set<TopicRange> initial = new HashSet<>();
			Set<TopicRange> initialAll = new HashSet<>();
			for (int i = 0; i <= 7; i++) {
				String topic = topicName(TEST_TOPIC, i);
				initial.add(new TopicRange(topic));
				initialAll.add(new TopicRange(topic));
			}

			List<Set<TopicRange>> mockGet = Arrays.asList(initial, mockAllTopics);
			int numTasks = 3;
			int minInitialPartitionsPerConsumer = initial.size() / numTasks;
			int maxInitialPartitionsPerConsumer = initial.size() / numTasks + 1;
			int minAll = allTopics.size() / numTasks;
			int maxAll = allTopics.size() / numTasks + 1;

			TestMetadataReader discover1 = new TestMetadataReader(params, 0, numTasks,
				TestMetadataReader.createMockGetAllTopicsSequenceFromTwoReturns(mockGet));

			TestMetadataReader discover2 = new TestMetadataReader(params, 1, numTasks,
				TestMetadataReader.createMockGetAllTopicsSequenceFromTwoReturns(mockGet));

			TestMetadataReader discover3 = new TestMetadataReader(params, 2, numTasks,
				TestMetadataReader.createMockGetAllTopicsSequenceFromTwoReturns(mockGet));

			Set<TopicRange> initials1 = discover1.discoverTopicChanges();
			Set<TopicRange> initials2 = discover2.discoverTopicChanges();
			Set<TopicRange> initials3 = discover3.discoverTopicChanges();

			assertTrue(initials1.size() >= minInitialPartitionsPerConsumer &&
				initials1.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(initials2.size() >= minInitialPartitionsPerConsumer &&
				initials2.size() <= maxInitialPartitionsPerConsumer);
			assertTrue(initials3.size() >= minInitialPartitionsPerConsumer &&
				initials3.size() <= maxInitialPartitionsPerConsumer);

			for (TopicRange tp : initials1) {
				assertTrue(initialAll.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 0);
				initialAll.remove(tp);
			}

			for (TopicRange tp : initials2) {
				assertTrue(initialAll.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 1);
				initialAll.remove(tp);
			}

			for (TopicRange tp : initials3) {
				assertTrue(initialAll.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 2);
				initialAll.remove(tp);
			}

			assertTrue(initialAll.isEmpty());

			Set<TopicRange> seconds1 = discover1.discoverTopicChanges();
			Set<TopicRange> seconds2 = discover2.discoverTopicChanges();
			Set<TopicRange> seconds3 = discover3.discoverTopicChanges();

			assertTrue(Collections.disjoint(seconds1, initials1));
			assertTrue(Collections.disjoint(seconds2, initials2));
			assertTrue(Collections.disjoint(seconds3, initials3));

			assertTrue(initials1.size() + seconds1.size() >= minAll
				&& initials1.size() + seconds1.size() <= maxAll);
			assertTrue(initials2.size() + seconds2.size() >= minAll
				&& initials2.size() + seconds2.size() <= maxAll);
			assertTrue(initials3.size() + seconds3.size() >= minAll
				&& initials3.size() + seconds3.size() <= maxAll);

			for (TopicRange tp : initials1) {
				assertTrue(allTopics.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 0);
				allTopics.remove(tp);
			}

			for (TopicRange tp : initials2) {
				assertTrue(allTopics.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 1);
				allTopics.remove(tp);
			}

			for (TopicRange tp : initials3) {
				assertTrue(allTopics.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 2);
				allTopics.remove(tp);
			}

			for (TopicRange tp : seconds1) {
				assertTrue(allTopics.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 0);
				allTopics.remove(tp);
			}

			for (TopicRange tp : seconds2) {
				assertTrue(allTopics.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 1);
				allTopics.remove(tp);
			}

			for (TopicRange tp : seconds3) {
				assertTrue(allTopics.contains(tp));
				Assert.assertEquals(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks), 2);
				allTopics.remove(tp);
			}

			assertTrue(allTopics.isEmpty());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

