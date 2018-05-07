/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Tests that cover the specific behavior of partition discoverer for Kafka API 0.9.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Kafka09PartitionDiscoverer.class)
public class Kafka09PartitionDiscovererTest {

	private static final String NON_EXISTING_TOPIC = "non-existing-topic";
	private static final String EXISTING_TOPIC = "existing-topic";

	@Test
	public void testWhenTopicDoesNotExist() throws Exception {
		List<String> topics = singletonList(NON_EXISTING_TOPIC);

		KafkaConsumer<?, ?> mockConsumer = mock(KafkaConsumer.class);
		when(mockConsumer.partitionsFor(anyString())).thenReturn(null);

		whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

		final Kafka09PartitionDiscoverer discoverer = new Kafka09PartitionDiscoverer
			(new KafkaTopicsDescriptor(topics, null),
				0,
				1,
				new Properties());

		discoverer.initializeConnections();
		List<KafkaTopicPartition> results = discoverer.getAllPartitionsForTopics(topics);
		assertTrue("The returned partition list is not empty", results.isEmpty());
	}

	@Test
	public void testWhenSomeTopicsDoExist() throws Exception {
		List<String> topics = asList(EXISTING_TOPIC, NON_EXISTING_TOPIC);

		KafkaConsumer<?, ?> mockConsumer = mock(KafkaConsumer.class);
		when(mockConsumer.partitionsFor(eq(NON_EXISTING_TOPIC))).thenReturn(null);
		when(mockConsumer.partitionsFor(eq(EXISTING_TOPIC)))
			.thenReturn(singletonList(new PartitionInfo(EXISTING_TOPIC, 0, null, null, null)));

		whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

		final Kafka09PartitionDiscoverer discoverer = new Kafka09PartitionDiscoverer
			(new KafkaTopicsDescriptor(topics, null),
				0,
				1,
				new Properties());

		discoverer.initializeConnections();
		List<KafkaTopicPartition> results = discoverer.getAllPartitionsForTopics(topics);
		assertEquals("The returned partition list size is not as expected", 1, results.size());
	}
}
