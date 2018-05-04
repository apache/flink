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

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Tests that cover the specific behavior of partition discoverer for Kafka API 0.9.
 */
@Ignore
@RunWith(PowerMockRunner.class)
@PrepareForTest(Kafka09PartitionDiscoverer.class)
public class Kafka09PartitionDiscovererTest {

	private static final String TEST_TOPIC = "test-topic";

	@Test(expected = IllegalStateException.class)
	public void testWhenTopicDoesNotExist() throws Exception {
		List<String> topics = singletonList(TEST_TOPIC);

		KafkaConsumer<?, ?> mockConsumer = mock(KafkaConsumer.class);
		when(mockConsumer.partitionsFor(anyString())).thenReturn(null);

		whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

		final Kafka09PartitionDiscoverer discoverer = new Kafka09PartitionDiscoverer
			(new KafkaTopicsDescriptor(topics, null),
				0,
				1,
				new Properties());

		discoverer.initializeConnections();
		discoverer.getAllPartitionsForTopics(topics);
	}
}
