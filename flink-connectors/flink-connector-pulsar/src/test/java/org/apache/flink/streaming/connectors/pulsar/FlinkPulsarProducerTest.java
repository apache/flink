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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.serde.IntegerSerializationSchema;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit test of {@link FlinkPulsarProducer}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PulsarClient.class)
public class FlinkPulsarProducerTest {

	private static final String MOCK_SERVICE_URIL = "http://localhost:8080";
	private static final String MOCK_TOPIC_NAME = "mock_topic";
	private static final String ROUTING_KEY = "mock_key";

	/**
	 * Test Constructor.
	 */
	@Test
	public void testConstructor() throws Exception {
		Producer producer = mockPulsarProducer();
		PulsarKeyExtractor<Integer> keyExtractor = new TestKeyExtractor<>();
		FlinkPulsarProducer<Integer> sink = spySink(producer, keyExtractor);
		assertEquals(PulsarProduceMode.AT_LEAST_ONE, sink.getProduceMode());
		assertSame(keyExtractor, sink.getKeyExtractor());
	}

	//
	// Utilities
	//

	private Producer mockPulsarProducer() {
		return mock(Producer.class);
	}

	private FlinkPulsarProducer<Integer> spySink(
			Producer producer,
			PulsarKeyExtractor<Integer> keyExtractor) throws Exception {
		ProducerConfiguration conf = new ProducerConfiguration();
		FlinkPulsarProducer<Integer> flinkProducer = spy(new FlinkPulsarProducer<>(
			MOCK_SERVICE_URIL,
			MOCK_TOPIC_NAME,
			new IntegerSerializationSchema(),
			conf,
			keyExtractor));
		PowerMockito.mockStatic(PulsarClient.class);
		PulsarClient client = mock(PulsarClient.class);
		when(PulsarClient.create(anyString())).thenReturn(client);
		when(client.createProducer(anyString(), any(ProducerConfiguration.class))).thenReturn(producer);
		return flinkProducer;
	}

	/**
	 * A key extractor used for testing.
	 */
	private static class TestKeyExtractor<T> implements PulsarKeyExtractor<T> {

		@Override
		public String getKey(T t) {
			return ROUTING_KEY;
		}

	}

}
