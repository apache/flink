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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.serde.IntegerSerializationSchema;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Unit test of {@link FlinkPulsarProducer}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PulsarClient.class)
public class FlinkPulsarProducerTest {

	private static final String MOCK_SERVICE_URL = "http://localhost:8080";
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

	/**
	 * Test open producer.
	 * @throws Exception
	 */
	@Test
	public void testOpen() throws Exception {
		Producer producer = mock(Producer.class);
		FlinkPulsarProducer<Integer> sink = mock(FlinkPulsarProducer.class);
		when(sink.getRuntimeContext()).thenReturn(mock(RuntimeContext.class));
		PowerMockito.mockStatic(PulsarClient.class);
		PulsarClient client = mock(PulsarClient.class);
		when(PulsarClient.create(anyString())).thenReturn(client);
		when(client.createProducer(anyString(), any(ProducerConfiguration.class))).thenReturn(producer);

		sink.open(mock(Configuration.class));

		PowerMockito.verifyPrivate(sink, times(1)).invoke("createProducer", any(ProducerConfiguration.class));
	}

	/**
	 * Test invoke producer.
	 * @throws Exception
	 */
	@Test
	public void testInvoke() throws Exception {
		SinkFunction.Context context = mock(SinkFunction.Context.class);
		when(context.timestamp()).thenReturn(System.currentTimeMillis());
		Producer producer = mockPulsarProducer();

		PulsarKeyExtractor<Integer> keyExtractor = new TestKeyExtractor<>();
		FlinkPulsarProducer<Integer> sink = spySink(producer, keyExtractor);
		PowerMockito.when(
			sink,
			PowerMockito.method(
				FlinkPulsarProducer.class,
				"createProducer",
				ProducerConfiguration.class))
			.withArguments(any(ProducerConfiguration.class))
			.thenReturn(producer);

		CompletableFuture mockedFuture = mock(CompletableFuture.class);
		when(producer.sendAsync(any(Message.class))).thenReturn(mockedFuture);
		when(mockedFuture.thenApply(any(java.util.function.Function.class))).thenReturn(mockedFuture);

		Whitebox.setInternalState(sink, "producer", producer);
		sink.invoke(1, context);

		verify(producer, times(1)).sendAsync(Mockito.any(Message.class));
		verify(mockedFuture, atMost(1)).thenApply(any(java.util.function.Function.class));
		verify(mockedFuture, atMost(1)).exceptionally(any(java.util.function.Function.class));
	}

	/**
	 * Test close producer.
	 * @throws Exception
	 */
	@Test
	public void testClose() throws Exception {
		Producer producer = mock(Producer.class);
		PulsarKeyExtractor<Integer> keyExtractor = new TestKeyExtractor<>();

		FlinkPulsarProducer sink = spySink(producer, keyExtractor);
		Whitebox.setInternalState(sink, "producer", producer);

		sink.close();

		verify(producer, times(1)).close();
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
			MOCK_SERVICE_URL,
			MOCK_TOPIC_NAME,
			new IntegerSerializationSchema(),
			conf,
			keyExtractor));
		PowerMockito.mockStatic(PulsarClient.class);
		PulsarClient client = mock(PulsarClient.class);
		CompletableFuture mockedFuture = mock(CompletableFuture.class);
		when(mockedFuture.thenApply(any(java.util.function.Function.class))).thenReturn(mockedFuture);
		when(mockedFuture.exceptionally(any(java.util.function.Function.class))).thenReturn(mockedFuture);
		when(producer.sendAsync(any(Message.class))).thenReturn(mock(CompletableFuture.class));
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
