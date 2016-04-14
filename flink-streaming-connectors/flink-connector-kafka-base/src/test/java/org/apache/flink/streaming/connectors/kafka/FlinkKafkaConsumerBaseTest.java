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

import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FlinkKafkaConsumerBaseTest {

	/**
	 * Tests that not both types of timestamp extractors / watermark generators can be used.
	 */
	@Test
	public void testEitherWatermarkExtractor() {
		try {
			new DummyFlinkKafkaConsumer<>().assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<Object>) null);
			fail();
		} catch (NullPointerException ignored) {}

		try {
			new DummyFlinkKafkaConsumer<>().assignTimestampsAndWatermarks((AssignerWithPunctuatedWatermarks<Object>) null);
			fail();
		} catch (NullPointerException ignored) {}
		
		@SuppressWarnings("unchecked")
		final AssignerWithPeriodicWatermarks<String> periodicAssigner = mock(AssignerWithPeriodicWatermarks.class);
		@SuppressWarnings("unchecked")
		final AssignerWithPunctuatedWatermarks<String> punctuatedAssigner = mock(AssignerWithPunctuatedWatermarks.class);
		
		DummyFlinkKafkaConsumer<String> c1 = new DummyFlinkKafkaConsumer<>();
		c1.assignTimestampsAndWatermarks(periodicAssigner);
		try {
			c1.assignTimestampsAndWatermarks(punctuatedAssigner);
			fail();
		} catch (IllegalStateException ignored) {}

		DummyFlinkKafkaConsumer<String> c2 = new DummyFlinkKafkaConsumer<>();
		c2.assignTimestampsAndWatermarks(punctuatedAssigner);
		try {
			c2.assignTimestampsAndWatermarks(periodicAssigner);
			fail();
		} catch (IllegalStateException ignored) {}
	}

	/**
	 * Tests that no checkpoints happen when the fetcher is not running.
	 */
	@Test
	public void ignoreCheckpointWhenNotRunning() throws Exception {
		@SuppressWarnings("unchecked")
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		FlinkKafkaConsumerBase<String> consumer = getConsumer(fetcher, new LinkedMap(), false);
		assertNull(consumer.snapshotState(17L, 23L));
		consumer.notifyCheckpointComplete(66L);
	}

	/**
	 * Tests that no checkpoints happen when the fetcher is not running.
	 */
	@Test
	public void checkRestoredCheckpointWhenFetcherNotReady() throws Exception {
		HashMap<KafkaTopicPartition, Long> restoreState = new HashMap<>();
		restoreState.put(new KafkaTopicPartition("abc", 13), 16768L);
		restoreState.put(new KafkaTopicPartition("def", 7), 987654321L);

		FlinkKafkaConsumerBase<String> consumer = getConsumer(null, new LinkedMap(), true);
		consumer.restoreState(restoreState);
		
		assertEquals(restoreState, consumer.snapshotState(17L, 23L));
	}

	/**
	 * Tests that no checkpoints happen when the fetcher is not running.
	 */
	@Test
	public void checkRestoredNullCheckpointWhenFetcherNotReady() throws Exception {
		FlinkKafkaConsumerBase<String> consumer = getConsumer(null, new LinkedMap(), true);
		assertNull(consumer.snapshotState(17L, 23L));
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testSnapshotState() throws Exception {
		final HashMap<KafkaTopicPartition, Long> state1 = new HashMap<>();
		state1.put(new KafkaTopicPartition("abc", 13), 16768L);
		state1.put(new KafkaTopicPartition("def", 7), 987654321L);

		final HashMap<KafkaTopicPartition, Long> state2 = new HashMap<>();
		state2.put(new KafkaTopicPartition("abc", 13), 16770L);
		state2.put(new KafkaTopicPartition("def", 7), 987654329L);

		final HashMap<KafkaTopicPartition, Long> state3 = new HashMap<>();
		state2.put(new KafkaTopicPartition("abc", 13), 16780L);
		state2.put(new KafkaTopicPartition("def", 7), 987654377L);
		
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);
		when(fetcher.snapshotCurrentState()).thenReturn(state1, state2, state3);
			
		final LinkedMap pendingCheckpoints = new LinkedMap();
	
		FlinkKafkaConsumerBase<String> consumer = getConsumer(fetcher, pendingCheckpoints, true);
		assertEquals(0, pendingCheckpoints.size());
		
		// checkpoint 1
		HashMap<KafkaTopicPartition, Long> snapshot1 = consumer.snapshotState(138L, 19L);
		assertEquals(state1, snapshot1);
		assertEquals(1, pendingCheckpoints.size());
		assertEquals(state1, pendingCheckpoints.get(138L));

		// checkpoint 2
		HashMap<KafkaTopicPartition, Long> snapshot2 = consumer.snapshotState(140L, 1578L);
		assertEquals(state2, snapshot2);
		assertEquals(2, pendingCheckpoints.size());
		assertEquals(state2, pendingCheckpoints.get(140L));
		
		// ack checkpoint 1
		consumer.notifyCheckpointComplete(138L);
		assertEquals(1, pendingCheckpoints.size());
		assertTrue(pendingCheckpoints.containsKey(140L));

		// checkpoint 3
		HashMap<KafkaTopicPartition, Long> snapshot3 = consumer.snapshotState(141L, 1578L);
		assertEquals(state3, snapshot3);
		assertEquals(2, pendingCheckpoints.size());
		assertEquals(state3, pendingCheckpoints.get(141L));
		
		// ack checkpoint 3, subsumes number 2
		consumer.notifyCheckpointComplete(141L);
		assertEquals(0, pendingCheckpoints.size());


		consumer.notifyCheckpointComplete(666); // invalid checkpoint
		assertEquals(0, pendingCheckpoints.size());

		// create 500 snapshots
		for (int i = 100; i < 600; i++) {
			consumer.snapshotState(i, 15 * i);
		}
		assertEquals(FlinkKafkaConsumerBase.MAX_NUM_PENDING_CHECKPOINTS, pendingCheckpoints.size());

		// commit only the second last
		consumer.notifyCheckpointComplete(598);
		assertEquals(1, pendingCheckpoints.size());

		// access invalid checkpoint
		consumer.notifyCheckpointComplete(590);

		// and the last
		consumer.notifyCheckpointComplete(599);
		assertEquals(0, pendingCheckpoints.size());
	}

	// ------------------------------------------------------------------------

	private static <T> FlinkKafkaConsumerBase<T> getConsumer(
			AbstractFetcher<T, ?> fetcher, LinkedMap pendingCheckpoints, boolean running) throws Exception
	{
		FlinkKafkaConsumerBase<T> consumer = new DummyFlinkKafkaConsumer<>();

		Field fetcherField = FlinkKafkaConsumerBase.class.getDeclaredField("kafkaFetcher");
		fetcherField.setAccessible(true);
		fetcherField.set(consumer, fetcher);

		Field mapField = FlinkKafkaConsumerBase.class.getDeclaredField("pendingCheckpoints");
		mapField.setAccessible(true);
		mapField.set(consumer, pendingCheckpoints);

		Field runningField = FlinkKafkaConsumerBase.class.getDeclaredField("running");
		runningField.setAccessible(true);
		runningField.set(consumer, running);

		return consumer;
	}

	// ------------------------------------------------------------------------

	private static final class DummyFlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		public DummyFlinkKafkaConsumer() {
			super((KeyedDeserializationSchema<T>) mock(KeyedDeserializationSchema.class));
		}

		@Override
		protected AbstractFetcher<T, ?> createFetcher(SourceContext<T> sourceContext, List<KafkaTopicPartition> thisSubtaskPartitions, SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic, SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated, StreamingRuntimeContext runtimeContext) throws Exception {
			return null;
		}
	}
}
