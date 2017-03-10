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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlinkKafkaConsumerBaseTest {

	/**
	 * Tests that not both types of timestamp extractors / watermark generators can be used.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testEitherWatermarkExtractor() {
		try {
			new DummyFlinkKafkaConsumer<>(mock(AbstractFetcher.class), Collections.<KafkaTopicPartition>emptyList())
					.assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<Object>) null);
			fail();
		} catch (NullPointerException ignored) {}

		try {
			new DummyFlinkKafkaConsumer<>(mock(AbstractFetcher.class), Collections.<KafkaTopicPartition>emptyList())
					.assignTimestampsAndWatermarks((AssignerWithPunctuatedWatermarks<Object>) null);
			fail();
		} catch (NullPointerException ignored) {}
		
		@SuppressWarnings("unchecked")
		final AssignerWithPeriodicWatermarks<String> periodicAssigner = mock(AssignerWithPeriodicWatermarks.class);
		@SuppressWarnings("unchecked")
		final AssignerWithPunctuatedWatermarks<String> punctuatedAssigner = mock(AssignerWithPunctuatedWatermarks.class);
		
		DummyFlinkKafkaConsumer<String> c1 = new DummyFlinkKafkaConsumer<>(mock(AbstractFetcher.class), Collections.<KafkaTopicPartition>emptyList());
		c1.assignTimestampsAndWatermarks(periodicAssigner);
		try {
			c1.assignTimestampsAndWatermarks(punctuatedAssigner);
			fail();
		} catch (IllegalStateException ignored) {}

		DummyFlinkKafkaConsumer<String> c2 = new DummyFlinkKafkaConsumer<>(mock(AbstractFetcher.class), Collections.<KafkaTopicPartition>emptyList());
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
		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		TestingListState<Tuple2<KafkaTopicPartition, Long>> listState = new TestingListState<>();
		when(operatorStateStore.getOperatorState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));

		assertFalse(listState.get().iterator().hasNext());
		consumer.notifyCheckpointComplete(66L);
	}

	/**
	 * Tests that no checkpoints happen when the fetcher is not running.
	 */
	@Test
	public void checkRestoredCheckpointWhenFetcherNotReady() throws Exception {
		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);

		TestingListState<Serializable> listState = new TestingListState<>();
		listState.add(Tuple2.of(new KafkaTopicPartition("abc", 13), 16768L));
		listState.add(Tuple2.of(new KafkaTopicPartition("def", 7), 987654321L));

		FlinkKafkaConsumerBase<String> consumer = getConsumer(null, new LinkedMap(), true);

		when(operatorStateStore.getSerializableListState(Matchers.any(String.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);

		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(true);

		consumer.initializeState(initializationContext);

		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(17, 17));

		// ensure that the list was cleared and refilled. while this is an implementation detail, we use it here
		// to figure out that snapshotState() actually did something.
		Assert.assertTrue(listState.isClearCalled());

		Set<Serializable> expected = new HashSet<>();

		for (Serializable serializable : listState.get()) {
			expected.add(serializable);
		}

		int counter = 0;

		for (Serializable serializable : listState.get()) {
			assertTrue(expected.contains(serializable));
			counter++;
		}

		assertEquals(expected.size(), counter);
	}

	/**
	 * Tests that no checkpoints happen when the fetcher is not running.
	 */
	@Test
	public void checkRestoredNullCheckpointWhenFetcherNotReady() throws Exception {
		FlinkKafkaConsumerBase<String> consumer = getConsumer(null, new LinkedMap(), true);

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		TestingListState<Serializable> listState = new TestingListState<>();
		when(operatorStateStore.getSerializableListState(Matchers.any(String.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);

		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(false);

		consumer.initializeState(initializationContext);

		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(17, 17));

		assertFalse(listState.get().iterator().hasNext());
	}

	/**
	 * Tests that on snapshots, states and offsets to commit to Kafka are correct
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void checkUseFetcherWhenNoCheckpoint() throws Exception {

		FlinkKafkaConsumerBase<String> consumer = getConsumer(mock(AbstractFetcher.class), new LinkedMap(), true);
		List<KafkaTopicPartition> partitionList = new ArrayList<>(1);
		partitionList.add(new KafkaTopicPartition("test", 0));
		consumer.setSubscribedPartitions(partitionList);

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		TestingListState<Serializable> listState = new TestingListState<>();
		when(operatorStateStore.getSerializableListState(Matchers.any(String.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);

		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);

		// make the context signal that there is no restored state, then validate that
		when(initializationContext.isRestored()).thenReturn(false);
		consumer.initializeState(initializationContext);
		consumer.run(mock(SourceFunction.SourceContext.class));
	}

	/**
	 * Tests that the fetcher is restored with all partitions in the restored state,
	 * regardless of the queried complete list of Kafka partitions.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testStateIntactOnRestore() throws Exception {
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		FlinkKafkaConsumerBase<String> consumer = new DummyFlinkKafkaConsumer<>(
				fetcher,
				Collections.<KafkaTopicPartition>emptyList()); // mock queried list; this should not affect restore state

		final OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		final ListState mockListState = mock(ListState.class);
		List<Tuple2<KafkaTopicPartition, Long>> restoredPartitionOffsets = new ArrayList<>(3);
		restoredPartitionOffsets.add(new Tuple2<>(new KafkaTopicPartition("test-topic", 0), 23L));
		restoredPartitionOffsets.add(new Tuple2<>(new KafkaTopicPartition("test-topic", 1), 40L));
		restoredPartitionOffsets.add(new Tuple2<>(new KafkaTopicPartition("test-topic", 2), 32L));
		when(mockListState.get()).thenReturn(restoredPartitionOffsets);
		when(operatorStateStore.getSerializableListState(Matchers.anyString())).thenReturn(mockListState);

		List<KafkaTopicPartition> expectedSubscribedPartitions = new ArrayList<>(3);
		expectedSubscribedPartitions.add(new KafkaTopicPartition("test-topic", 0));
		expectedSubscribedPartitions.add(new KafkaTopicPartition("test-topic", 1));
		expectedSubscribedPartitions.add(new KafkaTopicPartition("test-topic", 2));

		Map<KafkaTopicPartition, Long> expectedFetcherRestoreState = new HashMap<>();
		expectedFetcherRestoreState.put(new KafkaTopicPartition("test-topic", 0), 23L);
		expectedFetcherRestoreState.put(new KafkaTopicPartition("test-topic", 1), 40L);
		expectedFetcherRestoreState.put(new KafkaTopicPartition("test-topic", 2), 32L);

		consumer.initializeState(new FunctionInitializationContext() {
			@Override
			public boolean isRestored() {
				return true;
			}

			@Override
			public OperatorStateStore getOperatorStateStore() {
				return operatorStateStore;
			}

			@Override
			public KeyedStateStore getKeyedStateStore() {
				throw new UnsupportedOperationException();
			}
		});

		consumer.open(new Configuration());

		consumer.run(mock(SourceFunction.SourceContext.class));

		assertEquals(expectedSubscribedPartitions.size(), consumer.getSubscribedPartitions().size());
		for (KafkaTopicPartition expectedPartition : expectedSubscribedPartitions) {
			consumer.getSubscribedPartitions().contains(expectedPartition);
		}
		verify(fetcher).restoreOffsets(expectedFetcherRestoreState);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSnapshotState() throws Exception {

		// --------------------------------------------------------------------
		//   prepare fake states
		// --------------------------------------------------------------------

		final HashMap<KafkaTopicPartition, Long> state1 = new HashMap<>();
		state1.put(new KafkaTopicPartition("abc", 13), 16768L);
		state1.put(new KafkaTopicPartition("def", 7), 987654321L);

		final HashMap<KafkaTopicPartition, Long> state2 = new HashMap<>();
		state2.put(new KafkaTopicPartition("abc", 13), 16770L);
		state2.put(new KafkaTopicPartition("def", 7), 987654329L);

		final HashMap<KafkaTopicPartition, Long> state3 = new HashMap<>();
		state3.put(new KafkaTopicPartition("abc", 13), 16780L);
		state3.put(new KafkaTopicPartition("def", 7), 987654377L);

		// --------------------------------------------------------------------
		
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);
		when(fetcher.snapshotCurrentState()).thenReturn(state1, state2, state3);
			
		final LinkedMap pendingOffsetsToCommit = new LinkedMap();
	
		FlinkKafkaConsumerBase<String> consumer = getConsumer(fetcher, pendingOffsetsToCommit, true);
		assertEquals(0, pendingOffsetsToCommit.size());

		OperatorStateStore backend = mock(OperatorStateStore.class);

		TestingListState<Serializable> listState = new TestingListState<>();

		when(backend.getSerializableListState(Matchers.any(String.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);

		when(initializationContext.getOperatorStateStore()).thenReturn(backend);
		when(initializationContext.isRestored()).thenReturn(false, true, true, true);

		consumer.initializeState(initializationContext);

		// checkpoint 1
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));

		HashMap<KafkaTopicPartition, Long> snapshot1 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot1.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state1, snapshot1);
		assertEquals(1, pendingOffsetsToCommit.size());
		assertEquals(state1, pendingOffsetsToCommit.get(138L));

		// checkpoint 2
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(140, 140));

		HashMap<KafkaTopicPartition, Long> snapshot2 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot2.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state2, snapshot2);
		assertEquals(2, pendingOffsetsToCommit.size());
		assertEquals(state2, pendingOffsetsToCommit.get(140L));
		
		// ack checkpoint 1
		consumer.notifyCheckpointComplete(138L);
		assertEquals(1, pendingOffsetsToCommit.size());
		assertTrue(pendingOffsetsToCommit.containsKey(140L));

		// checkpoint 3
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(141, 141));

		HashMap<KafkaTopicPartition, Long> snapshot3 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot3.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state3, snapshot3);
		assertEquals(2, pendingOffsetsToCommit.size());
		assertEquals(state3, pendingOffsetsToCommit.get(141L));
		
		// ack checkpoint 3, subsumes number 2
		consumer.notifyCheckpointComplete(141L);
		assertEquals(0, pendingOffsetsToCommit.size());


		consumer.notifyCheckpointComplete(666); // invalid checkpoint
		assertEquals(0, pendingOffsetsToCommit.size());

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		listState = new TestingListState<>();
		when(operatorStateStore.getOperatorState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		// create 500 snapshots
		for (int i = 100; i < 600; i++) {
			consumer.snapshotState(new StateSnapshotContextSynchronousImpl(i, i));
			listState.clear();
		}
		assertEquals(FlinkKafkaConsumerBase.MAX_NUM_PENDING_CHECKPOINTS, pendingOffsetsToCommit.size());

		// commit only the second last
		consumer.notifyCheckpointComplete(598);
		assertEquals(1, pendingOffsetsToCommit.size());

		// access invalid checkpoint
		consumer.notifyCheckpointComplete(590);

		// and the last
		consumer.notifyCheckpointComplete(599);
		assertEquals(0, pendingOffsetsToCommit.size());
	}

	// ------------------------------------------------------------------------

	private static <T> FlinkKafkaConsumerBase<T> getConsumer(
			AbstractFetcher<T, ?> fetcher, LinkedMap pendingOffsetsToCommit, boolean running) throws Exception
	{
		FlinkKafkaConsumerBase<T> consumer = new DummyFlinkKafkaConsumer<>(fetcher, Collections.<KafkaTopicPartition>emptyList());

		Field fetcherField = FlinkKafkaConsumerBase.class.getDeclaredField("kafkaFetcher");
		fetcherField.setAccessible(true);
		fetcherField.set(consumer, fetcher);

		Field mapField = FlinkKafkaConsumerBase.class.getDeclaredField("pendingOffsetsToCommit");
		mapField.setAccessible(true);
		mapField.set(consumer, pendingOffsetsToCommit);

		Field runningField = FlinkKafkaConsumerBase.class.getDeclaredField("running");
		runningField.setAccessible(true);
		runningField.set(consumer, running);

		return consumer;
	}

	// ------------------------------------------------------------------------

	private static class DummyFlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {
		private static final long serialVersionUID = 1L;

		private final AbstractFetcher<T, ?> mockFetcher;
		private final List<KafkaTopicPartition> mockKafkaPartitions;

		@SuppressWarnings("unchecked")
		public DummyFlinkKafkaConsumer(AbstractFetcher<T, ?> mockFetcher, List<KafkaTopicPartition> mockKafkaPartitions) {
			super(Arrays.asList("dummy-topic"), (KeyedDeserializationSchema < T >) mock(KeyedDeserializationSchema.class));
			this.mockFetcher = mockFetcher;
			this.mockKafkaPartitions = mockKafkaPartitions;
		}

		@Override
		protected AbstractFetcher<T, ?> createFetcher(
				SourceContext<T> sourceContext,
				List<KafkaTopicPartition> thisSubtaskPartitions,
				SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
				SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
				StreamingRuntimeContext runtimeContext) throws Exception {
			return mockFetcher;
		}

		@Override
		protected List<KafkaTopicPartition> getKafkaPartitions(List<String> topics) {
			return mockKafkaPartitions;
		}

		@Override
		public RuntimeContext getRuntimeContext() {
			return mock(StreamingRuntimeContext.class);
		}
	}

	private static final class TestingListState<T> implements ListState<T> {

		private final List<T> list = new ArrayList<>();
		private boolean clearCalled = false;

		@Override
		public void clear() {
			list.clear();
			clearCalled = true;
		}

		@Override
		public Iterable<T> get() throws Exception {
			return list;
		}

		@Override
		public void add(T value) throws Exception {
			list.add(value);
		}

		public List<T> getList() {
			return list;
		}

		public boolean isClearCalled() {
			return clearCalled;
		}
	}
}
