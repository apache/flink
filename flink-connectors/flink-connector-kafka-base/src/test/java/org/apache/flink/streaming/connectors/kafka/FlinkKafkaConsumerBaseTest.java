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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.streaming.connectors.kafka.testutils.TestPartitionDiscoverer;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.collections.map.LinkedMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

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

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsIn.isIn;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link FlinkKafkaConsumerBase}.
 */
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
		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		TestingListState<Tuple2<KafkaTopicPartition, Long>> listState = new TestingListState<>();
		when(operatorStateStore.getListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));

		assertFalse(listState.get().iterator().hasNext());
		consumer.notifyCheckpointComplete(66L);
	}

	/**
	 * Tests that when taking a checkpoint when the fetcher is not running yet,
	 * the checkpoint correctly contains the restored state instead.
	 */
	@Test
	public void checkRestoredCheckpointWhenFetcherNotReady() throws Exception {
		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);

		TestingListState<Serializable> restoredListState = new TestingListState<>();
		restoredListState.add(Tuple2.of(new KafkaTopicPartition("abc", 13), 16768L));
		restoredListState.add(Tuple2.of(new KafkaTopicPartition("def", 7), 987654321L));

		FlinkKafkaConsumerBase<String> consumer = getConsumer(null, new LinkedMap(), true);
		StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
		when(context.getNumberOfParallelSubtasks()).thenReturn(1);
		when(context.getIndexOfThisSubtask()).thenReturn(0);
		consumer.setRuntimeContext(context);

		// mock old 1.2 state (empty)
		when(operatorStateStore.getSerializableListState(Matchers.any(String.class))).thenReturn(new TestingListState<Serializable>());
		// mock 1.3 state
		when(operatorStateStore.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(restoredListState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);

		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(true);

		consumer.initializeState(initializationContext);

		consumer.open(new Configuration());

		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(17, 17));

		// ensure that the list was cleared and refilled. while this is an implementation detail, we use it here
		// to figure out that snapshotState() actually did something.
		Assert.assertTrue(restoredListState.isClearCalled());

		Set<Serializable> expected = new HashSet<>();

		for (Serializable serializable : restoredListState.get()) {
			expected.add(serializable);
		}

		int counter = 0;

		for (Serializable serializable : restoredListState.get()) {
			assertTrue(expected.contains(serializable));
			counter++;
		}

		assertEquals(expected.size(), counter);
	}

	@Test
	public void testConfigureOnCheckpointsCommitMode() throws Exception {

		DummyFlinkKafkaConsumer consumer = new DummyFlinkKafkaConsumer();
		consumer.setIsAutoCommitEnabled(true); // this should be ignored

		StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
		when(context.getIndexOfThisSubtask()).thenReturn(0);
		when(context.getNumberOfParallelSubtasks()).thenReturn(1);
		when(context.isCheckpointingEnabled()).thenReturn(true); // enable checkpointing, auto commit should be ignored
		consumer.setRuntimeContext(context);

		consumer.open(new Configuration());
		assertEquals(OffsetCommitMode.ON_CHECKPOINTS, consumer.getOffsetCommitMode());
	}

	@Test
	public void testConfigureAutoCommitMode() throws Exception {

		DummyFlinkKafkaConsumer consumer = new DummyFlinkKafkaConsumer();
		consumer.setIsAutoCommitEnabled(true);

		StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
		when(context.getIndexOfThisSubtask()).thenReturn(0);
		when(context.getNumberOfParallelSubtasks()).thenReturn(1);
		when(context.isCheckpointingEnabled()).thenReturn(false); // disable checkpointing, auto commit should be respected
		consumer.setRuntimeContext(context);

		consumer.open(new Configuration());
		assertEquals(OffsetCommitMode.KAFKA_PERIODIC, consumer.getOffsetCommitMode());
	}

	@Test
	public void testConfigureDisableOffsetCommitWithCheckpointing() throws Exception {

		DummyFlinkKafkaConsumer consumer = new DummyFlinkKafkaConsumer();
		consumer.setIsAutoCommitEnabled(true); // this should be ignored

		StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
		when(context.getIndexOfThisSubtask()).thenReturn(0);
		when(context.getNumberOfParallelSubtasks()).thenReturn(1);
		when(context.isCheckpointingEnabled()).thenReturn(true); // enable checkpointing, auto commit should be ignored
		consumer.setRuntimeContext(context);

		consumer.setCommitOffsetsOnCheckpoints(false); // disabling offset committing should override everything

		consumer.open(new Configuration());
		assertEquals(OffsetCommitMode.DISABLED, consumer.getOffsetCommitMode());
	}

	@Test
	public void testConfigureDisableOffsetCommitWithoutCheckpointing() throws Exception {

		DummyFlinkKafkaConsumer consumer = new DummyFlinkKafkaConsumer();
		consumer.setIsAutoCommitEnabled(false);

		StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
		when(context.getIndexOfThisSubtask()).thenReturn(0);
		when(context.getNumberOfParallelSubtasks()).thenReturn(1);
		when(context.isCheckpointingEnabled()).thenReturn(false); // disable checkpointing, auto commit should be respected
		consumer.setRuntimeContext(context);

		consumer.open(new Configuration());
		assertEquals(OffsetCommitMode.DISABLED, consumer.getOffsetCommitMode());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSnapshotStateWithCommitOnCheckpointsEnabled() throws Exception {

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
		StreamingRuntimeContext mockRuntimeContext = mock(StreamingRuntimeContext.class);
		when(mockRuntimeContext.isCheckpointingEnabled()).thenReturn(true); // enable checkpointing
		when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(0);
		when(mockRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(1);
		consumer.setRuntimeContext(mockRuntimeContext);

		assertEquals(0, pendingOffsetsToCommit.size());

		OperatorStateStore backend = mock(OperatorStateStore.class);

		TestingListState<Serializable> listState = new TestingListState<>();
		// mock old 1.2 state (empty)
		when(backend.getSerializableListState(Matchers.any(String.class))).thenReturn(new TestingListState<Serializable>());
		// mock 1.3 state
		when(backend.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);

		when(initializationContext.getOperatorStateStore()).thenReturn(backend);
		when(initializationContext.isRestored()).thenReturn(false, true, true, true);

		consumer.initializeState(initializationContext);

		consumer.open(new Configuration());

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
		when(operatorStateStore.getListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

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

	@Test
	@SuppressWarnings("unchecked")
	public void testSnapshotStateWithCommitOnCheckpointsDisabled() throws Exception {
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
		StreamingRuntimeContext mockRuntimeContext = mock(StreamingRuntimeContext.class);
		when(mockRuntimeContext.isCheckpointingEnabled()).thenReturn(true); // enable checkpointing
		when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(0);
		when(mockRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(1);
		consumer.setRuntimeContext(mockRuntimeContext);

		consumer.setCommitOffsetsOnCheckpoints(false); // disable offset committing

		assertEquals(0, pendingOffsetsToCommit.size());

		OperatorStateStore backend = mock(OperatorStateStore.class);

		TestingListState<Serializable> listState = new TestingListState<>();
		// mock old 1.2 state (empty)
		when(backend.getSerializableListState(Matchers.any(String.class))).thenReturn(new TestingListState<Serializable>());
		// mock 1.3 state
		when(backend.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);

		when(initializationContext.getOperatorStateStore()).thenReturn(backend);
		when(initializationContext.isRestored()).thenReturn(false, true, true, true);

		consumer.initializeState(initializationContext);

		consumer.open(new Configuration());

		// checkpoint 1
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));

		HashMap<KafkaTopicPartition, Long> snapshot1 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot1.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state1, snapshot1);
		assertEquals(0, pendingOffsetsToCommit.size()); // pending offsets to commit should not be updated

		// checkpoint 2
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(140, 140));

		HashMap<KafkaTopicPartition, Long> snapshot2 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot2.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state2, snapshot2);
		assertEquals(0, pendingOffsetsToCommit.size()); // pending offsets to commit should not be updated

		// ack checkpoint 1
		consumer.notifyCheckpointComplete(138L);
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed

		// checkpoint 3
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(141, 141));

		HashMap<KafkaTopicPartition, Long> snapshot3 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot3.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state3, snapshot3);
		assertEquals(0, pendingOffsetsToCommit.size()); // pending offsets to commit should not be updated

		// ack checkpoint 3, subsumes number 2
		consumer.notifyCheckpointComplete(141L);
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed

		consumer.notifyCheckpointComplete(666); // invalid checkpoint
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		listState = new TestingListState<>();
		when(operatorStateStore.getListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		// create 500 snapshots
		for (int i = 100; i < 600; i++) {
			consumer.snapshotState(new StateSnapshotContextSynchronousImpl(i, i));
			listState.clear();
		}
		assertEquals(0, pendingOffsetsToCommit.size()); // pending offsets to commit should not be updated

		// commit only the second last
		consumer.notifyCheckpointComplete(598);
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed

		// access invalid checkpoint
		consumer.notifyCheckpointComplete(590);
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed

		// and the last
		consumer.notifyCheckpointComplete(599);
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed
	}

	@Test
	public void testScaleUp() throws Exception {
		testRescaling(5, 2, 15, 1000);
	}

	@Test
	public void testScaleDown() throws Exception {
		testRescaling(5, 10, 2, 100);
	}

	/**
	 * Tests whether the Kafka consumer behaves correctly when scaling the parallelism up/down,
	 * which means that operator state is being reshuffled.
	 *
	 * <p>This also verifies that a restoring source is always impervious to changes in the list
	 * of topics fetched from Kafka.
	 */
	@SuppressWarnings("unchecked")
	void testRescaling(
		final int initialParallelism,
		final int numPartitions,
		final int restoredParallelism,
		final int restoredNumPartitions) throws Exception {

		Preconditions.checkArgument(
			restoredNumPartitions >= numPartitions,
			"invalid test case for Kafka repartitioning; Kafka only allows increasing partitions.");

		List<KafkaTopicPartition> mockFetchedPartitionsOnStartup = new ArrayList<>();
		for (int i = 0; i < numPartitions; i++) {
			mockFetchedPartitionsOnStartup.add(new KafkaTopicPartition("test-topic", i));
		}

		DummyFlinkKafkaConsumer<String>[] consumers =
			new DummyFlinkKafkaConsumer[initialParallelism];

		AbstractStreamOperatorTestHarness<String>[] testHarnesses =
			new AbstractStreamOperatorTestHarness[initialParallelism];

		for (int i = 0; i < initialParallelism; i++) {
			consumers[i] = new DummyFlinkKafkaConsumer<>(
				Collections.singletonList("test-topic"), mockFetchedPartitionsOnStartup);
			testHarnesses[i] = createTestHarness(consumers[i], initialParallelism, i);

			// initializeState() is always called, null signals that we didn't restore
			testHarnesses[i].initializeState(null);
			testHarnesses[i].open();
		}

		Map<KafkaTopicPartition, Long> globalSubscribedPartitions = new HashMap<>();

		for (int i = 0; i < initialParallelism; i++) {
			Map<KafkaTopicPartition, Long> subscribedPartitions =
				consumers[i].getSubscribedPartitionsToStartOffsets();

			// make sure that no one else is subscribed to these partitions
			for (KafkaTopicPartition partition : subscribedPartitions.keySet()) {
				assertThat(globalSubscribedPartitions, not(hasKey(partition)));
			}
			globalSubscribedPartitions.putAll(subscribedPartitions);
		}

		assertThat(globalSubscribedPartitions.values(), hasSize(numPartitions));
		assertThat(mockFetchedPartitionsOnStartup, everyItem(isIn(globalSubscribedPartitions.keySet())));

		OperatorStateHandles[] state = new OperatorStateHandles[initialParallelism];

		for (int i = 0; i < initialParallelism; i++) {
			state[i] = testHarnesses[i].snapshot(0, 0);
		}

		OperatorStateHandles mergedState = AbstractStreamOperatorTestHarness.repackageState(state);

		// -----------------------------------------------------------------------------------------
		// restore

		List<KafkaTopicPartition> mockFetchedPartitionsAfterRestore = new ArrayList<>();
		for (int i = 0; i < restoredNumPartitions; i++) {
			mockFetchedPartitionsAfterRestore.add(new KafkaTopicPartition("test-topic", i));
		}

		DummyFlinkKafkaConsumer<String>[] restoredConsumers =
			new DummyFlinkKafkaConsumer[restoredParallelism];

		AbstractStreamOperatorTestHarness<String>[] restoredTestHarnesses =
			new AbstractStreamOperatorTestHarness[restoredParallelism];

		for (int i = 0; i < restoredParallelism; i++) {
			restoredConsumers[i] = new DummyFlinkKafkaConsumer<>(
				Collections.singletonList("test-topic"), mockFetchedPartitionsAfterRestore);
			restoredTestHarnesses[i] = createTestHarness(restoredConsumers[i], restoredParallelism, i);

			// initializeState() is always called, null signals that we didn't restore
			restoredTestHarnesses[i].initializeState(mergedState);
			restoredTestHarnesses[i].open();
		}

		Map<KafkaTopicPartition, Long> restoredGlobalSubscribedPartitions = new HashMap<>();

		for (int i = 0; i < restoredParallelism; i++) {
			Map<KafkaTopicPartition, Long> subscribedPartitions =
				restoredConsumers[i].getSubscribedPartitionsToStartOffsets();

			// make sure that no one else is subscribed to these partitions
			for (KafkaTopicPartition partition : subscribedPartitions.keySet()) {
				assertThat(restoredGlobalSubscribedPartitions, not(hasKey(partition)));
			}
			restoredGlobalSubscribedPartitions.putAll(subscribedPartitions);
		}

		assertThat(restoredGlobalSubscribedPartitions.values(), hasSize(restoredNumPartitions));
		assertThat(mockFetchedPartitionsOnStartup, everyItem(isIn(restoredGlobalSubscribedPartitions.keySet())));
	}

	// ------------------------------------------------------------------------

	private static <T> FlinkKafkaConsumerBase<T> getConsumer(
			AbstractFetcher<T, ?> fetcher, LinkedMap pendingOffsetsToCommit, boolean running) throws Exception {
		FlinkKafkaConsumerBase<T> consumer = new DummyFlinkKafkaConsumer<>();
		StreamingRuntimeContext mockRuntimeContext = mock(StreamingRuntimeContext.class);
		Mockito.when(mockRuntimeContext.isCheckpointingEnabled()).thenReturn(true);
		consumer.setRuntimeContext(mockRuntimeContext);

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

	private static <T> AbstractStreamOperatorTestHarness<T> createTestHarness(
		SourceFunction<T> source, int numSubtasks, int subtaskIndex) throws Exception {

		AbstractStreamOperatorTestHarness<T> testHarness =
			new AbstractStreamOperatorTestHarness<>(
				new StreamSource<>(source), Short.MAX_VALUE / 2, numSubtasks, subtaskIndex);

		testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

		return testHarness;
	}


	// ------------------------------------------------------------------------

	private static class DummyFlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {
		private static final long serialVersionUID = 1L;

		boolean isAutoCommitEnabled = false;

		private List<String> fixedMockGetAllTopicsReturnSequence;
		private List<KafkaTopicPartition> fixedMockGetAllPartitionsForTopicsReturnSequence;

		public DummyFlinkKafkaConsumer() {
			this(Collections.singletonList("dummy-topic"), Collections.singletonList(new KafkaTopicPartition("dummy-topic", 0)));
		}

		@SuppressWarnings("unchecked")
		public DummyFlinkKafkaConsumer(
				List<String> fixedMockGetAllTopicsReturnSequence,
				List<KafkaTopicPartition> fixedMockGetAllPartitionsForTopicsReturnSequence) {
			super(Arrays.asList("dummy-topic"), null, (KeyedDeserializationSchema < T >) mock(KeyedDeserializationSchema.class), 0);
			this.fixedMockGetAllTopicsReturnSequence = Preconditions.checkNotNull(fixedMockGetAllTopicsReturnSequence);
			this.fixedMockGetAllPartitionsForTopicsReturnSequence = Preconditions.checkNotNull(fixedMockGetAllPartitionsForTopicsReturnSequence);
		}

		@Override
		@SuppressWarnings("unchecked")
		protected AbstractFetcher<T, ?> createFetcher(
				SourceContext<T> sourceContext,
				Map<KafkaTopicPartition, Long> thisSubtaskPartitionsWithStartOffsets,
				SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
				SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
				StreamingRuntimeContext runtimeContext,
				OffsetCommitMode offsetCommitMode) throws Exception {
			return mock(AbstractFetcher.class);
		}

		@Override
		protected AbstractPartitionDiscoverer createPartitionDiscoverer(
				KafkaTopicsDescriptor topicsDescriptor,
				int indexOfThisSubtask,
				int numParallelSubtasks) {
			return new TestPartitionDiscoverer(
				topicsDescriptor,
				indexOfThisSubtask,
				numParallelSubtasks,
				TestPartitionDiscoverer.createMockGetAllTopicsSequenceFromFixedReturn(fixedMockGetAllTopicsReturnSequence),
				TestPartitionDiscoverer.createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(fixedMockGetAllPartitionsForTopicsReturnSequence));
		}

		@Override
		protected boolean getIsAutoCommitEnabled() {
			return isAutoCommitEnabled;
		}

		public void setIsAutoCommitEnabled(boolean isAutoCommitEnabled) {
			this.isAutoCommitEnabled = isAutoCommitEnabled;
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
