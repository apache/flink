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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.ArrayList;
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
	@SuppressWarnings("unchecked")
	public void testEitherWatermarkExtractor() {
		try {
			new DummyFlinkKafkaConsumer<String>().assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<String>) null);
			fail();
		} catch (NullPointerException ignored) {}

		try {
			new DummyFlinkKafkaConsumer<String>().assignTimestampsAndWatermarks((AssignerWithPunctuatedWatermarks<String>) null);
			fail();
		} catch (NullPointerException ignored) {}

		final AssignerWithPeriodicWatermarks<String> periodicAssigner = mock(AssignerWithPeriodicWatermarks.class);
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
		final FlinkKafkaConsumerBase<String> consumer = new DummyFlinkKafkaConsumer<>();

		final TestingListState<Tuple2<KafkaTopicPartition, Long>> listState = new TestingListState<>();
		setupConsumer(consumer, false, listState, true, 0, 1);

		// snapshot before the fetcher starts running
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));

		// no state should have been checkpointed
		assertFalse(listState.get().iterator().hasNext());
	}

	/**
	 * Tests that when taking a checkpoint when the fetcher is not running yet,
	 * the checkpoint correctly contains the restored state instead.
	 */
	@Test
	public void checkRestoredCheckpointWhenFetcherNotReady() throws Exception {
		@SuppressWarnings("unchecked")
		final FlinkKafkaConsumerBase<String> consumer = new DummyFlinkKafkaConsumer<>();

		final TestingListState<Tuple2<KafkaTopicPartition, Long>> restoredListState = new TestingListState<>();
		setupConsumer(consumer, true, restoredListState, true, 0, 1);

		// snapshot before the fetcher starts running
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
		@SuppressWarnings("unchecked")
		// auto-commit enabled; this should be ignored in this case
		final DummyFlinkKafkaConsumer<String> consumer = new DummyFlinkKafkaConsumer<>(true);

		setupConsumer(
			consumer,
			false,
			null,
			true, // enable checkpointing; auto commit should be ignored
			0,
			1);

		assertEquals(OffsetCommitMode.ON_CHECKPOINTS, consumer.getOffsetCommitMode());
	}

	@Test
	public void testConfigureAutoCommitMode() throws Exception {
		@SuppressWarnings("unchecked")
		final DummyFlinkKafkaConsumer<String> consumer = new DummyFlinkKafkaConsumer<>(true);

		setupConsumer(
			consumer,
			false,
			null,
			false, // disable checkpointing; auto commit should be respected
			0,
			1);

		assertEquals(OffsetCommitMode.KAFKA_PERIODIC, consumer.getOffsetCommitMode());
	}

	@Test
	public void testConfigureDisableOffsetCommitWithCheckpointing() throws Exception {
		@SuppressWarnings("unchecked")
		// auto-commit enabled; this should be ignored in this case
		final DummyFlinkKafkaConsumer<String> consumer = new DummyFlinkKafkaConsumer<>(true);
		consumer.setCommitOffsetsOnCheckpoints(false); // disabling offset committing should override everything

		setupConsumer(
			consumer,
			false,
			null,
			true, // enable checkpointing; auto commit should be ignored
			0,
			1);

		assertEquals(OffsetCommitMode.DISABLED, consumer.getOffsetCommitMode());
	}

	@Test
	public void testConfigureDisableOffsetCommitWithoutCheckpointing() throws Exception {
		@SuppressWarnings("unchecked")
		final DummyFlinkKafkaConsumer<String> consumer = new DummyFlinkKafkaConsumer<>(false);

		setupConsumer(
			consumer,
			false,
			null,
			false, // disable checkpointing; auto commit should be respected
			0,
			1);

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

		final OneShotLatch runLatch = new OneShotLatch();
		final OneShotLatch stopLatch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = getRunnableMockFetcher(runLatch, stopLatch);
		when(fetcher.snapshotCurrentState()).thenReturn(state1, state2, state3);

		final FlinkKafkaConsumerBase<String> consumer = new DummyFlinkKafkaConsumer<>(
				fetcher,
				mock(AbstractPartitionDiscoverer.class),
				false);

		final TestingListState<Serializable> listState = new TestingListState<>();

		// setup and run the consumer; wait until the consumer reaches the main fetch loop before continuing test
		setupConsumer(consumer, false, listState, true, 0, 1);

		final CheckedThread runThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				consumer.run(mock(SourceFunction.SourceContext.class));
			}

			@Override
			public void sync() throws Exception {
				stopLatch.trigger();
				super.sync();
			}
		};
		runThread.start();
		runLatch.await();

		assertEquals(0, consumer.getPendingOffsetsToCommit().size());

		// checkpoint 1
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));

		HashMap<KafkaTopicPartition, Long> snapshot1 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot1.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state1, snapshot1);
		assertEquals(1, consumer.getPendingOffsetsToCommit().size());
		assertEquals(state1, consumer.getPendingOffsetsToCommit().get(138L));

		// checkpoint 2
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(140, 140));

		HashMap<KafkaTopicPartition, Long> snapshot2 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot2.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state2, snapshot2);
		assertEquals(2, consumer.getPendingOffsetsToCommit().size());
		assertEquals(state2, consumer.getPendingOffsetsToCommit().get(140L));

		// ack checkpoint 1
		consumer.notifyCheckpointComplete(138L);
		assertEquals(1, consumer.getPendingOffsetsToCommit().size());
		assertTrue(consumer.getPendingOffsetsToCommit().containsKey(140L));

		// checkpoint 3
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(141, 141));

		HashMap<KafkaTopicPartition, Long> snapshot3 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot3.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state3, snapshot3);
		assertEquals(2, consumer.getPendingOffsetsToCommit().size());
		assertEquals(state3, consumer.getPendingOffsetsToCommit().get(141L));

		// ack checkpoint 3, subsumes number 2
		consumer.notifyCheckpointComplete(141L);
		assertEquals(0, consumer.getPendingOffsetsToCommit().size());

		consumer.notifyCheckpointComplete(666); // invalid checkpoint
		assertEquals(0, consumer.getPendingOffsetsToCommit().size());

		// create 500 snapshots
		for (int i = 100; i < 600; i++) {
			consumer.snapshotState(new StateSnapshotContextSynchronousImpl(i, i));
			listState.clear();
		}
		assertEquals(FlinkKafkaConsumerBase.MAX_NUM_PENDING_CHECKPOINTS, consumer.getPendingOffsetsToCommit().size());

		// commit only the second last
		consumer.notifyCheckpointComplete(598);
		assertEquals(1, consumer.getPendingOffsetsToCommit().size());

		// access invalid checkpoint
		consumer.notifyCheckpointComplete(590);

		// and the last
		consumer.notifyCheckpointComplete(599);
		assertEquals(0, consumer.getPendingOffsetsToCommit().size());

		runThread.sync();
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

		final OneShotLatch runLatch = new OneShotLatch();
		final OneShotLatch stopLatch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = getRunnableMockFetcher(runLatch, stopLatch);
		when(fetcher.snapshotCurrentState()).thenReturn(state1, state2, state3);

		final FlinkKafkaConsumerBase<String> consumer = new DummyFlinkKafkaConsumer<>(
				fetcher,
				mock(AbstractPartitionDiscoverer.class),
				false);
		consumer.setCommitOffsetsOnCheckpoints(false); // disable offset committing

		final TestingListState<Serializable> listState = new TestingListState<>();

		// setup and run the consumer; wait until the consumer reaches the main fetch loop before continuing test
		setupConsumer(consumer, false, listState, true, 0, 1);

		final CheckedThread runThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				consumer.run(mock(SourceFunction.SourceContext.class));
			}

			@Override
			public void sync() throws Exception {
				stopLatch.trigger();
				super.sync();
			}
		};
		runThread.start();
		runLatch.await();

		assertEquals(0, consumer.getPendingOffsetsToCommit().size());

		// checkpoint 1
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));

		HashMap<KafkaTopicPartition, Long> snapshot1 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot1.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state1, snapshot1);
		assertEquals(0, consumer.getPendingOffsetsToCommit().size()); // pending offsets to commit should not be updated

		// checkpoint 2
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(140, 140));

		HashMap<KafkaTopicPartition, Long> snapshot2 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionLongTuple2 = (Tuple2<KafkaTopicPartition, Long>) serializable;
			snapshot2.put(kafkaTopicPartitionLongTuple2.f0, kafkaTopicPartitionLongTuple2.f1);
		}

		assertEquals(state2, snapshot2);
		assertEquals(0, consumer.getPendingOffsetsToCommit().size()); // pending offsets to commit should not be updated

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
		assertEquals(0, consumer.getPendingOffsetsToCommit().size()); // pending offsets to commit should not be updated

		// ack checkpoint 3, subsumes number 2
		consumer.notifyCheckpointComplete(141L);
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed

		consumer.notifyCheckpointComplete(666); // invalid checkpoint
		verify(fetcher, never()).commitInternalOffsetsToKafka(anyMap(), Matchers.any(KafkaCommitCallback.class)); // no offsets should be committed

		// create 500 snapshots
		for (int i = 100; i < 600; i++) {
			consumer.snapshotState(new StateSnapshotContextSynchronousImpl(i, i));
			listState.clear();
		}
		assertEquals(0, consumer.getPendingOffsetsToCommit().size()); // pending offsets to commit should not be updated

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
		testRescaling(5, 2, 8, 30);
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
	private void testRescaling(
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
			TestPartitionDiscoverer partitionDiscoverer = new TestPartitionDiscoverer(
				new KafkaTopicsDescriptor(Collections.singletonList("test-topic"), null),
				i,
				initialParallelism,
				TestPartitionDiscoverer.createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList("test-topic")),
				TestPartitionDiscoverer.createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(mockFetchedPartitionsOnStartup));

			consumers[i] = new DummyFlinkKafkaConsumer<>(mock(AbstractFetcher.class), partitionDiscoverer, false);
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
			TestPartitionDiscoverer partitionDiscoverer = new TestPartitionDiscoverer(
				new KafkaTopicsDescriptor(Collections.singletonList("test-topic"), null),
				i,
				restoredParallelism,
				TestPartitionDiscoverer.createMockGetAllTopicsSequenceFromFixedReturn(Collections.singletonList("test-topic")),
				TestPartitionDiscoverer.createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(mockFetchedPartitionsAfterRestore));

			restoredConsumers[i] = new DummyFlinkKafkaConsumer<>(mock(AbstractFetcher.class), partitionDiscoverer, false);
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

	private static <T> AbstractStreamOperatorTestHarness<T> createTestHarness(
		SourceFunction<T> source, int numSubtasks, int subtaskIndex) throws Exception {

		AbstractStreamOperatorTestHarness<T> testHarness =
			new AbstractStreamOperatorTestHarness<>(
				new StreamSource<>(source), Short.MAX_VALUE / 2, numSubtasks, subtaskIndex);

		testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

		return testHarness;
	}


	// ------------------------------------------------------------------------

	/**
	 * An instantiable dummy {@link FlinkKafkaConsumerBase} that supports injecting
	 * mocks for {@link FlinkKafkaConsumerBase#kafkaFetcher}, {@link FlinkKafkaConsumerBase#partitionDiscoverer},
	 * and {@link FlinkKafkaConsumerBase#getIsAutoCommitEnabled()}.
	 */
	private static class DummyFlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {
		private static final long serialVersionUID = 1L;

		private AbstractFetcher<T, ?> testFetcher;
		private AbstractPartitionDiscoverer testPartitionDiscoverer;
		private boolean isAutoCommitEnabled;

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaConsumer() {
			this(false);
		}

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaConsumer(boolean isAutoCommitEnabled) {
			this(mock(AbstractFetcher.class), mock(AbstractPartitionDiscoverer.class), isAutoCommitEnabled);
		}

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaConsumer(
				AbstractFetcher<T, ?> testFetcher,
				AbstractPartitionDiscoverer testPartitionDiscoverer,
				boolean isAutoCommitEnabled) {

			super(
					Collections.singletonList("dummy-topic"),
					null,
					(KeyedDeserializationSchema < T >) mock(KeyedDeserializationSchema.class),
					PARTITION_DISCOVERY_DISABLED);

			this.testFetcher = testFetcher;
			this.testPartitionDiscoverer = testPartitionDiscoverer;
			this.isAutoCommitEnabled = isAutoCommitEnabled;
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
			return this.testFetcher;
		}

		@Override
		protected AbstractPartitionDiscoverer createPartitionDiscoverer(
				KafkaTopicsDescriptor topicsDescriptor,
				int indexOfThisSubtask,
				int numParallelSubtasks) {
			return this.testPartitionDiscoverer;
		}

		@Override
		protected boolean getIsAutoCommitEnabled() {
			return this.isAutoCommitEnabled;
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

		boolean isClearCalled() {
			return clearCalled;
		}

		@Override
		public void update(List<T> values) throws Exception {
			clear();

			if (values != null && !values.isEmpty()) {
				list.addAll(values);
			}
		}
	}

	/**
	 * Returns a mock {@link AbstractFetcher}, with run / stop latches injected in
	 * the {@link AbstractFetcher#runFetchLoop()} method.
	 */
	private static <T> AbstractFetcher<T, ?> getRunnableMockFetcher(
			OneShotLatch runLatch,
			OneShotLatch stopLatch) throws Exception {

		@SuppressWarnings("unchecked")
		final AbstractFetcher<T, ?> fetcher = mock(AbstractFetcher.class);

		Mockito.doAnswer(invocationOnMock -> {
			runLatch.trigger();
			stopLatch.await();
			return null;
		}).when(fetcher).runFetchLoop();

		return fetcher;
	}

	@SuppressWarnings("unchecked")
	private static <T, S> void setupConsumer(
			FlinkKafkaConsumerBase<T> consumer,
			boolean isRestored,
			ListState<S> restoredListState,
			boolean isCheckpointingEnabled,
			int subtaskIndex,
			int totalNumSubtasks) throws Exception {

		// run setup procedure in operator life cycle
		consumer.setRuntimeContext(new MockRuntimeContext(isCheckpointingEnabled, totalNumSubtasks, subtaskIndex));
		consumer.initializeState(new MockFunctionInitializationContext(isRestored, new MockOperatorStateStore(restoredListState)));
		consumer.open(new Configuration());
	}

	private static class MockRuntimeContext extends StreamingRuntimeContext {

		private final boolean isCheckpointingEnabled;

		private final int numParallelSubtasks;
		private final int subtaskIndex;

		private MockRuntimeContext(
				boolean isCheckpointingEnabled,
				int numParallelSubtasks,
				int subtaskIndex) {

			super(
				new MockStreamOperator(),
				new MockEnvironment("mockTask", 4 * MemoryManager.DEFAULT_PAGE_SIZE, null, 16),
				Collections.<String, Accumulator<?, ?>>emptyMap());

			this.isCheckpointingEnabled = isCheckpointingEnabled;
			this.numParallelSubtasks = numParallelSubtasks;
			this.subtaskIndex = subtaskIndex;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return new UnregisteredMetricsGroup();
		}

		@Override
		public boolean isCheckpointingEnabled() {
			return isCheckpointingEnabled;
		}

		@Override
		public int getIndexOfThisSubtask() {
			return subtaskIndex;
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return numParallelSubtasks;
		}

		// ------------------------------------------------------------------------

		private static class MockStreamOperator extends AbstractStreamOperator<Integer> {
			private static final long serialVersionUID = -1153976702711944427L;

			@Override
			public ExecutionConfig getExecutionConfig() {
				return new ExecutionConfig();
			}
		}
	}

	private static class MockOperatorStateStore implements OperatorStateStore {

		private final ListState<?> mockRestoredUnionListState;

		private MockOperatorStateStore(ListState<?> restoredUnionListState) {
			this.mockRestoredUnionListState = restoredUnionListState;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			return (ListState<S>) mockRestoredUnionListState;
		}

		@Override
		public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
			// return empty state for the legacy 1.2 Kafka consumer state
			return new TestingListState<>();
		}

		// ------------------------------------------------------------------------

		@Override
		public <S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<String> getRegisteredStateNames() {
			throw new UnsupportedOperationException();
		}
	}

	private static class MockFunctionInitializationContext implements FunctionInitializationContext {

		private final boolean isRestored;
		private final OperatorStateStore operatorStateStore;

		private MockFunctionInitializationContext(boolean isRestored, OperatorStateStore operatorStateStore) {
			this.isRestored = isRestored;
			this.operatorStateStore = operatorStateStore;
		}

		@Override
		public boolean isRestored() {
			return isRestored;
		}

		@Override
		public OperatorStateStore getOperatorStateStore() {
			return operatorStateStore;
		}

		@Override
		public KeyedStateStore getKeyedStateStore() {
			throw new UnsupportedOperationException();
		}
	}
}
