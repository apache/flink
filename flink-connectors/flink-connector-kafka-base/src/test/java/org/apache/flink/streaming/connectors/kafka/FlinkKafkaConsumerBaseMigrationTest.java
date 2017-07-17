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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.migration.MigrationTestUtil;
import org.apache.flink.streaming.util.migration.MigrationVersion;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;

/**
 * Tests for checking whether {@link FlinkKafkaConsumerBase} can restore from snapshots that were
 * done using previous Flink versions' {@link FlinkKafkaConsumerBase}.
 *
 * <p>For regenerating the binary snapshot files run {@link #writeSnapshot()} on the corresponding
 * Flink release-* branch.
 */
@RunWith(Parameterized.class)
public class FlinkKafkaConsumerBaseMigrationTest {

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on write*Snapshot() methods to generate savepoints
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = null;

	final static HashMap<KafkaTopicPartition, Long> PARTITION_STATE = new HashMap<>();

	static {
		PARTITION_STATE.put(new KafkaTopicPartition("abc", 13), 16768L);
		PARTITION_STATE.put(new KafkaTopicPartition("def", 7), 987654321L);
	}

	private final MigrationVersion testMigrateVersion;

	@Parameterized.Parameters(name = "Migration Savepoint: {0}")
	public static Collection<MigrationVersion> parameters () {
		return Arrays.asList(MigrationVersion.v1_1, MigrationVersion.v1_2, MigrationVersion.v1_3);
	}

	public FlinkKafkaConsumerBaseMigrationTest(MigrationVersion testMigrateVersion) {
		this.testMigrateVersion = testMigrateVersion;
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeSnapshot() throws Exception {
		writeSnapshot("src/test/resources/kafka-consumer-migration-test-flink" + flinkGenerateSavepointVersion + "-snapshot", PARTITION_STATE);

		final HashMap<KafkaTopicPartition, Long> emptyState = new HashMap<>();
		writeSnapshot("src/test/resources/kafka-consumer-migration-test-flink" + flinkGenerateSavepointVersion + "-empty-state-snapshot", emptyState);
	}

	private void writeSnapshot(String path, HashMap<KafkaTopicPartition, Long> state) throws Exception {

		final OneShotLatch latch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.trigger();
				return null;
			}
		}).when(fetcher).runFetchLoop();

		when(fetcher.snapshotCurrentState()).thenReturn(state);

		final List<KafkaTopicPartition> partitions = new ArrayList<>(PARTITION_STATE.keySet());

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(fetcher, partitions);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
				new StreamSource<>(consumerFunction);


		final AbstractStreamOperatorTestHarness<String> testHarness =
				new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		testHarness.open();

		final Throwable[] error = new Throwable[1];

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					consumerFunction.run(new DummySourceContext() {
						@Override
						public void collect(String element) {

						}
					});
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
				}
			}
		};
		runner.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		final OperatorStateHandles snapshot;
		synchronized (testHarness.getCheckpointLock()) {
			snapshot = testHarness.snapshot(0L, 0L);
		}

		OperatorSnapshotUtil.writeStateHandle(snapshot, path);

		consumerOperator.close();
		runner.join();
	}

	/**
	 * Test restoring from an legacy empty state, when no partitions could be found for topics.
	 */
	@Test
	public void testRestoreFromEmptyStateNoPartitions() throws Exception {
		final DummyFlinkKafkaConsumer<String> consumerFunction =
				new DummyFlinkKafkaConsumer<>(Collections.<KafkaTopicPartition>emptyList());

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator = new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
				new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();

		// restore state from binary snapshot file
		MigrationTestUtil.restoreFromSnapshot(
			testHarness,
			OperatorSnapshotUtil.getResourceFilename(
				"kafka-consumer-migration-test-flink" + testMigrateVersion + "-empty-state-snapshot"),
			testMigrateVersion);

		testHarness.open();

		// assert that no partitions were found and is empty
		assertTrue(consumerFunction.getSubscribedPartitionsToStartOffsets() != null);
		assertTrue(consumerFunction.getSubscribedPartitionsToStartOffsets().isEmpty());

		// assert that no state was restored
		assertTrue(consumerFunction.getRestoredState().isEmpty());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	/**
	 * Test restoring from an empty state taken using a previous Flink version, when some partitions could be
	 * found for topics.
	 */
	@Test
	public void testRestoreFromEmptyStateWithPartitions() throws Exception {
		final List<KafkaTopicPartition> partitions = new ArrayList<>(PARTITION_STATE.keySet());

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(partitions);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
				new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
				new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();

		// restore state from binary snapshot file
		MigrationTestUtil.restoreFromSnapshot(
			testHarness,
			OperatorSnapshotUtil.getResourceFilename(
				"kafka-consumer-migration-test-flink" + testMigrateVersion + "-empty-state-snapshot"),
			testMigrateVersion);

		testHarness.open();

		// the expected state in "kafka-consumer-migration-test-flink*-empty-state-snapshot";
		// since the state is empty, the consumer should reflect on the startup mode to determine start offsets.
		final HashMap<KafkaTopicPartition, Long> expectedSubscribedPartitionsWithStartOffsets = new HashMap<>();
		for (KafkaTopicPartition partition : PARTITION_STATE.keySet()) {
			expectedSubscribedPartitionsWithStartOffsets.put(partition, KafkaTopicPartitionStateSentinel.GROUP_OFFSET);
		}

		// verify that we do not try to fetch the partitions list and subscribe to them even though we have empty state
		assertTrue(consumerFunction.getRestoredState().isEmpty());
		assertTrue(consumerFunction.getSubscribedPartitionsToStartOffsets().isEmpty());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	/**
	 * Test restoring from a non-empty state taken using a previous Flink version, when some partitions could be
	 * found for topics.
	 */
	@Test
	public void testRestore() throws Exception {
		final List<KafkaTopicPartition> partitions = new ArrayList<>(PARTITION_STATE.keySet());

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(partitions);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
				new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
				new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();

		// restore state from binary snapshot file
		MigrationTestUtil.restoreFromSnapshot(
			testHarness,
			OperatorSnapshotUtil.getResourceFilename(
				"kafka-consumer-migration-test-flink" + testMigrateVersion + "-snapshot"),
			testMigrateVersion);

		testHarness.open();

		// assert that there are partitions and is identical to expected list
		assertTrue(consumerFunction.getSubscribedPartitionsToStartOffsets() != null);
		assertTrue(!consumerFunction.getSubscribedPartitionsToStartOffsets().isEmpty());

		// on restore, subscribedPartitionsToStartOffsets should be identical to the restored state
		Assert.assertEquals(PARTITION_STATE, consumerFunction.getSubscribedPartitionsToStartOffsets());

		// assert that state is correctly restored from legacy checkpoint
		assertTrue(consumerFunction.getRestoredState() != null);
		Assert.assertEquals(PARTITION_STATE, consumerFunction.getRestoredState());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	// ------------------------------------------------------------------------

	private static class DummyFlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {
		private static final long serialVersionUID = 1L;

		private final List<KafkaTopicPartition> partitions;

		private final AbstractFetcher<T, ?> fetcher;

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaConsumer(AbstractFetcher<T, ?> fetcher, List<KafkaTopicPartition> partitions) {
			super(Arrays.asList("dummy-topic"), (KeyedDeserializationSchema< T >) mock(KeyedDeserializationSchema.class));
			this.fetcher = fetcher;
			this.partitions = partitions;
		}

		DummyFlinkKafkaConsumer(List<KafkaTopicPartition> partitions) {
			super(Arrays.asList("dummy-topic"), (KeyedDeserializationSchema< T >) mock(KeyedDeserializationSchema.class));
			this.fetcher = mock(AbstractFetcher.class);
			this.partitions = partitions;
		}

		@Override
		protected AbstractFetcher<T, ?> createFetcher(
				SourceContext<T> sourceContext,
				Map<KafkaTopicPartition, Long> thisSubtaskPartitionsWithStartOffsets,
				SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
				SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
				StreamingRuntimeContext runtimeContext,
				OffsetCommitMode offsetCommitMode) throws Exception {
			return fetcher;
		}

		@Override
		protected List<KafkaTopicPartition> getKafkaPartitions(List<String> topics) {
			return partitions;
		}

		@Override
		protected boolean getIsAutoCommitEnabled() {
			return false;
		}
	}


	private static abstract class DummySourceContext
			implements SourceFunction.SourceContext<String> {

		private final Object lock = new Object();

		@Override
		public void collectWithTimestamp(String element, long timestamp) {
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {
		}

		@Override
		public void markAsTemporarilyIdle() {

		}
	}
}
