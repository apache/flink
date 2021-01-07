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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarCommitCallback;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarFetcher;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;
import org.apache.flink.streaming.connectors.pulsar.testutils.TestMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.testutils.TestSourceContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.collections.MapUtils;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsIn.isIn;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Source unit tests.
 */
public class FlinkPulsarSourceTest extends TestLogger {

	private static final int maxParallelism = Short.MAX_VALUE / 2;

	@Test
	@SuppressWarnings("unchecked")
	public void testEitherWatermarkExtractor() {
		try {
			new DummyFlinkPulsarSource<String>()
				.assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<String>) null);
			fail();
		} catch (NullPointerException ignored) {
		}

		try {
			new DummyFlinkPulsarSource<String>()
				.assignTimestampsAndWatermarks((AssignerWithPunctuatedWatermarks<String>) null);
			fail();
		} catch (NullPointerException ignored) {
		}

		final AssignerWithPeriodicWatermarks<String> periodicAssigner = mock(
			AssignerWithPeriodicWatermarks.class);
		final AssignerWithPunctuatedWatermarks<String> punctuatedAssigner =
			mock(AssignerWithPunctuatedWatermarks.class);

		DummyFlinkPulsarSource<String> c1 = new DummyFlinkPulsarSource<>();
		c1.assignTimestampsAndWatermarks(periodicAssigner);
		try {
			c1.assignTimestampsAndWatermarks(punctuatedAssigner);
			fail();
		} catch (IllegalStateException ignored) {
		}

		DummyFlinkPulsarSource<String> c2 = new DummyFlinkPulsarSource<>();
		c2.assignTimestampsAndWatermarks(punctuatedAssigner);
		try {
			c2.assignTimestampsAndWatermarks(periodicAssigner);
			fail();
		} catch (IllegalStateException ignored) {
		}
	}

	/**
	 * Tests that no checkpoints happen when the fetcher is not running.
	 */
	@Test
	public void ignoreCheckpointWhenNotRunning() throws Exception {
		@SuppressWarnings("unchecked") final MockFetcher<String> fetcher = new MockFetcher<>();
		final FlinkPulsarSource<String> source = new DummyFlinkPulsarSource<>(
			fetcher,
			mock(PulsarMetadataReader.class),
			dummyProperties);

		final TestingListState<Tuple2<String, MessageId>> listState = new TestingListState<>();
		setupSource(source, false, listState, true, 0, 1);

		// snapshot before the fetcher starts running
		source.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));

		// no state should have been checkpointed
		assertFalse(listState.get().iterator().hasNext());

		// acknowledgement of the checkpoint should also not result in any offset commits
		source.notifyCheckpointComplete(1L);
		assertNull(fetcher.getAndClearLastCommittedOffsets());
		assertEquals(0, fetcher.getCommitCount());
	}

	/**
	 * Tests that when taking a checkpoint when the fetcher is not running yet,
	 * the checkpoint correctly contains the restored state instead.
	 */
	@Test
	public void checkRestoredCheckpointWhenFetcherNotReady() throws Exception {
		@SuppressWarnings("unchecked")
		final FlinkPulsarSource<String> source = new DummyFlinkPulsarSource<>();

		final TestingListState<Tuple2<String, MessageId>> restoredListState = new TestingListState<>();
		setupSource(source, true, restoredListState, true, 0, 1);

		// snapshot before the fetcher starts running
		source.snapshotState(new StateSnapshotContextSynchronousImpl(17, 17));

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
	@SuppressWarnings("unchecked")
	public void testSnapshotStateWithCommitOnCheckpoint() throws Exception {
		Map<TopicRange, MessageId> state1 = ImmutableMap.of(
			new TopicRange("abc"),
			dummyMessageId(5),
			new TopicRange("def"),
			dummyMessageId(90));
		Map<TopicRange, MessageId> state2 = ImmutableMap.of(
			new TopicRange("abc"),
			dummyMessageId(10),
			new TopicRange("def"),
			dummyMessageId(95));
		Map<TopicRange, MessageId> state3 = ImmutableMap.of(
			new TopicRange("abc"),
			dummyMessageId(15),
			new TopicRange("def"),
			dummyMessageId(100));

		MockFetcher<String> fetcher = new MockFetcher<>(state1, state2, state3);

		final FlinkPulsarSource<String> source = new DummyFlinkPulsarSource<>(
			fetcher,
			mock(PulsarMetadataReader.class),
			dummyProperties);

		final TestingListState<Serializable> listState = new TestingListState<>();
		setupSource(source, false, listState, true, 0, 1);

		final CheckedThread runThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				source.run(new TestSourceContext<>());
			}
		};
		runThread.start();
		fetcher.waitUntilRun();

		assertEquals(0, source.getPendingOffsetsToCommit().size());

		// checkpoint 1
		source.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));

		HashMap<TopicRange, MessageId> snapshot1 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple3<TopicRange, MessageId, String> tuple2 = (Tuple3<TopicRange, MessageId, String>) serializable;
			snapshot1.put(tuple2.f0, tuple2.f1);
		}

		assertEquals(state1, snapshot1);
		assertEquals(1, source.getPendingOffsetsToCommit().size());
		assertEquals(state1, source.getPendingOffsetsToCommit().get(138L));

		// checkpoint 2
		source.snapshotState(new StateSnapshotContextSynchronousImpl(140, 140));

		HashMap<TopicRange, MessageId> snapshot2 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple3<TopicRange, MessageId, String> tuple2 = (Tuple3<TopicRange, MessageId, String>) serializable;
			snapshot2.put(tuple2.f0, tuple2.f1);
		}

		assertEquals(state2, snapshot2);
		assertEquals(2, source.getPendingOffsetsToCommit().size());
		assertEquals(state2, source.getPendingOffsetsToCommit().get(140L));

		// ack checkpoint 1
		source.notifyCheckpointComplete(138L);
		assertEquals(1, source.getPendingOffsetsToCommit().size());
		assertTrue(source.getPendingOffsetsToCommit().containsKey(140L));
		assertEquals(state1, fetcher.getAndClearLastCommittedOffsets());
		assertEquals(1, fetcher.getCommitCount());

		// checkpoint 3
		source.snapshotState(new StateSnapshotContextSynchronousImpl(141, 141));

		HashMap<TopicRange, MessageId> snapshot3 = new HashMap<>();

		for (Serializable serializable : listState.get()) {
			Tuple3<TopicRange, MessageId, String> tuple2 = (Tuple3<TopicRange, MessageId, String>) serializable;
			snapshot3.put(tuple2.f0, tuple2.f1);
		}

		assertEquals(state3, snapshot3);
		assertEquals(2, source.getPendingOffsetsToCommit().size());
		assertEquals(state3, source.getPendingOffsetsToCommit().get(141L));

		// ack checkpoint 3, subsumes number 2
		source.notifyCheckpointComplete(141L);
		assertEquals(0, source.getPendingOffsetsToCommit().size());
		assertEquals(state3, fetcher.getAndClearLastCommittedOffsets());
		assertEquals(2, fetcher.getCommitCount());

		source.notifyCheckpointComplete(666); // invalid checkpoint
		assertEquals(0, source.getPendingOffsetsToCommit().size());
		assertNull(fetcher.getAndClearLastCommittedOffsets());
		assertEquals(2, fetcher.getCommitCount());

		source.cancel();
		runThread.sync();
	}

	@Test
	public void testCloseDiscovererWhenOpenThrowException() throws Exception {
		final RuntimeException failureCause =
			new RuntimeException(new FlinkException("Test partition discoverer exception"));
		final FailingPartitionDiscoverer failingPartitionDiscoverer = new FailingPartitionDiscoverer(
			failureCause);

		final DummyFlinkPulsarSource source = new DummyFlinkPulsarSource(failingPartitionDiscoverer);
		testFailingSourceLifecycle(source, failureCause);
		assertTrue(
			"partitionDiscoverer should be closed when consumer is closed",
			failingPartitionDiscoverer.isClosed());
	}

	@Test
	public void testClosePartitionDiscovererWhenCreateFetcherFails() throws Exception {
		final FlinkException failureCause = new FlinkException("create fetcher failure");
		final DummyPartitionDiscoverer testDiscoverer = new DummyPartitionDiscoverer();
		final DummyFlinkPulsarSource<String> source = new DummyFlinkPulsarSource<>(
			() -> {
				throw failureCause;
			},
			testDiscoverer,
			dummyProperties);
		testFailingSourceLifecycle(source, failureCause);
		assertTrue(
			"partitionDiscoverer should be closed when consumer is closed",
			testDiscoverer.isClosed());
	}

	@Test
	public void testClosePartitionDiscovererWhenFetcherFails() throws Exception {
		final FlinkException failureCause = new FlinkException("Run fetcher failure.");

		// in this scenario, the partition discoverer will be concurrently accessed;
		// use the WakeupBeforeCloseTestingPartitionDiscoverer to verify that we always call
		// wakeup() before closing the discoverer
		final DummyPartitionDiscoverer testDiscoverer = new DummyPartitionDiscoverer();
		final PulsarFetcher<String> mock = mock(PulsarFetcher.class);
		doThrow(failureCause).when(mock).runFetchLoop();
		final DummyFlinkPulsarSource<String> source = new DummyFlinkPulsarSource<>(
			() -> mock,
			testDiscoverer,
			dummyProperties);

		testFailingSourceLifecycle(source, failureCause);
		assertTrue(
			"partitionDiscoverer should be closed when consumer is closed",
			testDiscoverer.isClosed());
	}

	@Test
	public void testClosePartitionDiscovererWithCancellation() throws Exception {
		final DummyPartitionDiscoverer testPartitionDiscoverer = new DummyPartitionDiscoverer();

		final TestingFlinkPulsarSource<String> consumer = new TestingFlinkPulsarSource<>(
			testPartitionDiscoverer);

		testNormalConsumerLifecycle(consumer);
		assertTrue(
			"partitionDiscoverer should be closed when consumer is closed",
			testPartitionDiscoverer.isClosed());
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
	 * Tests whether the pulsar consumer behaves correctly when scaling the parallelism up/down,
	 * which means that operator state is being reshuffled.
	 *
	 * <p>This also verifies that a restoring source is always impervious to changes in the list
	 * of topics fetched from pulsar.
	 */
	@SuppressWarnings("unchecked")
	private void testRescaling(
		final int initialParallelism,
		final int numPartitions,
		final int restoredParallelism,
		final int restoredNumPartitions) throws Exception {

		Preconditions.checkArgument(
			restoredNumPartitions >= numPartitions,
			"invalid test case for Pulsar repartitioning; Pulsar only allows increasing partitions.");

		List<TopicRange> startupTopics =
			IntStream.range(0, numPartitions).mapToObj(i -> topicName("test-topic", i))
				.map(TopicRange::new)
				.collect(Collectors.toList());

		DummyFlinkPulsarSource<String>[] sources =
			new DummyFlinkPulsarSource[initialParallelism];

		AbstractStreamOperatorTestHarness<String>[] testHarnesses =
			new AbstractStreamOperatorTestHarness[initialParallelism];

		for (int i = 0; i < initialParallelism; i++) {
			TestMetadataReader discoverer = new TestMetadataReader(
				Collections.singletonMap("topic", "test-topic"),
				i,
				initialParallelism,
				TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(Sets.newHashSet(
					startupTopics)));

			sources[i] = new DummyFlinkPulsarSource(discoverer);
			testHarnesses[i] = createTestHarness(sources[i], initialParallelism, i);
			testHarnesses[i].initializeEmptyState();
			testHarnesses[i].open();
		}

		Map<TopicRange, MessageId> globalSubscribedPartitions = new HashMap<>();

		for (int i = 0; i < initialParallelism; i++) {
			Map<TopicRange, MessageId> subscribedPartitions = sources[i].getOwnedTopicStarts();

			for (TopicRange topic : subscribedPartitions.keySet()) {
				assertThat(globalSubscribedPartitions, not(hasKey(topic)));
			}
			globalSubscribedPartitions.putAll(subscribedPartitions);
		}

		assertThat(globalSubscribedPartitions.values(), hasSize(numPartitions));
		assertThat(startupTopics, everyItem(isIn(globalSubscribedPartitions.keySet())));

		OperatorSubtaskState[] state = new OperatorSubtaskState[initialParallelism];

		for (int i = 0; i < initialParallelism; i++) {
			state[i] = testHarnesses[i].snapshot(0, 0);
		}

		OperatorSubtaskState mergedState = AbstractStreamOperatorTestHarness.repackageState(state);

		// restore

		List<TopicRange> restoredTopics = new ArrayList<>();
		for (int i = 0; i < restoredNumPartitions; i++) {
			restoredTopics.add(new TopicRange(topicName("testTopic", i)));
		}

		DummyFlinkPulsarSource<String>[] restoredConsumers =
			new DummyFlinkPulsarSource[restoredParallelism];

		AbstractStreamOperatorTestHarness<String>[] restoredTestHarnesses =
			new AbstractStreamOperatorTestHarness[restoredParallelism];

		for (int i = 0; i < restoredParallelism; i++) {
			OperatorSubtaskState initState = AbstractStreamOperatorTestHarness.repartitionOperatorState(
				mergedState, maxParallelism, initialParallelism, restoredParallelism, i);

			TestMetadataReader discoverer = new TestMetadataReader(
				Collections.singletonMap("topic", "test-topic"),
				i,
				restoredParallelism,
				TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(Sets.newHashSet(
					restoredTopics)));

			restoredConsumers[i] = new DummyFlinkPulsarSource<>(discoverer);
			restoredTestHarnesses[i] = createTestHarness(
				restoredConsumers[i],
				restoredParallelism,
				i);

			// initializeState() is always called, null signals that we didn't restore
			restoredTestHarnesses[i].initializeState(initState);
			restoredTestHarnesses[i].open();
		}

		Map<TopicRange, MessageId> restoredGlobalSubscribedPartitions = new HashMap<>();

		for (int i = 0; i < restoredParallelism; i++) {
			Map<TopicRange, MessageId> subscribedPartitions =
				restoredConsumers[i].getOwnedTopicStarts();

			// make sure that no one else is subscribed to these partitions
			for (TopicRange partition : subscribedPartitions.keySet()) {
				assertThat(restoredGlobalSubscribedPartitions, not(hasKey(partition)));
			}
			restoredGlobalSubscribedPartitions.putAll(subscribedPartitions);
		}

		assertThat(restoredGlobalSubscribedPartitions.values(), hasSize(restoredNumPartitions));
		assertThat(restoredTopics, everyItem(isIn(restoredGlobalSubscribedPartitions.keySet())));

	}

	private void testFailingSourceLifecycle(
		FlinkPulsarSource<String> source,
		Exception e) throws Exception {
		try {
			setupSource(source);
			source.run(new TestSourceContext());

			fail("Exception should have been thrown from open / run method of FlinkPulsarSource.");
		} catch (Exception exc) {
			assertThat(ExceptionUtils
				.findThrowable(e, throwable -> throwable.equals(e))
				.isPresent(), is(true));
		}
		source.close();
	}

	private void testNormalConsumerLifecycle(FlinkPulsarSource<String> source) throws Exception {
		setupSource(source);
		final CompletableFuture<Void> runFuture =
			CompletableFuture.runAsync(ThrowingRunnable.unchecked(() -> source.run(new TestSourceContext<>())));
		source.close();
		runFuture.get();
	}

	private static <T> AbstractStreamOperatorTestHarness<T> createTestHarness(
		SourceFunction<T> source, int numSubtasks, int subtaskIndex) throws Exception {

		AbstractStreamOperatorTestHarness<T> testHarness =
			new AbstractStreamOperatorTestHarness<>(
				new StreamSource<>(source), maxParallelism, numSubtasks, subtaskIndex);

		testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

		return testHarness;
	}

	private static class FailingPartitionDiscoverer extends PulsarMetadataReader {
		private volatile boolean closed = false;

		private final RuntimeException failureCause;

		public FailingPartitionDiscoverer(RuntimeException failureCause) throws PulsarClientException {
			super("http://localhost:8080",
				new ClientConfigurationData(),
				"",
				Collections.singletonMap("topic", "foo"),
				0,
				1);
			this.failureCause = failureCause;
		}

		@Override
		public Set<TopicRange> getTopicPartitionsAll() throws PulsarAdminException {
			return null;
		}

		public boolean isClosed() {
			return closed;
		}

		@Override
		public Set<TopicRange> discoverTopicChanges() throws PulsarAdminException, ClosedException {
			throw failureCause;
		}

		@Override
		public void close() {
			closed = true;
		}
	}

	private static class DummyPartitionDiscoverer extends PulsarMetadataReader {
		private volatile boolean closed = false;

		private static Set<TopicRange> allPartitions = Sets.newHashSet(new TopicRange("foo"));

		public DummyPartitionDiscoverer() throws PulsarClientException {
			super("http://localhost:8080",
				new ClientConfigurationData(),
				"",
				Collections.singletonMap("topic", "foo"),
				0,
				1);
		}

		@Override
		public Set<TopicRange> getTopicPartitionsAll() throws PulsarAdminException {
			try {
				checkState();
			} catch (ClosedException e) {
				throw new RuntimeException(e);
			}
			return allPartitions;
		}

		public void checkState() throws ClosedException {
			if (closed) {
				throw new ClosedException();
			}
		}

		public boolean isClosed() {
			return closed;
		}

		@Override
		public void close() {
			closed = true;
		}
	}

	private static <T, S> void setupSource(FlinkPulsarSource<T> source) throws Exception {
		setupSource(source, false, null, false, 0, 1);
	}

	private static <T, S> void setupSource(
		FlinkPulsarSource<T> source, boolean isRestored,
		ListState<S> restoredListState, boolean isCheckpointEnabled,
		int subtaskIndex, int totalNumberSubtasks) throws Exception {
		source.setRuntimeContext(
			new MockStreamingRuntimeContext(
				isCheckpointEnabled,
				totalNumberSubtasks,
				subtaskIndex));
		source.initializeState(
			new MockFunctionInitializationContext(
				isRestored,
				new MockOperatorStateStore(restoredListState)));
		source.open(new Configuration());
	}

	public static Properties dummyProperties = MapUtils.toProperties(Collections.singletonMap(
		"topic",
		"c"));

	private static String topicName(String topic, int partition) {
		return TopicName.get(topic).getPartition(partition).toString();
	}

	private static MessageId dummyMessageId(int i) {
		return new MessageIdImpl(5, i, -1);
	}

	private static class TestingFlinkPulsarSource<T> extends FlinkPulsarSource<T> {

		final PulsarMetadataReader discoverer;

		public TestingFlinkPulsarSource(PulsarMetadataReader discoverer) {
			super("", "", (PulsarDeserializationSchema<T>) null, dummyProperties);
			this.discoverer = discoverer;
		}

		@Override
		protected PulsarFetcher<T> createFetcher(
			SourceContext<T> sourceContext,
			Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			StreamingRuntimeContext streamingRuntime,
			boolean useMetrics) throws Exception {
			return new TestingFetcher<>(sourceContext,
				seedTopicsWithInitialOffsets,
				watermarkStrategy,
				processingTimeProvider,
				autoWatermarkInterval);
		}

		@Override
		protected PulsarMetadataReader createMetadataReader() throws PulsarClientException {
			return discoverer;
		}
	}

	private static class DummyFlinkPulsarSource<T> extends FlinkPulsarSource<T> {
		private static final long serialVersionUID = 1L;

		private SupplierWithException<PulsarFetcher<T>, Exception> testFetcherSupplier;
		private PulsarMetadataReader discoverer;

		public DummyFlinkPulsarSource(
			SupplierWithException<PulsarFetcher<T>, Exception> testFetcherSupplier,
			PulsarMetadataReader discoverer,
			Properties properties) {
			super("a", "b", mock(PulsarDeserializationSchema.class), properties);
			this.testFetcherSupplier = testFetcherSupplier;
			this.discoverer = discoverer;
		}

		public DummyFlinkPulsarSource(
			PulsarFetcher<T> testFetcher,
			PulsarMetadataReader discoverer,
			Properties properties) {
			this(() -> testFetcher, discoverer, properties);
		}

		public DummyFlinkPulsarSource(PulsarMetadataReader metadataReader) {
			this(() -> mock(PulsarFetcher.class), metadataReader, dummyProperties);
		}

		public DummyFlinkPulsarSource() {
			this(mock(PulsarMetadataReader.class));
		}

		@Override
		protected PulsarFetcher<T> createFetcher(
			SourceContext<T> sourceContext,
			Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			StreamingRuntimeContext streamingRuntime,
			boolean useMetrics) throws Exception {
			return testFetcherSupplier.get();
		}

		@Override
		protected PulsarMetadataReader createMetadataReader() throws PulsarClientException {
			return discoverer;
		}
	}

	private static final class TestingFetcher<T> extends PulsarFetcher<T> {

		private volatile boolean isRunning = true;

		public TestingFetcher(
			SourceFunction.SourceContext<T> sourceContext,
			Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval) throws Exception {
			super(
				sourceContext,
				seedTopicsWithInitialOffsets,
				watermarkStrategy,
				processingTimeProvider,
				autoWatermarkInterval,
				TestingFetcher.class.getClassLoader(),
				null,
				null,
				null,
				0,
				null,
				null,
				new UnregisteredMetricsGroup(),
				false);
		}

		@Override
		public void runFetchLoop() throws Exception {
			while (isRunning) {
				Thread.sleep(10L);
			}
		}

		@Override
		public void cancel() throws Exception {
			isRunning = false;
		}

		@Override
		public void doCommitOffsetToPulsar(
			Map<TopicRange, MessageId> offset,
			PulsarCommitCallback offsetCommitCallback) {
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
			Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
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

			addAll(values);
		}

		@Override
		public void addAll(List<T> values) throws Exception {
			if (values != null) {
				values.forEach(v -> Preconditions.checkNotNull(
					v,
					"You cannot add null to a ListState."));

				list.addAll(values);
			}
		}
	}

	private static class MockFetcher<T> extends PulsarFetcher<T> {

		private final OneShotLatch runLatch = new OneShotLatch();
		private final OneShotLatch stopLatch = new OneShotLatch();

		private final ArrayDeque<Map<TopicRange, MessageId>> stateSnapshotsToReturn = new ArrayDeque<>();

		private Map<TopicRange, MessageId> lastCommittedOffsets;
		private int commitCount = 0;

		private MockFetcher(Map<TopicRange, MessageId>... stateSnapshotsToReturn) throws Exception {
			super(
				new TestSourceContext<>(),
				new HashMap<>(),
				null,
				new TestProcessingTimeService(),
				0,
				MockFetcher.class.getClassLoader(),
				null,
				null,
				null,
				0,
				null,
				null,
				new UnregisteredMetricsGroup(),
				false);

			this.stateSnapshotsToReturn.addAll(Arrays.asList(stateSnapshotsToReturn));
		}

		@Override
		public void doCommitOffsetToPulsar(
			Map<TopicRange, MessageId> offset,
			PulsarCommitCallback offsetCommitCallback) {
			this.lastCommittedOffsets = offset;
			this.commitCount++;
			offsetCommitCallback.onSuccess();
		}

		@Override
		public void runFetchLoop() throws Exception {
			runLatch.trigger();
			stopLatch.await();
		}

		@Override
		public Map<TopicRange, MessageId> snapshotCurrentState() {
			Preconditions.checkState(!stateSnapshotsToReturn.isEmpty());
			return stateSnapshotsToReturn.poll();
		}

		@Override
		public void cancel() throws Exception {
			stopLatch.trigger();
		}

		public void waitUntilRun() throws InterruptedException {
			runLatch.await();
		}

		public Map<TopicRange, MessageId> getAndClearLastCommittedOffsets() {
			Map<TopicRange, MessageId> off = this.lastCommittedOffsets;
			this.lastCommittedOffsets = null;
			return off;
		}

		public int getCommitCount() {
			return commitCount;
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

		public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
			return new TestingListState<>();
		}

		// ------------------------------------------------------------------------

		public <S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)
			throws Exception {
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

		@Override
		public Set<String> getRegisteredBroadcastStateNames() {
			throw new UnsupportedOperationException();
		}
	}

	private static class MockFunctionInitializationContext implements FunctionInitializationContext {

		private final boolean isRestored;
		private final OperatorStateStore operatorStateStore;

		private MockFunctionInitializationContext(
			boolean isRestored,
			OperatorStateStore operatorStateStore) {
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
