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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.testutils.TestSourceContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link AbstractFetcher}.
 */
@SuppressWarnings("serial")
public class AbstractFetcherTest {

	@Test
	public void testIgnorePartitionStateSentinelInSnapshot() throws Exception {
		final String testTopic = "test topic name";
		Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
		originalPartitions.put(new KafkaTopicPartition(testTopic, 1), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);
		originalPartitions.put(new KafkaTopicPartition(testTopic, 2), KafkaTopicPartitionStateSentinel.GROUP_OFFSET);
		originalPartitions.put(new KafkaTopicPartition(testTopic, 3), KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET);

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestFetcher<Long> fetcher = new TestFetcher<>(
			sourceContext,
			originalPartitions,
			null,
			null,
			new TestProcessingTimeService(),
			0);

		synchronized (sourceContext.getCheckpointLock()) {
			HashMap<KafkaTopicPartition, Long> currentState = fetcher.snapshotCurrentState();
			fetcher.commitInternalOffsetsToKafka(currentState, new KafkaCommitCallback() {
				@Override
				public void onSuccess() {
				}

				@Override
				public void onException(Throwable cause) {
					throw new RuntimeException("Callback failed", cause);
				}
			});

			assertTrue(fetcher.getLastCommittedOffsets().isPresent());
			assertEquals(Collections.emptyMap(), fetcher.getLastCommittedOffsets().get());
		}
	}

	// ------------------------------------------------------------------------
	//   Record emitting tests
	// ------------------------------------------------------------------------

	@Test
	public void testSkipCorruptedRecord() throws Exception {
		final String testTopic = "test topic name";
		Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
		originalPartitions.put(new KafkaTopicPartition(testTopic, 1), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestFetcher<Long> fetcher = new TestFetcher<>(
			sourceContext,
			originalPartitions,
			null, /* periodic watermark assigner */
			null, /* punctuated watermark assigner */
			new TestProcessingTimeService(),
			0);

		final KafkaTopicPartitionState<Object> partitionStateHolder = fetcher.subscribedPartitionStates().get(0);

		fetcher.emitRecord(1L, partitionStateHolder, 1L);
		fetcher.emitRecord(2L, partitionStateHolder, 2L);
		assertEquals(2L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(2L, partitionStateHolder.getOffset());

		// emit null record
		fetcher.emitRecord(null, partitionStateHolder, 3L);
		assertEquals(2L, sourceContext.getLatestElement().getValue().longValue()); // the null record should be skipped
		assertEquals(3L, partitionStateHolder.getOffset()); // the offset in state still should have advanced
	}

	@Test
	public void testSkipCorruptedRecordWithPunctuatedWatermarks() throws Exception {
		final String testTopic = "test topic name";
		Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
		originalPartitions.put(new KafkaTopicPartition(testTopic, 1), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestProcessingTimeService processingTimeProvider = new TestProcessingTimeService();

		TestFetcher<Long> fetcher = new TestFetcher<>(
			sourceContext,
			originalPartitions,
			null, /* periodic watermark assigner */
			new SerializedValue<AssignerWithPunctuatedWatermarks<Long>>(new PunctuatedTestExtractor()), /* punctuated watermark assigner */
			processingTimeProvider,
			0);

		final KafkaTopicPartitionState<Object> partitionStateHolder = fetcher.subscribedPartitionStates().get(0);

		// elements generate a watermark if the timestamp is a multiple of three
		fetcher.emitRecord(1L, partitionStateHolder, 1L);
		fetcher.emitRecord(2L, partitionStateHolder, 2L);
		fetcher.emitRecord(3L, partitionStateHolder, 3L);
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
		assertTrue(sourceContext.hasWatermark());
		assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());
		assertEquals(3L, partitionStateHolder.getOffset());

		// emit null record
		fetcher.emitRecord(null, partitionStateHolder, 4L);

		// no elements or watermarks should have been collected
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
		assertFalse(sourceContext.hasWatermark());
		// the offset in state still should have advanced
		assertEquals(4L, partitionStateHolder.getOffset());
	}

	@Test
	public void testSkipCorruptedRecordWithPeriodicWatermarks() throws Exception {
		final String testTopic = "test topic name";
		Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
		originalPartitions.put(new KafkaTopicPartition(testTopic, 1), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestProcessingTimeService processingTimeProvider = new TestProcessingTimeService();

		TestFetcher<Long> fetcher = new TestFetcher<>(
			sourceContext,
			originalPartitions,
			new SerializedValue<AssignerWithPeriodicWatermarks<Long>>(new PeriodicTestExtractor()), /* periodic watermark assigner */
			null, /* punctuated watermark assigner */
			processingTimeProvider,
			10);

		final KafkaTopicPartitionState<Object> partitionStateHolder = fetcher.subscribedPartitionStates().get(0);

		// elements generate a watermark if the timestamp is a multiple of three
		fetcher.emitRecord(1L, partitionStateHolder, 1L);
		fetcher.emitRecord(2L, partitionStateHolder, 2L);
		fetcher.emitRecord(3L, partitionStateHolder, 3L);
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
		assertEquals(3L, partitionStateHolder.getOffset());

		// advance timer for watermark emitting
		processingTimeProvider.setCurrentTime(10L);
		assertTrue(sourceContext.hasWatermark());
		assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());

		// emit null record
		fetcher.emitRecord(null, partitionStateHolder, 4L);

		// no elements should have been collected
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
		// the offset in state still should have advanced
		assertEquals(4L, partitionStateHolder.getOffset());

		// no watermarks should be collected
		processingTimeProvider.setCurrentTime(20L);
		assertFalse(sourceContext.hasWatermark());
	}

	// ------------------------------------------------------------------------
	//   Timestamps & watermarks tests
	// ------------------------------------------------------------------------

	@Test
	public void testPunctuatedWatermarks() throws Exception {
		final String testTopic = "test topic name";
		Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
		originalPartitions.put(new KafkaTopicPartition(testTopic, 7), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);
		originalPartitions.put(new KafkaTopicPartition(testTopic, 13), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);
		originalPartitions.put(new KafkaTopicPartition(testTopic, 21), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestProcessingTimeService processingTimeProvider = new TestProcessingTimeService();

		TestFetcher<Long> fetcher = new TestFetcher<>(
				sourceContext,
				originalPartitions,
				null, /* periodic watermark assigner */
				new SerializedValue<AssignerWithPunctuatedWatermarks<Long>>(new PunctuatedTestExtractor()),
				processingTimeProvider,
				0);

		final KafkaTopicPartitionState<Object> part1 = fetcher.subscribedPartitionStates().get(0);
		final KafkaTopicPartitionState<Object> part2 = fetcher.subscribedPartitionStates().get(1);
		final KafkaTopicPartitionState<Object> part3 = fetcher.subscribedPartitionStates().get(2);

		// elements generate a watermark if the timestamp is a multiple of three

		// elements for partition 1
		fetcher.emitRecord(1L, part1, 1L);
		fetcher.emitRecord(2L, part1, 2L);
		fetcher.emitRecord(3L, part1, 3L);
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
		assertFalse(sourceContext.hasWatermark());

		// elements for partition 2
		fetcher.emitRecord(12L, part2, 1L);
		assertEquals(12L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(12L, sourceContext.getLatestElement().getTimestamp());
		assertFalse(sourceContext.hasWatermark());

		// elements for partition 3
		fetcher.emitRecord(101L, part3, 1L);
		fetcher.emitRecord(102L, part3, 2L);
		assertEquals(102L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(102L, sourceContext.getLatestElement().getTimestamp());

		// now, we should have a watermark
		assertTrue(sourceContext.hasWatermark());
		assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());

		// advance partition 3
		fetcher.emitRecord(1003L, part3, 3L);
		fetcher.emitRecord(1004L, part3, 4L);
		fetcher.emitRecord(1005L, part3, 5L);
		assertEquals(1005L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(1005L, sourceContext.getLatestElement().getTimestamp());

		// advance partition 1 beyond partition 2 - this bumps the watermark
		fetcher.emitRecord(30L, part1, 4L);
		assertEquals(30L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(30L, sourceContext.getLatestElement().getTimestamp());
		assertTrue(sourceContext.hasWatermark());
		assertEquals(12L, sourceContext.getLatestWatermark().getTimestamp());

		// advance partition 2 again - this bumps the watermark
		fetcher.emitRecord(13L, part2, 2L);
		assertFalse(sourceContext.hasWatermark());
		fetcher.emitRecord(14L, part2, 3L);
		assertFalse(sourceContext.hasWatermark());
		fetcher.emitRecord(15L, part2, 3L);
		assertTrue(sourceContext.hasWatermark());
		assertEquals(15L, sourceContext.getLatestWatermark().getTimestamp());
	}

	@Test
	public void testPeriodicWatermarks() throws Exception {
		final String testTopic = "test topic name";
		Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
		originalPartitions.put(new KafkaTopicPartition(testTopic, 7), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);
		originalPartitions.put(new KafkaTopicPartition(testTopic, 13), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);
		originalPartitions.put(new KafkaTopicPartition(testTopic, 21), KafkaTopicPartitionStateSentinel.LATEST_OFFSET);

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

		TestFetcher<Long> fetcher = new TestFetcher<>(
				sourceContext,
				originalPartitions,
				new SerializedValue<AssignerWithPeriodicWatermarks<Long>>(new PeriodicTestExtractor()),
				null, /* punctuated watermarks assigner*/
				processingTimeService,
				10);

		final KafkaTopicPartitionState<Object> part1 = fetcher.subscribedPartitionStates().get(0);
		final KafkaTopicPartitionState<Object> part2 = fetcher.subscribedPartitionStates().get(1);
		final KafkaTopicPartitionState<Object> part3 = fetcher.subscribedPartitionStates().get(2);

		// elements generate a watermark if the timestamp is a multiple of three

		// elements for partition 1
		fetcher.emitRecord(1L, part1, 1L);
		fetcher.emitRecord(2L, part1, 2L);
		fetcher.emitRecord(3L, part1, 3L);
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());

		// elements for partition 2
		fetcher.emitRecord(12L, part2, 1L);
		assertEquals(12L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(12L, sourceContext.getLatestElement().getTimestamp());

		// elements for partition 3
		fetcher.emitRecord(101L, part3, 1L);
		fetcher.emitRecord(102L, part3, 2L);
		assertEquals(102L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(102L, sourceContext.getLatestElement().getTimestamp());

		processingTimeService.setCurrentTime(10);

		// now, we should have a watermark (this blocks until the periodic thread emitted the watermark)
		assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());

		// advance partition 3
		fetcher.emitRecord(1003L, part3, 3L);
		fetcher.emitRecord(1004L, part3, 4L);
		fetcher.emitRecord(1005L, part3, 5L);
		assertEquals(1005L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(1005L, sourceContext.getLatestElement().getTimestamp());

		// advance partition 1 beyond partition 2 - this bumps the watermark
		fetcher.emitRecord(30L, part1, 4L);
		assertEquals(30L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(30L, sourceContext.getLatestElement().getTimestamp());

		processingTimeService.setCurrentTime(20);

		// this blocks until the periodic thread emitted the watermark
		assertEquals(12L, sourceContext.getLatestWatermark().getTimestamp());

		// advance partition 2 again - this bumps the watermark
		fetcher.emitRecord(13L, part2, 2L);
		fetcher.emitRecord(14L, part2, 3L);
		fetcher.emitRecord(15L, part2, 3L);

		processingTimeService.setCurrentTime(30);
		// this blocks until the periodic thread emitted the watermark
		long watermarkTs = sourceContext.getLatestWatermark().getTimestamp();
		assertTrue(watermarkTs >= 13L && watermarkTs <= 15L);
	}

	@Test
	public void testPeriodicWatermarksWithNoSubscribedPartitionsShouldYieldNoWatermarks() throws Exception {
		final String testTopic = "test topic name";
		Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestProcessingTimeService processingTimeProvider = new TestProcessingTimeService();

		TestFetcher<Long> fetcher = new TestFetcher<>(
			sourceContext,
			originalPartitions,
			new SerializedValue<AssignerWithPeriodicWatermarks<Long>>(new PeriodicTestExtractor()),
			null, /* punctuated watermarks assigner*/
			processingTimeProvider,
			10);

		processingTimeProvider.setCurrentTime(10);
		// no partitions; when the periodic watermark emitter fires, no watermark should be emitted
		assertFalse(sourceContext.hasWatermark());

		// counter-test that when the fetcher does actually have partitions,
		// when the periodic watermark emitter fires again, a watermark really is emitted
		fetcher.addDiscoveredPartitions(Collections.singletonList(new KafkaTopicPartition(testTopic, 0)));
		fetcher.emitRecord(100L, fetcher.subscribedPartitionStates().get(0), 3L);
		processingTimeProvider.setCurrentTime(20);
		assertEquals(100, sourceContext.getLatestWatermark().getTimestamp());
	}

	@Test
	public void testConcurrentPartitionsDiscoveryAndLoopFetching() throws Exception {
		// test data
		final KafkaTopicPartition testPartition = new KafkaTopicPartition("test", 42);

		// ----- create the test fetcher -----

		@SuppressWarnings("unchecked")
		SourceContext<String> sourceContext = new TestSourceContext<>();
		Map<KafkaTopicPartition, Long> partitionsWithInitialOffsets =
			Collections.singletonMap(testPartition, KafkaTopicPartitionStateSentinel.GROUP_OFFSET);

		final OneShotLatch fetchLoopWaitLatch = new OneShotLatch();
		final OneShotLatch stateIterationBlockLatch = new OneShotLatch();

		final TestFetcher<String> fetcher = new TestFetcher<>(
			sourceContext,
			partitionsWithInitialOffsets,
			null, /* periodic assigner */
			null, /* punctuated assigner */
			new TestProcessingTimeService(),
			10,
			fetchLoopWaitLatch,
			stateIterationBlockLatch);

		// ----- run the fetcher -----

		final CheckedThread checkedThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				fetcher.runFetchLoop();
			}
		};
		checkedThread.start();

		// wait until state iteration begins before adding discovered partitions
		fetchLoopWaitLatch.await();
		fetcher.addDiscoveredPartitions(Collections.singletonList(testPartition));

		stateIterationBlockLatch.trigger();
		checkedThread.sync();
	}

	// ------------------------------------------------------------------------
	//  Test mocks
	// ------------------------------------------------------------------------

	private static final class TestFetcher<T> extends AbstractFetcher<T, Object> {
		Optional<Map<KafkaTopicPartition, Long>> lastCommittedOffsets = Optional.empty();

		private final OneShotLatch fetchLoopWaitLatch;
		private final OneShotLatch stateIterationBlockLatch;

		TestFetcher(
				SourceContext<T> sourceContext,
				Map<KafkaTopicPartition, Long> assignedPartitionsWithStartOffsets,
				SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
				SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
				ProcessingTimeService processingTimeProvider,
				long autoWatermarkInterval) throws Exception {

			this(
				sourceContext,
				assignedPartitionsWithStartOffsets,
				watermarksPeriodic,
				watermarksPunctuated,
				processingTimeProvider,
				autoWatermarkInterval,
				null,
				null);
		}

		TestFetcher(
				SourceContext<T> sourceContext,
				Map<KafkaTopicPartition, Long> assignedPartitionsWithStartOffsets,
				SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
				SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
				ProcessingTimeService processingTimeProvider,
				long autoWatermarkInterval,
				OneShotLatch fetchLoopWaitLatch,
				OneShotLatch stateIterationBlockLatch) throws Exception {

			super(
				sourceContext,
				assignedPartitionsWithStartOffsets,
				watermarksPeriodic,
				watermarksPunctuated,
				processingTimeProvider,
				autoWatermarkInterval,
				TestFetcher.class.getClassLoader(),
				new UnregisteredMetricsGroup(),
				false);

			this.fetchLoopWaitLatch = fetchLoopWaitLatch;
			this.stateIterationBlockLatch = stateIterationBlockLatch;
		}

		/**
		 * Emulation of partition's iteration which is required for
		 * {@link AbstractFetcherTest#testConcurrentPartitionsDiscoveryAndLoopFetching}.
		 */
		@Override
		public void runFetchLoop() throws Exception {
			if (fetchLoopWaitLatch != null) {
				for (KafkaTopicPartitionState ignored : subscribedPartitionStates()) {
					fetchLoopWaitLatch.trigger();
					stateIterationBlockLatch.await();
				}
			} else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public void cancel() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object createKafkaPartitionHandle(KafkaTopicPartition partition) {
			return new Object();
		}

		@Override
		protected void doCommitInternalOffsetsToKafka(
				Map<KafkaTopicPartition, Long> offsets,
				@Nonnull KafkaCommitCallback callback) throws Exception {
			lastCommittedOffsets = Optional.of(offsets);
			callback.onSuccess();
		}

		public Optional<Map<KafkaTopicPartition, Long>> getLastCommittedOffsets() {
			return lastCommittedOffsets;
		}
	}

	// ------------------------------------------------------------------------

	private static class PeriodicTestExtractor implements AssignerWithPeriodicWatermarks<Long> {

		private volatile long maxTimestamp = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(Long element, long previousElementTimestamp) {
			maxTimestamp = Math.max(maxTimestamp, element);
			return element;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(maxTimestamp);
		}
	}

	private static class PunctuatedTestExtractor implements AssignerWithPunctuatedWatermarks<Long> {

		@Override
		public long extractTimestamp(Long element, long previousElementTimestamp) {
			return element;
		}

		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
			return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
		}

	}
}
