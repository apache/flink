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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.testutils.MockRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class AbstractFetcherTimestampsTest {
	
	@Test
	public void testPunctuatedWatermarks() throws Exception {
		final String testTopic = "test topic name";
		List<KafkaTopicPartition> originalPartitions = Arrays.asList(
				new KafkaTopicPartition(testTopic, 7),
				new KafkaTopicPartition(testTopic, 13),
				new KafkaTopicPartition(testTopic, 21));

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestFetcher<Long> fetcher = new TestFetcher<>(
				sourceContext, originalPartitions, null,
				new SerializedValue<AssignerWithPunctuatedWatermarks<Long>>(new PunctuatedTestExtractor()),
				new MockRuntimeContext(17, 3));

		final KafkaTopicPartitionState<Object> part1 = fetcher.subscribedPartitions()[0];
		final KafkaTopicPartitionState<Object> part2 = fetcher.subscribedPartitions()[1];
		final KafkaTopicPartitionState<Object> part3 = fetcher.subscribedPartitions()[2];

		// elements generate a watermark if the timestamp is a multiple of three
		
		// elements for partition 1
		fetcher.emitRecord(1L, part1, 1L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 1L));
		fetcher.emitRecord(2L, part1, 2L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 2L));
		fetcher.emitRecord(3L, part1, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 3L));
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());
		assertFalse(sourceContext.hasWatermark());

		// elements for partition 2
		fetcher.emitRecord(12L, part2, 1L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 1L));
		assertEquals(12L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(12L, sourceContext.getLatestElement().getTimestamp());
		assertFalse(sourceContext.hasWatermark());

		// elements for partition 3
		fetcher.emitRecord(101L, part3, 1L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 1L));
		fetcher.emitRecord(102L, part3, 2L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 2L));
		assertEquals(102L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(102L, sourceContext.getLatestElement().getTimestamp());
		
		// now, we should have a watermark
		assertTrue(sourceContext.hasWatermark());
		assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());
		
		// advance partition 3
		fetcher.emitRecord(1003L, part3, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 3L));
		fetcher.emitRecord(1004L, part3, 4L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 4L));
		fetcher.emitRecord(1005L, part3, 5L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 5L));
		assertEquals(1005L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(1005L, sourceContext.getLatestElement().getTimestamp());

		// advance partition 1 beyond partition 2 - this bumps the watermark
		fetcher.emitRecord(30L, part1, 4L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 4L));
		assertEquals(30L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(30L, sourceContext.getLatestElement().getTimestamp());
		assertTrue(sourceContext.hasWatermark());
		assertEquals(12L, sourceContext.getLatestWatermark().getTimestamp());

		// advance partition 2 again - this bumps the watermark
		fetcher.emitRecord(13L, part2, 2L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 2L));
		assertFalse(sourceContext.hasWatermark());
		fetcher.emitRecord(14L, part2, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 3L));
		assertFalse(sourceContext.hasWatermark());
		fetcher.emitRecord(15L, part2, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 3L));
		assertTrue(sourceContext.hasWatermark());
		assertEquals(15L, sourceContext.getLatestWatermark().getTimestamp());
	}
	
	@Test
	public void testPeriodicWatermarks() throws Exception {
		ExecutionConfig config = new ExecutionConfig();
		config.setAutoWatermarkInterval(10);

		final String testTopic = "test topic name";
		List<KafkaTopicPartition> originalPartitions = Arrays.asList(
				new KafkaTopicPartition(testTopic, 7),
				new KafkaTopicPartition(testTopic, 13),
				new KafkaTopicPartition(testTopic, 21));

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();

		TestFetcher<Long> fetcher = new TestFetcher<>(
				sourceContext, originalPartitions,
				new SerializedValue<AssignerWithPeriodicWatermarks<Long>>(new PeriodicTestExtractor()),
				null, new MockRuntimeContext(17, 3, config, sourceContext.getCheckpointLock()));

		final KafkaTopicPartitionState<Object> part1 = fetcher.subscribedPartitions()[0];
		final KafkaTopicPartitionState<Object> part2 = fetcher.subscribedPartitions()[1];
		final KafkaTopicPartitionState<Object> part3 = fetcher.subscribedPartitions()[2];

		// elements generate a watermark if the timestamp is a multiple of three

		// elements for partition 1
		fetcher.emitRecord(1L, part1, 1L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 1L));
		fetcher.emitRecord(2L, part1, 2L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 2L));
		fetcher.emitRecord(3L, part1, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 3L));
		assertEquals(3L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(3L, sourceContext.getLatestElement().getTimestamp());

		// elements for partition 2
		fetcher.emitRecord(12L, part2, 1L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 1L));
		assertEquals(12L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(12L, sourceContext.getLatestElement().getTimestamp());

		// elements for partition 3
		fetcher.emitRecord(101L, part3, 1L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 1L));
		fetcher.emitRecord(102L, part3, 2L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 2L));
		assertEquals(102L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(102L, sourceContext.getLatestElement().getTimestamp());

		// now, we should have a watermark (this blocks until the periodic thread emitted the watermark)
		assertEquals(3L, sourceContext.getLatestWatermark().getTimestamp());

		// advance partition 3
		fetcher.emitRecord(1003L, part3, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 3L));
		fetcher.emitRecord(1004L, part3, 4L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 4L));
		fetcher.emitRecord(1005L, part3, 5L, new ConsumerRecord<byte[], byte[]>(testTopic, 21, new byte[]{0}, 5L));
		assertEquals(1005L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(1005L, sourceContext.getLatestElement().getTimestamp());

		// advance partition 1 beyond partition 2 - this bumps the watermark
		fetcher.emitRecord(30L, part1, 4L, new ConsumerRecord<byte[], byte[]>(testTopic, 7, new byte[]{0}, 4L));
		assertEquals(30L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(30L, sourceContext.getLatestElement().getTimestamp());
		
		// this blocks until the periodic thread emitted the watermark
		assertEquals(12L, sourceContext.getLatestWatermark().getTimestamp());

		// advance partition 2 again - this bumps the watermark
		fetcher.emitRecord(13L, part2, 2L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 2L));
		fetcher.emitRecord(14L, part2, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 3L));
		fetcher.emitRecord(15L, part2, 3L, new ConsumerRecord<byte[], byte[]>(testTopic, 13, new byte[]{0}, 3L));

		// this blocks until the periodic thread emitted the watermark
		long watermarkTs = sourceContext.getLatestWatermark().getTimestamp();
		assertTrue(watermarkTs >= 13L && watermarkTs <= 15L);
	}

	// ------------------------------------------------------------------------
	//  Test mocks
	// ------------------------------------------------------------------------

	private static final class TestFetcher<T> extends AbstractFetcher<T, Object> {

		protected TestFetcher(
				SourceContext<T> sourceContext,
				List<KafkaTopicPartition> assignedPartitions,
				SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
				SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
				StreamingRuntimeContext runtimeContext) throws Exception
		{
			super(sourceContext, assignedPartitions, watermarksPeriodic, watermarksPunctuated, runtimeContext, false);
		}

		@Override
		public void runFetchLoop() throws Exception {
			throw new UnsupportedOperationException();
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
		public void commitSpecificOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets) throws Exception {
			throw new UnsupportedOperationException();
		}
	}

	// ------------------------------------------------------------------------

	private static final class TestSourceContext<T> implements SourceContext<T> {

		private final Object checkpointLock = new Object();
		private final Object watermarkLock = new Object();

		private volatile StreamRecord<T> latestElement;
		private volatile Watermark currentWatermark;

		@Override
		public void collect(T element) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			this.latestElement = new StreamRecord<>(element, timestamp);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			synchronized (watermarkLock) {
				currentWatermark = mark;
				watermarkLock.notifyAll();
			}
		}

		@Override
		public Object getCheckpointLock() {
			return checkpointLock;
		}

		@Override
		public void close() {}

		public StreamRecord<T> getLatestElement() {
			return latestElement;
		}

		public boolean hasWatermark() {
			return currentWatermark != null;
		}
		
		public Watermark getLatestWatermark() throws InterruptedException {
			synchronized (watermarkLock) {
				while (currentWatermark == null) {
					watermarkLock.wait();
				}
				Watermark wm = currentWatermark;
				currentWatermark = null;
				return wm;
			}
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
