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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.testutils.TestSourceContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pulsar Fetcher unit tests.
 */
public class PulsarFetcherTest extends TestLogger {

	String topicName(String topic, int partition) {
		return TopicName.get(topic).getPartition(partition).toString();
	}

	private MessageId dummyMessageId(int i) {
		return new MessageIdImpl(5, i, -1);
	}

	private long dummyMessageEventTime() {
		return 0L;
	}

	@Test
	public void testIgnorePartitionStates() throws Exception {
		String testTopic = "test-topic";
		Map<TopicRange, MessageId> offset = new HashMap<>();
		offset.put(new TopicRange(topicName(testTopic, 1)), MessageId.earliest);
		offset.put(new TopicRange(topicName(testTopic, 2)), MessageId.latest);

		TestSourceContext<Long> sourceContext = new TestSourceContext<>();
		TestFetcher<Long> fetcher = new TestFetcher<>(
			sourceContext,
			offset,
			null,
			new TestProcessingTimeService(),
			0);

		synchronized (sourceContext.getCheckpointLock()) {
			Map<TopicRange, MessageId> current = fetcher.snapshotCurrentState();
			fetcher.commitOffsetToPulsar(current, new PulsarCommitCallback() {
				@Override
				public void onSuccess() {
				}

				@Override
				public void onException(Throwable cause) {
					throw new RuntimeException("Callback failed", cause);
				}
			});

			assertTrue(fetcher.lastCommittedOffsets.isPresent());
			assertEquals(fetcher.lastCommittedOffsets.get().size(), 0);
		}
	}

	@Test
	public void testSkipCorruptedRecord() throws Exception {
		String testTopic = "test-topic";
		Map<TopicRange, MessageId> offset = Collections.singletonMap(
			new TopicRange(topicName(testTopic, 1)),
			MessageId.latest);

		TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
		TestFetcher<Long> fetcher = new TestFetcher<>(
			sourceContext,
			offset,
			null,
			new TestProcessingTimeService(),
			0);

		PulsarTopicState stateHolder = fetcher.getSubscribedTopicStates().get(0);
		fetcher.emitRecordsWithTimestamps(
			1L,
			stateHolder,
			dummyMessageId(1),
			dummyMessageEventTime());
		fetcher.emitRecordsWithTimestamps(
			2L,
			stateHolder,
			dummyMessageId(2),
			dummyMessageEventTime());
		assertEquals(2L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(dummyMessageId(2), stateHolder.getOffset());

		// emit null record
		fetcher.emitRecordsWithTimestamps(
			null,
			stateHolder,
			dummyMessageId(3),
			dummyMessageEventTime());
		assertEquals(2L, sourceContext.getLatestElement().getValue().longValue());
		assertEquals(dummyMessageId(3), stateHolder.getOffset());
	}

	@Test
	public void testConcurrentPartitionsDiscoveryAndLoopFetching() throws Exception {
		String tp = "test-topic";
		TestSourceContext<Long> sourceContext = new TestSourceContext<Long>();
		Map<TopicRange, MessageId> offset = Collections.singletonMap(
			new TopicRange(topicName(tp, 2)),
			MessageId.latest);

		OneShotLatch fetchLoopWaitLatch = new OneShotLatch();
		OneShotLatch stateIterationBlockLatch = new OneShotLatch();

		TestFetcher fetcher = new TestFetcher(
			sourceContext,
			offset,
			null,
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

		fetchLoopWaitLatch.await();
		fetcher.addDiscoveredTopics(Sets.newSet(new TopicRange(tp)));

		stateIterationBlockLatch.trigger();
		checkedThread.sync();
	}

	private static final class TestFetcher<T> extends PulsarFetcher<T> {

		private final OneShotLatch fetchLoopWaitLatch;
		private final OneShotLatch stateIterationBlockLatch;
		Optional<Map<TopicRange, MessageId>> lastCommittedOffsets = Optional.empty();

		public TestFetcher(
			SourceFunction.SourceContext<T> sourceContext,
			Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval) throws Exception {
			this(
				sourceContext,
				seedTopicsWithInitialOffsets,
				watermarkStrategy,
				processingTimeProvider,
				autoWatermarkInterval,
				null,
				null);
		}

		public TestFetcher(
			SourceFunction.SourceContext<T> sourceContext,
			Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			OneShotLatch fetchLoopWaitLatch,
			OneShotLatch stateIterationBlockLatch) throws Exception {
			super(
				sourceContext,
				seedTopicsWithInitialOffsets,
				watermarkStrategy,
				processingTimeProvider,
				autoWatermarkInterval,
				TestFetcher.class.getClassLoader(),
				null,
				null,
				null,
				0,
				null,
				null,
				new UnregisteredMetricsGroup(),
				false);
			this.fetchLoopWaitLatch = fetchLoopWaitLatch;
			this.stateIterationBlockLatch = stateIterationBlockLatch;
		}

		@Override
		public void runFetchLoop() throws Exception {
			if (fetchLoopWaitLatch != null) {
				for (PulsarTopicState state : subscribedPartitionStates) {
					fetchLoopWaitLatch.trigger();
					stateIterationBlockLatch.await();
				}
			} else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public void cancel() throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public void doCommitOffsetToPulsar(
			Map<TopicRange, MessageId> offset,
			PulsarCommitCallback offsetCommitCallback) {

			lastCommittedOffsets = Optional.of(offset);
			offsetCommitCallback.onSuccess();
		}
	}
}
