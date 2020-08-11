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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.polling;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult;
import org.apache.flink.streaming.connectors.kinesis.metrics.PollingRecordPublisherMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.COMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.INCOMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.StartingPosition.continueFromSequenceNumber;
import static org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link PollingRecordPublisher}.
 */
public class PollingRecordPublisherTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testRunPublishesRecordsToConsumer() throws InterruptedException {
		KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 1, 100);
		PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

		TestConsumer consumer = new TestConsumer();
		RecordPublisherRunResult result = recordPublisher.run(earliest(), consumer);

		assertEquals(1, consumer.recordBatches.size());
		assertEquals(5, consumer.recordBatches.get(0).getDeaggregatedRecordSize());
		assertEquals(100L, consumer.recordBatches.get(0).getMillisBehindLatest(), 0);
	}

	@Test
	public void testRunReturnsCompleteWhenShardExpires() throws InterruptedException {
		// There are 2 batches available in the stream
		KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 2, 100);
		PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

		// First call results in INCOMPLETE, there is one batch left
		assertEquals(INCOMPLETE, recordPublisher.run(earliest(), batch -> {}));

		// After second call the shard is complete
		assertEquals(COMPLETE, recordPublisher.run(earliest(), batch -> {}));
	}

	@Test
	public void testRunOnCompletelyConsumedShardReturnsComplete() throws InterruptedException {
		KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 1, 100);
		PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

		assertEquals(COMPLETE, recordPublisher.run(earliest(), batch -> {}));
		assertEquals(COMPLETE, recordPublisher.run(earliest(), batch -> {}));
	}

	@Test
	public void testRunGetShardIteratorReturnsNullIsComplete() throws InterruptedException {
		KinesisProxyInterface fakeKinesis = FakeKinesisBehavioursFactory.noShardsFoundForRequestedStreamsBehaviour();
		PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

		assertEquals(COMPLETE, recordPublisher.run(earliest(), batch -> {}));
	}

	@Test
	public void testRunGetRecordsRecoversFromExpiredIteratorException() throws InterruptedException {
		KinesisProxyInterface fakeKinesis = spy(FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(2, 2, 1, 500));
		PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

		recordPublisher.run(earliest(), batch -> {});

		// Get shard iterator is called twice, once during first run, secondly to refresh expired iterator
		verify(fakeKinesis, times(2)).getShardIterator(any(), any(), any());
	}

	@Test
	public void validateExpiredIteratorBackoffMillisNegativeThrows() {
		thrown.expect(IllegalArgumentException.class);

		new PollingRecordPublisher(
			TestUtils.createDummyStreamShardHandle(),
			mock(PollingRecordPublisherMetricsReporter.class),
			mock(KinesisProxyInterface.class),
			100,
			-1);
	}

	@Test
	public void validateMaxNumberOfRecordsPerFetchZeroThrows() {
		thrown.expect(IllegalArgumentException.class);

		new PollingRecordPublisher(
			TestUtils.createDummyStreamShardHandle(),
			mock(PollingRecordPublisherMetricsReporter.class),
			mock(KinesisProxyInterface.class),
			0,
			100);
	}

	private StartingPosition latest() {
		return continueFromSequenceNumber(SENTINEL_LATEST_SEQUENCE_NUM.get());
	}

	private StartingPosition earliest() {
		return continueFromSequenceNumber(SENTINEL_EARLIEST_SEQUENCE_NUM.get());
	}

	PollingRecordPublisher createPollingRecordPublisher(final KinesisProxyInterface kinesis) {
		PollingRecordPublisherMetricsReporter metricsReporter = new PollingRecordPublisherMetricsReporter(mock(MetricGroup.class));

		return new PollingRecordPublisher(
			TestUtils.createDummyStreamShardHandle(),
			metricsReporter,
			kinesis,
			10000,
			500L);
	}

	private static class TestConsumer implements Consumer<RecordBatch> {
		private final List<RecordBatch> recordBatches = new ArrayList<>();

		@Override
		public void accept(final RecordBatch batch) {
			recordBatches.add(batch);
		}
	}
}
