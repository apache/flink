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
import org.apache.flink.streaming.connectors.kinesis.metrics.PollingRecordPublisherMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils.TestConsumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.COMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.INCOMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link PollingRecordPublisher}. */
public class PollingRecordPublisherTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRunPublishesRecordsToConsumer() throws Exception {
        KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 1, 100);
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        TestConsumer consumer = new TestConsumer();
        recordPublisher.run(consumer);

        assertEquals(1, consumer.getRecordBatches().size());
        assertEquals(5, consumer.getRecordBatches().get(0).getDeaggregatedRecordSize());
        assertEquals(100L, consumer.getRecordBatches().get(0).getMillisBehindLatest(), 0);
    }

    @Test
    public void testRunReturnsCompleteWhenShardExpires() throws Exception {
        // There are 2 batches available in the stream
        KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 2, 100);
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        // First call results in INCOMPLETE, there is one batch left
        assertEquals(INCOMPLETE, recordPublisher.run(new TestConsumer()));

        // After second call the shard is complete
        assertEquals(COMPLETE, recordPublisher.run(new TestConsumer()));
    }

    @Test
    public void testRunOnCompletelyConsumedShardReturnsComplete() throws Exception {
        KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 1, 100);
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        assertEquals(COMPLETE, recordPublisher.run(new TestConsumer()));
        assertEquals(COMPLETE, recordPublisher.run(new TestConsumer()));
    }

    @Test
    public void testRunGetShardIteratorReturnsNullIsComplete() throws Exception {
        KinesisProxyInterface fakeKinesis =
                FakeKinesisBehavioursFactory.noShardsFoundForRequestedStreamsBehaviour();
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        assertEquals(COMPLETE, recordPublisher.run(new TestConsumer()));
    }

    @Test
    public void testRunGetRecordsRecoversFromExpiredIteratorException() throws Exception {
        KinesisProxyInterface fakeKinesis =
                spy(
                        FakeKinesisBehavioursFactory
                                .totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(
                                        2, 2, 1, 500));
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        recordPublisher.run(new TestConsumer());

        // Get shard iterator is called twice, once during first run, secondly to refresh expired
        // iterator
        verify(fakeKinesis, times(2)).getShardIterator(any(), any(), any());
    }

    @Test
    public void validateExpiredIteratorBackoffMillisNegativeThrows() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        new PollingRecordPublisher(
                StartingPosition.restartFromSequenceNumber(SENTINEL_EARLIEST_SEQUENCE_NUM.get()),
                TestUtils.createDummyStreamShardHandle(),
                mock(PollingRecordPublisherMetricsReporter.class),
                mock(KinesisProxyInterface.class),
                100,
                -1);
    }

    @Test
    public void validateMaxNumberOfRecordsPerFetchZeroThrows() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        new PollingRecordPublisher(
                StartingPosition.restartFromSequenceNumber(SENTINEL_EARLIEST_SEQUENCE_NUM.get()),
                TestUtils.createDummyStreamShardHandle(),
                mock(PollingRecordPublisherMetricsReporter.class),
                mock(KinesisProxyInterface.class),
                0,
                100);
    }

    PollingRecordPublisher createPollingRecordPublisher(final KinesisProxyInterface kinesis)
            throws Exception {
        PollingRecordPublisherMetricsReporter metricsReporter =
                new PollingRecordPublisherMetricsReporter(mock(MetricGroup.class));

        return new PollingRecordPublisher(
                StartingPosition.restartFromSequenceNumber(SENTINEL_EARLIEST_SEQUENCE_NUM.get()),
                TestUtils.createDummyStreamShardHandle(),
                metricsReporter,
                kinesis,
                10000,
                500L);
    }
}
