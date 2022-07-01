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

import org.apache.flink.streaming.connectors.kinesis.metrics.PollingRecordPublisherMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils.TestConsumer;

import org.junit.jupiter.api.Test;

import static org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumerTestUtils.createFakeShardConsumerMetricGroup;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.COMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.INCOMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link PollingRecordPublisher}. */
public class PollingRecordPublisherTest {

    private static final long FETCH_INTERVAL_MILLIS = 500L;

    @Test
    public void testRunPublishesRecordsToConsumer() throws Exception {
        KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 1, 100);
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        TestConsumer consumer = new TestConsumer();
        recordPublisher.run(consumer);

        assertThat(consumer.getRecordBatches()).hasSize(1);
        assertThat(consumer.getRecordBatches().get(0).getDeaggregatedRecordSize()).isEqualTo(5);
        assertThat(consumer.getRecordBatches().get(0).getMillisBehindLatest()).isEqualTo(100L);
    }

    @Test
    public void testRunEmitsRunLoopTimeNanos() throws Exception {
        PollingRecordPublisherMetricsReporter metricsReporter =
                spy(
                        new PollingRecordPublisherMetricsReporter(
                                createFakeShardConsumerMetricGroup()));

        KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 5, 100);
        PollingRecordPublisher recordPublisher =
                createPollingRecordPublisher(fakeKinesis, metricsReporter);

        recordPublisher.run(new TestConsumer());

        // Expect that the run loop took at least FETCH_INTERVAL_MILLIS in nanos
        verify(metricsReporter).setRunLoopTimeNanos(geq(FETCH_INTERVAL_MILLIS * 1_000_000));
    }

    @Test
    public void testRunReturnsCompleteWhenShardExpires() throws Exception {
        // There are 2 batches available in the stream
        KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 2, 100);
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        // First call results in INCOMPLETE, there is one batch left
        assertThat(recordPublisher.run(new TestConsumer())).isEqualTo(INCOMPLETE);

        // After second call the shard is complete
        assertThat(recordPublisher.run(new TestConsumer())).isEqualTo(COMPLETE);
    }

    @Test
    public void testRunOnCompletelyConsumedShardReturnsComplete() throws Exception {
        KinesisProxyInterface fakeKinesis = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 1, 100);
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        assertThat(recordPublisher.run(new TestConsumer())).isEqualTo(COMPLETE);
        assertThat(recordPublisher.run(new TestConsumer())).isEqualTo(COMPLETE);
    }

    @Test
    public void testRunGetShardIteratorReturnsNullIsComplete() throws Exception {
        KinesisProxyInterface fakeKinesis =
                FakeKinesisBehavioursFactory.noShardsFoundForRequestedStreamsBehaviour();
        PollingRecordPublisher recordPublisher = createPollingRecordPublisher(fakeKinesis);

        assertThat(recordPublisher.run(new TestConsumer())).isEqualTo(COMPLETE);
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
    public void validateExpiredIteratorBackoffMillisNegativeThrows() {
        assertThatThrownBy(
                        () -> {
                            new PollingRecordPublisher(
                                    StartingPosition.restartFromSequenceNumber(
                                            SENTINEL_EARLIEST_SEQUENCE_NUM.get()),
                                    TestUtils.createDummyStreamShardHandle(),
                                    mock(PollingRecordPublisherMetricsReporter.class),
                                    mock(KinesisProxyInterface.class),
                                    100,
                                    -1);
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void validateMaxNumberOfRecordsPerFetchZeroThrows() {
        assertThatThrownBy(
                        () -> {
                            new PollingRecordPublisher(
                                    StartingPosition.restartFromSequenceNumber(
                                            SENTINEL_EARLIEST_SEQUENCE_NUM.get()),
                                    TestUtils.createDummyStreamShardHandle(),
                                    mock(PollingRecordPublisherMetricsReporter.class),
                                    mock(KinesisProxyInterface.class),
                                    0,
                                    100);
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    PollingRecordPublisher createPollingRecordPublisher(final KinesisProxyInterface kinesis)
            throws Exception {
        PollingRecordPublisherMetricsReporter metricsReporter =
                new PollingRecordPublisherMetricsReporter(createFakeShardConsumerMetricGroup());

        return createPollingRecordPublisher(kinesis, metricsReporter);
    }

    PollingRecordPublisher createPollingRecordPublisher(
            final KinesisProxyInterface kinesis,
            final PollingRecordPublisherMetricsReporter metricGroupReporter)
            throws Exception {
        return new PollingRecordPublisher(
                StartingPosition.restartFromSequenceNumber(SENTINEL_EARLIEST_SEQUENCE_NUM.get()),
                TestUtils.createDummyStreamShardHandle(),
                metricGroupReporter,
                kinesis,
                10000,
                FETCH_INTERVAL_MILLIS);
    }
}
