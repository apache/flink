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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardConsumerMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.AbstractSingleShardFanOutKinesisV2;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.SingleShardFanOutKinesisV2;

import com.amazonaws.SdkClientException;
import com.amazonaws.http.timers.client.SdkInterruptedException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumerTestUtils.fakeSequenceNumber;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils.efoProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;

/** Tests for the {@link ShardConsumer} using Fan Out consumption mocked Kinesis behaviours. */
public class ShardConsumerFanOutTest {

    @Test
    public void testEmptyShard() throws Exception {
        SingleShardFanOutKinesisV2 kinesis = FakeKinesisFanOutBehavioursFactory.emptyShard();

        assertNumberOfMessagesReceivedFromKinesis(0, kinesis, fakeSequenceNumber());

        assertThat(kinesis.getNumberOfSubscribeToShardInvocations()).isEqualTo(1);
    }

    @Test
    public void testStartFromLatestIsTranslatedToTimestamp() throws Exception {
        Instant now = Instant.now();
        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard().build();
        SequenceNumber sequenceNumber = SENTINEL_LATEST_SEQUENCE_NUM.get();

        // Fake behaviour defaults to 10 messages
        assertNumberOfMessagesReceivedFromKinesis(10, kinesis, sequenceNumber, efoProperties());

        StartingPosition actual = kinesis.getStartingPositionForSubscription(0);
        assertThat(actual.type()).isEqualTo(AT_TIMESTAMP);

        // Considering milliseconds to make now have the same precision as actual
        assertThat(now.toEpochMilli()).isLessThanOrEqualTo(actual.timestamp().toEpochMilli());
    }

    @Test
    public void testStartFromLatestReceivesNoRecordsContinuesToUseTimestamp() throws Exception {
        AbstractSingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.emptyBatchFollowedBySingleRecord();

        SequenceNumber sequenceNumber = SENTINEL_LATEST_SEQUENCE_NUM.get();

        // Fake behaviour defaults to 10 messages
        assertNumberOfMessagesReceivedFromKinesis(1, kinesis, sequenceNumber, efoProperties());

        // This fake Kinesis will give 2 subscriptions
        assertThat(kinesis.getNumberOfSubscribeToShardInvocations()).isEqualTo(2);

        assertThat(kinesis.getStartingPositionForSubscription(0).type()).isEqualTo(AT_TIMESTAMP);
        assertThat(kinesis.getStartingPositionForSubscription(1).type()).isEqualTo(AT_TIMESTAMP);
    }

    @Test
    public void testBoundedShardConsumesFromTimestamp() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm";
        String timestamp = "2020-07-02T09:14";
        Instant expectedTimestamp = new SimpleDateFormat(format).parse(timestamp).toInstant();

        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard().build();

        Properties consumerConfig = efoProperties();
        consumerConfig.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
        consumerConfig.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);
        SequenceNumber sequenceNumber = SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get();

        // Fake behaviour defaults to 10 messages
        assertNumberOfMessagesReceivedFromKinesis(10, kinesis, sequenceNumber, consumerConfig);

        StartingPosition actual = kinesis.getStartingPositionForSubscription(0);
        assertThat(actual.type()).isEqualTo(AT_TIMESTAMP);
        assertThat(actual.timestamp()).isEqualTo(expectedTimestamp);
    }

    @Test
    public void testMillisBehindReported() throws Exception {
        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard()
                        .withMillisBehindLatest(123L)
                        .build();

        // Fake behaviour defaults to 10 messages
        ShardConsumerMetricsReporter metrics =
                assertNumberOfMessagesReceivedFromKinesis(10, kinesis, fakeSequenceNumber());

        assertThat(metrics.getMillisBehindLatest()).isEqualTo(123L);
    }

    @Test
    public void testBoundedShardConsumesCorrectNumberOfMessages() throws Exception {
        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard()
                        .withBatchCount(10)
                        .withRecordsPerBatch(5)
                        .build();

        // 10 batches of 5 records = 50
        assertNumberOfMessagesReceivedFromKinesis(50, kinesis, fakeSequenceNumber());

        assertThat(kinesis.getNumberOfSubscribeToShardInvocations()).isEqualTo(1);
    }

    @Test
    public void testBoundedShardResubscribesToShard() throws Exception {
        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard()
                        .withBatchCount(100)
                        .withRecordsPerBatch(10)
                        .withBatchesPerSubscription(5)
                        .build();

        // 100 batches of 10 records = 1000
        assertNumberOfMessagesReceivedFromKinesis(1000, kinesis, fakeSequenceNumber());

        // 100 batches / 5 batches per subscription = 20 subscriptions
        assertThat(kinesis.getNumberOfSubscribeToShardInvocations()).isEqualTo(20);

        // Starting from non-aggregated sequence number means we should start AFTER the sequence
        // number
        assertThat(kinesis.getStartingPositionForSubscription(0).type())
                .isEqualTo(AFTER_SEQUENCE_NUMBER);
    }

    @Test
    public void testBoundedShardWithAggregatedRecords() throws Exception {
        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard()
                        .withBatchCount(100)
                        .withRecordsPerBatch(10)
                        .withAggregationFactor(100)
                        .build();

        // 100 batches of 10 records * 100 aggregation factor = 100000
        assertNumberOfMessagesReceivedFromKinesis(100000, kinesis, fakeSequenceNumber());
    }

    @Test
    public void testBoundedShardResumingConsumptionFromAggregatedSubsequenceNumber()
            throws Exception {
        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard()
                        .withBatchCount(10)
                        .withRecordsPerBatch(1)
                        .withAggregationFactor(10)
                        .build();

        SequenceNumber subsequenceNumber = new SequenceNumber("1", 5);

        // 10 batches of 1 record * 10 aggregation factor - 6 previously consumed subsequence
        // records (0,1,2,3,4,5) = 94
        assertNumberOfMessagesReceivedFromKinesis(94, kinesis, subsequenceNumber);

        // Starting from aggregated sequence number means we should start AT the sequence number
        assertThat(kinesis.getStartingPositionForSubscription(0).type())
                .isEqualTo(AT_SEQUENCE_NUMBER);
    }

    @Test
    public void testSubscribeToShardUsesCorrectStartingSequenceNumbers() throws Exception {
        SingleShardFanOutKinesisV2 kinesis =
                FakeKinesisFanOutBehavioursFactory.boundedShard()
                        .withBatchCount(10)
                        .withRecordsPerBatch(1)
                        .withBatchesPerSubscription(2)
                        .build();

        // 10 batches of 1 records = 10
        assertNumberOfMessagesReceivedFromKinesis(10, kinesis, new SequenceNumber("0"));

        // 10 batches / 2 batches per subscription = 5 subscriptions
        assertThat(kinesis.getNumberOfSubscribeToShardInvocations()).isEqualTo(5);

        // Starting positions should correlate to the last consumed sequence number
        assertStartingPositionAfterSequenceNumber(
                kinesis.getStartingPositionForSubscription(0), "0");
        assertStartingPositionAfterSequenceNumber(
                kinesis.getStartingPositionForSubscription(1), "2");
        assertStartingPositionAfterSequenceNumber(
                kinesis.getStartingPositionForSubscription(2), "4");
        assertStartingPositionAfterSequenceNumber(
                kinesis.getStartingPositionForSubscription(3), "6");
        assertStartingPositionAfterSequenceNumber(
                kinesis.getStartingPositionForSubscription(4), "8");
    }

    @Test
    public void testShardConsumerExitsWhenRecordPublisherIsInterrupted() throws Exception {
        // Throws error after 5 records
        KinesisProxyV2Interface kinesis =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                        new SdkInterruptedException(null));

        int expectedNumberOfRecordsReadFromKinesisBeforeError = 5;
        SequenceNumber startingSequenceNumber = new SequenceNumber("0");
        SequenceNumber expectedLastProcessSequenceNumber = new SequenceNumber("5");

        // SdkInterruptedException will terminate the consumer, it will not retry and read only the
        // first 5 records
        ShardConsumerTestUtils.assertNumberOfMessagesReceivedFromKinesis(
                expectedNumberOfRecordsReadFromKinesisBeforeError,
                new FanOutRecordPublisherFactory(kinesis),
                startingSequenceNumber,
                efoProperties(),
                expectedLastProcessSequenceNumber);
    }

    @Test
    public void testShardConsumerRetriesGenericSdkError() throws Exception {
        // Throws error after 5 records and there are 25 records available in the shard
        KinesisProxyV2Interface kinesis =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                        new SdkClientException(""));

        int expectedNumberOfRecordsReadFromKinesisBeforeError = 25;
        SequenceNumber startingSequenceNumber = new SequenceNumber("0");

        Properties properties = efoProperties();
        // Speed up test by reducing backoff time
        properties.setProperty(SUBSCRIBE_TO_SHARD_BACKOFF_MAX, "1");

        // SdkClientException will cause a retry, each retry will result in 5 more records being
        // consumed
        // The shard will consume all 25 records
        assertNumberOfMessagesReceivedFromKinesis(
                expectedNumberOfRecordsReadFromKinesisBeforeError,
                kinesis,
                startingSequenceNumber,
                properties);
    }

    private void assertStartingPositionAfterSequenceNumber(
            final StartingPosition startingPosition, final String sequenceNumber) {
        assertThat(startingPosition.type()).isEqualTo(AFTER_SEQUENCE_NUMBER);
        assertThat(startingPosition.sequenceNumber()).isEqualTo(sequenceNumber);
    }

    private ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
            final int expectedNumberOfMessages,
            final KinesisProxyV2Interface kinesis,
            final SequenceNumber startingSequenceNumber)
            throws Exception {
        return assertNumberOfMessagesReceivedFromKinesis(
                expectedNumberOfMessages, kinesis, startingSequenceNumber, efoProperties());
    }

    private ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
            final int expectedNumberOfMessages,
            final KinesisProxyV2Interface kinesis,
            final SequenceNumber startingSequenceNumber,
            final Properties consumerConfig)
            throws Exception {
        return ShardConsumerTestUtils.assertNumberOfMessagesReceivedFromKinesis(
                expectedNumberOfMessages,
                new FanOutRecordPublisherFactory(kinesis),
                startingSequenceNumber,
                consumerConfig);
    }
}
