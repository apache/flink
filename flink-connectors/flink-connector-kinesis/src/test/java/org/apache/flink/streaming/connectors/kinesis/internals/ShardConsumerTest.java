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

import org.apache.flink.streaming.connectors.kinesis.internals.publisher.polling.PollingRecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardConsumerMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumerTestUtils.fakeSequenceNumber;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests for the {@link ShardConsumer} using Polling consumption mocked Kinesis behaviours.
 */
public class ShardConsumerTest {

	@Test
	public void testMetricsReporting() throws Exception {
		KinesisProxyInterface kinesis = FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls(500, 5, 500);

		ShardConsumerMetricsReporter metrics = assertNumberOfMessagesReceivedFromKinesis(500, kinesis, fakeSequenceNumber());
		assertEquals(500, metrics.getMillisBehindLatest());
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithStartingSequenceNumber() throws Exception {
		KinesisProxyInterface kinesis = spy(FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls(1000, 9, 500L));

		assertNumberOfMessagesReceivedFromKinesis(1000, kinesis, fakeSequenceNumber());
		verify(kinesis).getShardIterator(any(), eq("AFTER_SEQUENCE_NUMBER"), eq("fakeStartingState"));
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithStartingSequenceSentinelTimestamp() throws Exception {
		String format = "yyyy-MM-dd'T'HH:mm";
		String timestamp = "2020-07-02T09:14";
		Date expectedTimestamp = new SimpleDateFormat(format).parse(timestamp);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
		consumerProperties.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);
		SequenceNumber sequenceNumber = SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get();

		KinesisProxyInterface kinesis = spy(FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls(10, 1, 0));

		assertNumberOfMessagesReceivedFromKinesis(10, kinesis, sequenceNumber, consumerProperties);
		verify(kinesis).getShardIterator(any(), eq("AT_TIMESTAMP"), eq(expectedTimestamp));
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithStartingSequenceSentinelEarliest() throws Exception {
		SequenceNumber sequenceNumber = SENTINEL_EARLIEST_SEQUENCE_NUM.get();

		KinesisProxyInterface kinesis = spy(FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls(50, 2, 0));

		assertNumberOfMessagesReceivedFromKinesis(50, kinesis, sequenceNumber);
		verify(kinesis).getShardIterator(any(), eq("TRIM_HORIZON"), eq(null));
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithUnexpectedExpiredIterator() throws Exception {
		KinesisProxyInterface kinesis = FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(1000, 9, 7, 500L);

		// Get a total of 1000 records with 9 getRecords() calls,
		// and the 7th getRecords() call will encounter an unexpected expired shard iterator
		assertNumberOfMessagesReceivedFromKinesis(1000, kinesis, fakeSequenceNumber());
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithAdaptiveReads() throws Exception {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(SHARD_USE_ADAPTIVE_READS, "true");

		KinesisProxyInterface kinesis = FakeKinesisBehavioursFactory.initialNumOfRecordsAfterNumOfGetRecordsCallsWithAdaptiveReads(10, 2, 500L);

		// Avg record size for first batch --> 10 * 10 Kb/10 = 10 Kb
		// Number of records fetched in second batch --> 2 Mb/10Kb * 5 = 40
		// Total number of records = 10 + 40 = 50
		assertNumberOfMessagesReceivedFromKinesis(50, kinesis, fakeSequenceNumber(), consumerProperties);
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithAggregatedRecords() throws Exception {
		KinesisProxyInterface kinesis = spy(FakeKinesisBehavioursFactory.aggregatedRecords(3, 5, 10));

		// Expecting to receive all messages
		// 10 batches of 3 aggregated records each with 5 child records
		// 10 * 3 * 5 = 150
		ShardConsumerMetricsReporter metrics = assertNumberOfMessagesReceivedFromKinesis(150, kinesis, fakeSequenceNumber());
		assertEquals(3, metrics.getNumberOfAggregatedRecords());
		assertEquals(15, metrics.getNumberOfDeaggregatedRecords());

		verify(kinesis).getShardIterator(any(), eq("AFTER_SEQUENCE_NUMBER"), eq("fakeStartingState"));
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithAggregatedRecordsWithSubSequenceStartingNumber() throws Exception {
		SequenceNumber sequenceNumber = new SequenceNumber("0", 5);
		KinesisProxyInterface kinesis = spy(FakeKinesisBehavioursFactory.aggregatedRecords(1, 10, 5));

		// Expecting to start consuming from last sub sequence number
		// 5 batches of 1 aggregated record each with 10 child records
		// Last consumed message was sub-sequence 5 (6/10) (zero based) (remaining are 6, 7, 8, 9)
		// 5 * 1 * 10 - 6 = 44
		ShardConsumerMetricsReporter metrics = assertNumberOfMessagesReceivedFromKinesis(44, kinesis, sequenceNumber);
		assertEquals(1, metrics.getNumberOfAggregatedRecords());
		assertEquals(10, metrics.getNumberOfDeaggregatedRecords());

		verify(kinesis).getShardIterator(any(), eq("AT_SEQUENCE_NUMBER"), eq("0"));
	}

	private ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
			final int expectedNumberOfMessages,
			final KinesisProxyInterface kinesis,
			final SequenceNumber startingSequenceNumber) throws Exception {
		return assertNumberOfMessagesReceivedFromKinesis(expectedNumberOfMessages, kinesis, startingSequenceNumber, new Properties());
	}

	private ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
			final int expectedNumberOfMessages,
			final KinesisProxyInterface kinesis,
			final SequenceNumber startingSequenceNumber,
			final Properties consumerProperties) throws Exception {

		return ShardConsumerTestUtils.assertNumberOfMessagesReceivedFromKinesis(
			expectedNumberOfMessages,
			new PollingRecordPublisherFactory(config -> kinesis),
			startingSequenceNumber,
			consumerProperties);
	}

}
