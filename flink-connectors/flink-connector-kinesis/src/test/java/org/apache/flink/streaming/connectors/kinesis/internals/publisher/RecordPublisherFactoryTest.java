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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RecordPublisherFactory}.
 */
public class RecordPublisherFactoryTest {

	private final RecordPublisherFactory factory = new TestRecordPublisherFactory();

	@Test
	public void testGetStartingPositionContinuesFromSequenceNumber() {
		SequenceNumber sequenceNumber = new SequenceNumber("fake");

		StartingPosition startingPosition = factory.getStartingPosition(sequenceNumber, new Properties());

		assertEquals("fake", startingPosition.getStartingMarker());
		assertEquals(AFTER_SEQUENCE_NUMBER, startingPosition.getShardIteratorType());
	}

	@Test
	public void testGetStartingPositionRestartsFromAggregatedSequenceNumber() {
		SequenceNumber sequenceNumber = new SequenceNumber("fake", 1);

		StartingPosition startingPosition = factory.getStartingPosition(sequenceNumber, new Properties());

		assertEquals("fake", startingPosition.getStartingMarker());
		assertEquals(AT_SEQUENCE_NUMBER, startingPosition.getShardIteratorType());
	}

	@Test
	public void testGetStartingPositionFromLatest() {
		SequenceNumber sequenceNumber = SENTINEL_LATEST_SEQUENCE_NUM.get();

		StartingPosition startingPosition = factory.getStartingPosition(sequenceNumber, new Properties());

		assertEquals(LATEST, startingPosition.getShardIteratorType());
	}

	@Test
	public void testGetStartingPositionFromTimestamp() throws Exception {
		SequenceNumber sequenceNumber = SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get();

		String format = "yyyy-MM-dd'T'HH:mm";
		String timestamp = "2020-07-02T09:14";
		Date expectedTimestamp = new SimpleDateFormat(format).parse(timestamp);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
		consumerProperties.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);

		StartingPosition startingPosition = factory.getStartingPosition(sequenceNumber, consumerProperties);

		assertEquals(AT_TIMESTAMP, startingPosition.getShardIteratorType());
		assertEquals(expectedTimestamp, startingPosition.getStartingMarker());
	}

	private static class TestRecordPublisherFactory implements RecordPublisherFactory {

		@Override
		public RecordPublisher create(
				final SequenceNumber sequenceNumber,
				final Properties consumerConfig,
				final MetricGroup metricGroup,
				final StreamShardHandle streamShardHandle) throws InterruptedException {
			throw new UnsupportedOperationException("Not implemented for this test");
		}
	}

}
