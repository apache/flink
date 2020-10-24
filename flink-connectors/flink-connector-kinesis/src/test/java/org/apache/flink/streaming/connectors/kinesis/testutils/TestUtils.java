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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.NONE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;

/**
 * General test utils.
 */
public class TestUtils {
	/**
	 * Get standard Kinesis-related config properties.
	 */
	public static Properties getStandardProperties() {
		Properties config = new Properties();
		config.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		return config;
	}

	/**
	 * Creates a batch of {@code numOfAggregatedRecords} aggregated records.
	 * Each aggregated record contains {@code numOfChildRecords} child records.
	 * Each record is assigned the sequence number: {@code sequenceNumber + index * numOfChildRecords}.
	 * The next sequence number is output to the {@code sequenceNumber}.
	 *
	 * @param numOfAggregatedRecords the number of records in the batch
	 * @param numOfChildRecords the number of child records for each aggregated record
	 * @param sequenceNumber the starting sequence number, outputs the next sequence number
	 * @return the batch af aggregated records
	 */
	public static List<Record> createAggregatedRecordBatch(
			final int numOfAggregatedRecords,
			final int numOfChildRecords,
			final AtomicInteger sequenceNumber) {
		List<Record> recordBatch = new ArrayList<>();
		RecordAggregator recordAggregator = new RecordAggregator();

		for (int record = 0; record < numOfAggregatedRecords; record++) {
			String partitionKey = UUID.randomUUID().toString();

			for (int child = 0; child < numOfChildRecords; child++) {
				byte[] data = RandomStringUtils.randomAlphabetic(1024)
					.getBytes(ConfigConstants.DEFAULT_CHARSET);

				try {
					recordAggregator.addUserRecord(partitionKey, data);
				} catch (Exception e) {
					throw new IllegalStateException("Error aggregating message", e);
				}
			}

			AggRecord aggRecord = recordAggregator.clearAndGet();

			recordBatch.add(new Record()
				.withData(ByteBuffer.wrap(aggRecord.toRecordBytes()))
				.withPartitionKey(partitionKey)
				.withApproximateArrivalTimestamp(new Date(System.currentTimeMillis()))
				.withSequenceNumber(String.valueOf(sequenceNumber.getAndAdd(numOfChildRecords))));
		}

		return recordBatch;
	}

	public static StreamShardHandle createDummyStreamShardHandle() {
		return createDummyStreamShardHandle("stream-name", "000000");
	}

	public static StreamShardHandle createDummyStreamShardHandle(final String streamName, final String shardId) {
		final Shard shard = new Shard()
			.withSequenceNumberRange(new SequenceNumberRange()
				.withStartingSequenceNumber("0")
				.withEndingSequenceNumber("9999999999999"))
			.withHashKeyRange(new HashKeyRange()
				.withStartingHashKey("0")
				.withEndingHashKey(new BigInteger(StringUtils.repeat("FF", 16), 16).toString()))
			.withShardId(shardId);

		return new StreamShardHandle(streamName, shard);
	}

	public static Properties efoProperties() {
		Properties consumerConfig = new Properties();
		consumerConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
		consumerConfig.setProperty(EFO_REGISTRATION_TYPE, NONE.name());
		consumerConfig.setProperty(EFO_CONSUMER_ARN_PREFIX + "." + "fakeStream", "stream-consumer-arn");
		return consumerConfig;
	}

	/**
	 * A test record consumer used to capture messages from kinesis.
	 */
	public static class TestConsumer implements RecordPublisher.RecordBatchConsumer {
		private final List<RecordBatch> recordBatches = new ArrayList<>();
		private String latestSequenceNumber;

		@Override
		public SequenceNumber accept(final RecordBatch batch) {
			recordBatches.add(batch);

			if (batch.getDeaggregatedRecordSize() > 0) {
				List<UserRecord> records = batch.getDeaggregatedRecords();
				latestSequenceNumber = records.get(records.size() - 1).getSequenceNumber();
			}

			return new SequenceNumber(latestSequenceNumber);
		}

		public List<RecordBatch> getRecordBatches() {
			return recordBatches;
		}
	}
}
