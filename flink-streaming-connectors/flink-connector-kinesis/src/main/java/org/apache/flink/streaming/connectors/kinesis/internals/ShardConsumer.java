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

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thread that does the actual data pulling from AWS Kinesis shards. Each thread is in charge of one Kinesis shard only.
 */
public class ShardConsumer<T> implements Runnable {

	private final KinesisDeserializationSchema<T> deserializer;

	private final KinesisProxyInterface kinesis;

	private final int subscribedShardStateIndex;

	private final KinesisDataFetcher<T> fetcherRef;

	private final KinesisStreamShard subscribedShard;

	private final int maxNumberOfRecordsPerFetch;
	private final long fetchIntervalMillis;

	private SequenceNumber lastSequenceNum;

	/**
	 * Creates a shard consumer.
	 *
	 * @param fetcherRef reference to the owning fetcher
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param subscribedShard the shard this consumer is subscribed to
	 * @param lastSequenceNum the sequence number in the shard to start consuming
	 */
	public ShardConsumer(KinesisDataFetcher<T> fetcherRef,
						Integer subscribedShardStateIndex,
						KinesisStreamShard subscribedShard,
						SequenceNumber lastSequenceNum) {
		this(fetcherRef,
			subscribedShardStateIndex,
			subscribedShard,
			lastSequenceNum,
			KinesisProxy.create(fetcherRef.getConsumerConfiguration()));
	}

	/** This constructor is exposed for testing purposes */
	protected ShardConsumer(KinesisDataFetcher<T> fetcherRef,
							Integer subscribedShardStateIndex,
							KinesisStreamShard subscribedShard,
							SequenceNumber lastSequenceNum,
							KinesisProxyInterface kinesis) {
		this.fetcherRef = checkNotNull(fetcherRef);
		this.subscribedShardStateIndex = checkNotNull(subscribedShardStateIndex);
		this.subscribedShard = checkNotNull(subscribedShard);
		this.lastSequenceNum = checkNotNull(lastSequenceNum);

		this.deserializer = fetcherRef.getClonedDeserializationSchema();

		Properties consumerConfig = fetcherRef.getConsumerConfiguration();
		this.kinesis = kinesis;
		this.maxNumberOfRecordsPerFetch = Integer.valueOf(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
			Integer.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX)));
		this.fetchIntervalMillis = Long.valueOf(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
			Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		String nextShardItr;

		try {
			// before infinitely looping, we set the initial nextShardItr appropriately

			if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.get())) {
				// if the shard is already closed, there will be no latest next record to get for this shard
				if (subscribedShard.isClosed()) {
					nextShardItr = null;
				} else {
					nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.LATEST.toString(), null);
				}
			} else if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get())) {
				nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.TRIM_HORIZON.toString(), null);
			} else if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get())) {
				nextShardItr = null;
			} else {
				// we will be starting from an actual sequence number (due to restore from failure).
				// if the last sequence number refers to an aggregated record, we need to clean up any dangling sub-records
				// from the last aggregated record; otherwise, we can simply start iterating from the record right after.

				if (lastSequenceNum.isAggregated()) {
					String itrForLastAggregatedRecord =
						kinesis.getShardIterator(subscribedShard, ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), lastSequenceNum.getSequenceNumber());

					// get only the last aggregated record
					GetRecordsResult getRecordsResult = kinesis.getRecords(itrForLastAggregatedRecord, 1);

					List<UserRecord> fetchedRecords = deaggregateRecords(
						getRecordsResult.getRecords(),
						subscribedShard.getShard().getHashKeyRange().getStartingHashKey(),
						subscribedShard.getShard().getHashKeyRange().getEndingHashKey());

					long lastSubSequenceNum = lastSequenceNum.getSubSequenceNumber();
					for (UserRecord record : fetchedRecords) {
						// we have found a dangling sub-record if it has a larger subsequence number
						// than our last sequence number; if so, collect the record and update state
						if (record.getSubSequenceNumber() > lastSubSequenceNum) {
							deserializeRecordForCollectionAndUpdateState(record);
						}
					}

					// set the nextShardItr so we can continue iterating in the next while loop
					nextShardItr = getRecordsResult.getNextShardIterator();
				} else {
					// the last record was non-aggregated, so we can simply start from the next record
					nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), lastSequenceNum.getSequenceNumber());
				}
			}

			while(isRunning()) {
				if (nextShardItr == null) {
					fetcherRef.updateState(subscribedShardStateIndex, SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get());

					// we can close this consumer thread once we've reached the end of the subscribed shard
					break;
				} else {
					if (fetchIntervalMillis != 0) {
						Thread.sleep(fetchIntervalMillis);
					}

					GetRecordsResult getRecordsResult = kinesis.getRecords(nextShardItr, maxNumberOfRecordsPerFetch);

					// each of the Kinesis records may be aggregated, so we must deaggregate them before proceeding
					List<UserRecord> fetchedRecords = deaggregateRecords(
						getRecordsResult.getRecords(),
						subscribedShard.getShard().getHashKeyRange().getStartingHashKey(),
						subscribedShard.getShard().getHashKeyRange().getEndingHashKey());

					for (UserRecord record : fetchedRecords) {
						deserializeRecordForCollectionAndUpdateState(record);
					}

					nextShardItr = getRecordsResult.getNextShardIterator();
				}
			}
		} catch (Throwable t) {
			fetcherRef.stopWithError(t);
		}
	}

	/**
	 * The loop in run() checks this before fetching next batch of records. Since this runnable will be executed
	 * by the ExecutorService {@link KinesisDataFetcher#shardConsumersExecutor}, the only way to close down this thread
	 * would be by calling shutdownNow() on {@link KinesisDataFetcher#shardConsumersExecutor} and let the executor service
	 * interrupt all currently running {@link ShardConsumer}s.
	 */
	private boolean isRunning() {
		return !Thread.interrupted();
	}

	/**
	 * Deserializes a record for collection, and accordingly updates the shard state in the fetcher.
	 * Note that the server-side Kinesis timestamp is attached to the record when collected. When the
	 * user programs uses {@link TimeCharacteristic#EventTime}, this timestamp will be used by default.
	 *
	 * @param record
	 * @throws IOException
	 */
	private void deserializeRecordForCollectionAndUpdateState(UserRecord record)
		throws IOException {
		ByteBuffer recordData = record.getData();

		byte[] dataBytes = new byte[recordData.remaining()];
		recordData.get(dataBytes);

		final long approxArrivalTimestamp = record.getApproximateArrivalTimestamp().getTime();

		final T value = deserializer.deserialize(
			dataBytes,
			record.getPartitionKey(),
			record.getSequenceNumber(),
			approxArrivalTimestamp,
			subscribedShard.getStreamName(),
			subscribedShard.getShard().getShardId());

		if (record.isAggregated()) {
			fetcherRef.emitRecordAndUpdateState(
				value,
				approxArrivalTimestamp,
				subscribedShardStateIndex,
				new SequenceNumber(record.getSequenceNumber(), record.getSubSequenceNumber()));
		} else {
			fetcherRef.emitRecordAndUpdateState(
				value,
				approxArrivalTimestamp,
				subscribedShardStateIndex,
				new SequenceNumber(record.getSequenceNumber()));
		}
	}

	@SuppressWarnings("unchecked")
	protected static List<UserRecord> deaggregateRecords(List<Record> records, String startingHashKey, String endingHashKey) {
		return UserRecord.deaggregate(records, new BigInteger(startingHashKey), new BigInteger(endingHashKey));
	}
}
