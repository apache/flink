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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.isSentinelSequenceNumber;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thread that does the actual data pulling from AWS Kinesis shards. Each thread is in charge of one Kinesis shard only.
 */
@Internal
public class ShardConsumer<T> implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

	// AWS Kinesis has a read limit of 2 Mb/sec
	// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
	private static final long KINESIS_SHARD_BYTES_PER_SECOND_LIMIT = 2 * 1024L * 1024L;

	private final KinesisDeserializationSchema<T> deserializer;

	private final KinesisProxyInterface kinesis;

	private final int subscribedShardStateIndex;

	private final KinesisDataFetcher<T> fetcherRef;

	private final StreamShardHandle subscribedShard;

	private int maxNumberOfRecordsPerFetch;
	private final long fetchIntervalMillis;
	private final boolean useAdaptiveReads;

	private final ShardMetricsReporter shardMetricsReporter;

	private SequenceNumber lastSequenceNum;

	private Date initTimestamp;

	/**
	 * Creates a shard consumer.
	 *
	 * @param fetcherRef reference to the owning fetcher
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param subscribedShard the shard this consumer is subscribed to
	 * @param lastSequenceNum the sequence number in the shard to start consuming
	 * @param kinesis the proxy instance to interact with Kinesis
	 * @param shardMetricsReporter the reporter to report metrics to
	 */
	public ShardConsumer(KinesisDataFetcher<T> fetcherRef,
						Integer subscribedShardStateIndex,
						StreamShardHandle subscribedShard,
						SequenceNumber lastSequenceNum,
						KinesisProxyInterface kinesis,
						ShardMetricsReporter shardMetricsReporter) {
		this.fetcherRef = checkNotNull(fetcherRef);
		this.subscribedShardStateIndex = checkNotNull(subscribedShardStateIndex);
		this.subscribedShard = checkNotNull(subscribedShard);
		this.lastSequenceNum = checkNotNull(lastSequenceNum);

		this.shardMetricsReporter = checkNotNull(shardMetricsReporter);

		checkArgument(
			!lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get()),
			"Should not start a ShardConsumer if the shard has already been completely read.");

		this.deserializer = fetcherRef.getClonedDeserializationSchema();

		Properties consumerConfig = fetcherRef.getConsumerConfiguration();
		this.kinesis = kinesis;
		this.maxNumberOfRecordsPerFetch = Integer.valueOf(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
			Integer.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX)));
		this.fetchIntervalMillis = Long.valueOf(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
			Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS)));
		this.useAdaptiveReads = Boolean.valueOf(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS,
			Boolean.toString(ConsumerConfigConstants.DEFAULT_SHARD_USE_ADAPTIVE_READS)));

		if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get())) {
			String timestamp = consumerConfig.getProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP);

			try {
				String format = consumerConfig.getProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT,
					ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT);
				SimpleDateFormat customDateFormat = new SimpleDateFormat(format);
				this.initTimestamp = customDateFormat.parse(timestamp);
			} catch (IllegalArgumentException | NullPointerException exception) {
				throw new IllegalArgumentException(exception);
			} catch (ParseException exception) {
				this.initTimestamp = new Date((long) (Double.parseDouble(timestamp) * 1000));
			}
		} else {
			this.initTimestamp = null;
		}
	}

	/**
	 * Returns a shard iterator for the given {@link SequenceNumber}.
	 *
	 * @return shard iterator
	 * @throws Exception
	 */
	protected String getShardIterator(SequenceNumber sequenceNumber) throws Exception {

		if (isSentinelSequenceNumber(sequenceNumber)) {
			return getShardIteratorForSentinel(sequenceNumber);
		} else {
			// we will be starting from an actual sequence number (due to restore from failure).
			return getShardIteratorForRealSequenceNumber(sequenceNumber);
		}
	}

	protected String getShardIteratorForSentinel(SequenceNumber sentinelSequenceNumber) throws InterruptedException {
		String nextShardItr;

		if (sentinelSequenceNumber.equals(SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.get())) {
			// if the shard is already closed, there will be no latest next record to get for this shard
			if (subscribedShard.isClosed()) {
				nextShardItr = null;
			} else {
				nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.LATEST.toString(), null);
			}
		} else if (sentinelSequenceNumber.equals(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get())) {
			nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.TRIM_HORIZON.toString(), null);
		} else if (sentinelSequenceNumber.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get())) {
			nextShardItr = null;
		} else if (sentinelSequenceNumber.equals(SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get())) {
			nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.AT_TIMESTAMP.toString(), initTimestamp);
		} else {
			throw new RuntimeException("Unknown sentinel type: " + sentinelSequenceNumber);
		}

		return nextShardItr;
	}

	protected String getShardIteratorForRealSequenceNumber(SequenceNumber sequenceNumber)
			throws Exception {

		// if the last sequence number refers to an aggregated record, we need to clean up any dangling sub-records
		// from the last aggregated record; otherwise, we can simply start iterating from the record right after.

		if (sequenceNumber.isAggregated()) {
			return getShardIteratorForAggregatedSequenceNumber(sequenceNumber);
		} else {
			// the last record was non-aggregated, so we can simply start from the next record
			return kinesis.getShardIterator(
					subscribedShard,
					ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
					sequenceNumber.getSequenceNumber());
		}
	}

	protected String getShardIteratorForAggregatedSequenceNumber(SequenceNumber sequenceNumber)
			throws Exception {

		String itrForLastAggregatedRecord =
				kinesis.getShardIterator(
						subscribedShard,
						ShardIteratorType.AT_SEQUENCE_NUMBER.toString(),
						sequenceNumber.getSequenceNumber());

		// get only the last aggregated record
		GetRecordsResult getRecordsResult = getRecords(itrForLastAggregatedRecord, 1);

		List<UserRecord> fetchedRecords = deaggregateRecords(
				getRecordsResult.getRecords(),
				subscribedShard.getShard().getHashKeyRange().getStartingHashKey(),
				subscribedShard.getShard().getHashKeyRange().getEndingHashKey());

		long lastSubSequenceNum = sequenceNumber.getSubSequenceNumber();
		for (UserRecord record : fetchedRecords) {
			// we have found a dangling sub-record if it has a larger subsequence number
			// than our last sequence number; if so, collect the record and update state
			if (record.getSubSequenceNumber() > lastSubSequenceNum) {
				deserializeRecordForCollectionAndUpdateState(record);
			}
		}

		return getRecordsResult.getNextShardIterator();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		try {
			String nextShardItr = getShardIterator(lastSequenceNum);

			long processingStartTimeNanos = System.nanoTime();

			while (isRunning()) {
				if (nextShardItr == null) {
					fetcherRef.updateState(subscribedShardStateIndex, SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get());

					// we can close this consumer thread once we've reached the end of the subscribed shard
					break;
				} else {
					shardMetricsReporter.setMaxNumberOfRecordsPerFetch(maxNumberOfRecordsPerFetch);
					GetRecordsResult getRecordsResult = getRecords(nextShardItr, maxNumberOfRecordsPerFetch);

					List<Record> aggregatedRecords = getRecordsResult.getRecords();
					int numberOfAggregatedRecords = aggregatedRecords.size();
					shardMetricsReporter.setNumberOfAggregatedRecords(numberOfAggregatedRecords);

					// each of the Kinesis records may be aggregated, so we must deaggregate them before proceeding
					List<UserRecord> fetchedRecords = deaggregateRecords(
						aggregatedRecords,
						subscribedShard.getShard().getHashKeyRange().getStartingHashKey(),
						subscribedShard.getShard().getHashKeyRange().getEndingHashKey());

					long recordBatchSizeBytes = 0L;
					for (UserRecord record : fetchedRecords) {
						recordBatchSizeBytes += record.getData().remaining();
						deserializeRecordForCollectionAndUpdateState(record);
					}

					int numberOfDeaggregatedRecords = fetchedRecords.size();
					shardMetricsReporter.setNumberOfDeaggregatedRecords(numberOfDeaggregatedRecords);

					nextShardItr = getRecordsResult.getNextShardIterator();

					long adjustmentEndTimeNanos = adjustRunLoopFrequency(processingStartTimeNanos, System.nanoTime());
					long runLoopTimeNanos = adjustmentEndTimeNanos - processingStartTimeNanos;
					maxNumberOfRecordsPerFetch = adaptRecordsToRead(runLoopTimeNanos, fetchedRecords.size(), recordBatchSizeBytes, maxNumberOfRecordsPerFetch);
					shardMetricsReporter.setRunLoopTimeNanos(runLoopTimeNanos);
					processingStartTimeNanos = adjustmentEndTimeNanos; // for next time through the loop
				}
			}
		} catch (Throwable t) {
			fetcherRef.stopWithError(t);
		}
	}

	/**
	 * Adjusts loop timing to match target frequency if specified.
	 * @param processingStartTimeNanos The start time of the run loop "work"
	 * @param processingEndTimeNanos The end time of the run loop "work"
	 * @return The System.nanoTime() after the sleep (if any)
	 * @throws InterruptedException
	 */
	protected long adjustRunLoopFrequency(long processingStartTimeNanos, long processingEndTimeNanos)
		throws InterruptedException {
		long endTimeNanos = processingEndTimeNanos;
		if (fetchIntervalMillis != 0) {
			long processingTimeNanos = processingEndTimeNanos - processingStartTimeNanos;
			long sleepTimeMillis = fetchIntervalMillis - (processingTimeNanos / 1_000_000);
			if (sleepTimeMillis > 0) {
				Thread.sleep(sleepTimeMillis);
				endTimeNanos = System.nanoTime();
				shardMetricsReporter.setSleepTimeMillis(sleepTimeMillis);
			}
		}
		return endTimeNanos;
	}

	/**
	 * Calculates how many records to read each time through the loop based on a target throughput
	 * and the measured frequenecy of the loop.
	 * @param runLoopTimeNanos The total time of one pass through the loop
	 * @param numRecords The number of records of the last read operation
	 * @param recordBatchSizeBytes The total batch size of the last read operation
	 * @param maxNumberOfRecordsPerFetch The current maxNumberOfRecordsPerFetch
	 */
	private int adaptRecordsToRead(long runLoopTimeNanos, int numRecords, long recordBatchSizeBytes,
			int maxNumberOfRecordsPerFetch) {
		if (useAdaptiveReads && numRecords != 0 && runLoopTimeNanos != 0) {
			long averageRecordSizeBytes = recordBatchSizeBytes / numRecords;
			// Adjust number of records to fetch from the shard depending on current average record size
			// to optimize 2 Mb / sec read limits
			double loopFrequencyHz = 1000000000.0d / runLoopTimeNanos;
			double bytesPerRead = KINESIS_SHARD_BYTES_PER_SECOND_LIMIT / loopFrequencyHz;
			maxNumberOfRecordsPerFetch = (int) (bytesPerRead / averageRecordSizeBytes);
			// Ensure the value is greater than 0 and not more than 10000L
			maxNumberOfRecordsPerFetch = Math.max(1, Math.min(maxNumberOfRecordsPerFetch, ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX));

			// Set metrics
			shardMetricsReporter.setAverageRecordSizeBytes(averageRecordSizeBytes);
			shardMetricsReporter.setLoopFrequencyHz(loopFrequencyHz);
			shardMetricsReporter.setBytesPerRead(bytesPerRead);
		}
		return maxNumberOfRecordsPerFetch;
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
	 * Deserializes a record for collection, and accordingly updates the shard state in the fetcher. The last
	 * successfully collected sequence number in this shard consumer is also updated so that
	 * {@link ShardConsumer#getRecords(String, int)} may be able to use the correct sequence number to refresh shard
	 * iterators if necessary.
	 *
	 * <p>Note that the server-side Kinesis timestamp is attached to the record when collected. When the
	 * user programs uses {@link TimeCharacteristic#EventTime}, this timestamp will be used by default.
	 *
	 * @param record record to deserialize and collect
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

		SequenceNumber collectedSequenceNumber = (record.isAggregated())
			? new SequenceNumber(record.getSequenceNumber(), record.getSubSequenceNumber())
			: new SequenceNumber(record.getSequenceNumber());

		fetcherRef.emitRecordAndUpdateState(
			value,
			approxArrivalTimestamp,
			subscribedShardStateIndex,
			collectedSequenceNumber);

		lastSequenceNum = collectedSequenceNumber;
	}

	/**
	 * Calls {@link KinesisProxyInterface#getRecords(String, int)}, while also handling unexpected
	 * AWS {@link ExpiredIteratorException}s to assure that we get results and don't just fail on
	 * such occasions. The returned shard iterator within the successful {@link GetRecordsResult} should
	 * be used for the next call to this method.
	 *
	 * <p>Note: it is important that this method is not called again before all the records from the last result have been
	 * fully collected with {@link ShardConsumer#deserializeRecordForCollectionAndUpdateState(UserRecord)}, otherwise
	 * {@link ShardConsumer#lastSequenceNum} may refer to a sub-record in the middle of an aggregated record, leading to
	 * incorrect shard iteration if the iterator had to be refreshed.
	 *
	 * @param shardItr shard iterator to use
	 * @param maxNumberOfRecords the maximum number of records to fetch for this getRecords attempt
	 * @return get records result
	 * @throws InterruptedException
	 */
	private GetRecordsResult getRecords(String shardItr, int maxNumberOfRecords) throws Exception {
		GetRecordsResult getRecordsResult = null;
		while (getRecordsResult == null) {
			try {
				getRecordsResult = kinesis.getRecords(shardItr, maxNumberOfRecords);

				// Update millis behind latest so it gets reported by the millisBehindLatest gauge
				Long millisBehindLatest = getRecordsResult.getMillisBehindLatest();
				if (millisBehindLatest != null) {
					shardMetricsReporter.setMillisBehindLatest(millisBehindLatest);
				}
			} catch (ExpiredIteratorException eiEx) {
				LOG.warn("Encountered an unexpected expired iterator {} for shard {};" +
					" refreshing the iterator ...", shardItr, subscribedShard);

				shardItr = getShardIterator(lastSequenceNum);

				// sleep for the fetch interval before the next getRecords attempt with the refreshed iterator
				if (fetchIntervalMillis != 0) {
					Thread.sleep(fetchIntervalMillis);
				}
			}
		}
		return getRecordsResult;
	}

	@SuppressWarnings("unchecked")
	protected static List<UserRecord> deaggregateRecords(List<Record> records, String startingHashKey, String endingHashKey) {
		return UserRecord.deaggregate(records, new BigInteger(startingHashKey), new BigInteger(endingHashKey));
	}
}
