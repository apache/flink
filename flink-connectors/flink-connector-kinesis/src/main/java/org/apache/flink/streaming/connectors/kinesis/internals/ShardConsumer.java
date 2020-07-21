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
import org.apache.flink.streaming.connectors.kinesis.internals.fanout.FanOutStreamInfo;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardConsumerMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.util.Optional.ofNullable;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.COMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thread that does the actual data pulling from AWS Kinesis shards. Each thread is in charge of one Kinesis shard only.
 */
@Internal
public class ShardConsumer<T> implements Runnable {
	private final KinesisDeserializationSchema<T> deserializer;

	private final int subscribedShardStateIndex;

	private final KinesisDataFetcher<T> fetcherRef;

	private final StreamShardHandle subscribedShard;

	private final ShardConsumerMetricsReporter shardConsumerMetricsReporter;

	/** If the record publisher type is POLL or the efo registration type is NONE, then this is null. */
	@Nullable
	private List<FanOutStreamInfo> streamInfoList;

	/** If the record publisher type is POLL or the efo registration type is NONE, then this is null. */
	@Nullable
	private KinesisProxyV2Interface kinesis;

	@Nullable
	private List<String> streams;

	private KinesisDataFetcher.FlinkKinesisProxyV2Factory kinesisProxyV2Factory;

	private StartingPosition startingPosition;

	private SequenceNumber lastSequenceNum;

	private final RecordPublisher recordPublisher;

	/**
	 * Creates a shard consumer.
	 *
	 * @param fetcherRef reference to the owning fetcher
	 * @param recordPublisher the record publisher used to read records from kinesis
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param subscribedShard the shard this consumer is subscribed to
	 * @param lastSequenceNum the sequence number in the shard to start consuming
	 * @param shardConsumerMetricsReporter the reporter to report metrics to
	 * @param shardDeserializer used to deserialize incoming records
	 */
	public ShardConsumer(
		KinesisDataFetcher<T> fetcherRef,
		RecordPublisher recordPublisher,
		Integer subscribedShardStateIndex,
		StreamShardHandle subscribedShard,
		SequenceNumber lastSequenceNum,
		ShardConsumerMetricsReporter shardConsumerMetricsReporter,
		KinesisDeserializationSchema<T> shardDeserializer) {
		this(
			fetcherRef,
			recordPublisher,
			subscribedShardStateIndex,
			subscribedShard,
			lastSequenceNum,
			shardConsumerMetricsReporter,
			shardDeserializer,
			null,
			null,
			null,
			null
		);
	}

	/**
	 * Creates a shard consumer.
	 *
	 * @param fetcherRef reference to the owning fetcher
	 * @param recordPublisher the record publisher used to read records from kinesis
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param subscribedShard the shard this consumer is subscribed to
	 * @param lastSequenceNum the sequence number in the shard to start consuming
	 * @param shardConsumerMetricsReporter the reporter to report metrics to
	 * @param shardDeserializer used to deserialize incoming records
	 * @param streams list of kinesis stream names, only needed if the record publisher type is efo, used to construct a {@link KinesisProxyV2}
	 * @param configProps the config properties, only needed if the record publisher type is efo, used to construct a {@link KinesisProxyV2}
	 * @param streamInfoList only non-null if the record publisher type is efo and efo registration type is EAGER
	 */
	public ShardConsumer(
		KinesisDataFetcher<T> fetcherRef,
		RecordPublisher recordPublisher,
		Integer subscribedShardStateIndex,
		StreamShardHandle subscribedShard,
		SequenceNumber lastSequenceNum,
		ShardConsumerMetricsReporter shardConsumerMetricsReporter,
		KinesisDeserializationSchema<T> shardDeserializer,
		@Nullable Properties configProps,
		@Nullable List<String> streams,
		@Nullable KinesisDataFetcher.FlinkKinesisProxyV2Factory kinesisProxyV2Factory,
		@Nullable List<FanOutStreamInfo> streamInfoList) {
		this.fetcherRef = checkNotNull(fetcherRef);
		this.recordPublisher = checkNotNull(recordPublisher);
		this.subscribedShardStateIndex = checkNotNull(subscribedShardStateIndex);
		this.subscribedShard = checkNotNull(subscribedShard);
		this.shardConsumerMetricsReporter = checkNotNull(shardConsumerMetricsReporter);
		this.lastSequenceNum = checkNotNull(lastSequenceNum);
		if (isEfoRecordPublisher(configProps)) {
			checkNotNull(kinesisProxyV2Factory);
			checkNotNull(configProps);
			checkNotNull(streams);
			this.streams = streams;
			this.kinesis = kinesisProxyV2Factory.create(configProps, streams);
			if (streamInfoList != null) {
				this.streamInfoList = streamInfoList;
			}
		}
		checkArgument(
			!lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get()),
			"Should not start a ShardConsumer if the shard has already been completely read.");

		this.deserializer = shardDeserializer;

		Properties consumerConfig = fetcherRef.getConsumerConfiguration();

		if (lastSequenceNum.equals(SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get())) {
			Date initTimestamp;
			String timestamp = consumerConfig.getProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP);

			try {
				String format = consumerConfig.getProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT,
					ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT);
				SimpleDateFormat customDateFormat = new SimpleDateFormat(format);
				initTimestamp = customDateFormat.parse(timestamp);
			} catch (IllegalArgumentException | NullPointerException exception) {
				throw new IllegalArgumentException(exception);
			} catch (ParseException exception) {
				initTimestamp = new Date((long) (Double.parseDouble(timestamp) * 1000));
			}

			startingPosition = StartingPosition.fromTimestamp(initTimestamp);
		} else {
			startingPosition = StartingPosition.restartFromSequenceNumber(checkNotNull(lastSequenceNum));
		}
	}

	private boolean isEfoRecordPublisher(@Nullable Properties configProps) {
		return configProps != null &&
			configProps.containsKey(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE) &&
			configProps.getProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE).equals(ConsumerConfigConstants.RecordPublisherType.EFO.toString());
	}

	@Override
	public void run() {
		try {
			while (isRunning()) {
				//register stream consumers here
				if (kinesis != null && streamInfoList == null) {
					this.streamInfoList = registerStreamConsumers();
				}
				final RecordPublisherRunResult result = recordPublisher.run(startingPosition, batch -> {
					batch.getDeaggregatedRecords()
						.stream()
						.filter(this::filterDeaggregatedRecord)
						.forEach(this::deserializeRecordForCollectionAndUpdateState);

					shardConsumerMetricsReporter.setAverageRecordSizeBytes(batch.getAverageRecordSizeBytes());
					shardConsumerMetricsReporter.setNumberOfAggregatedRecords(batch.getAggregatedRecordSize());
					shardConsumerMetricsReporter.setNumberOfDeaggregatedRecords(batch.getDeaggregatedRecordSize());
					ofNullable(batch.getMillisBehindLatest()).ifPresent(shardConsumerMetricsReporter::setMillisBehindLatest);
				});

				if (result == COMPLETE) {
					fetcherRef.updateState(subscribedShardStateIndex, SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get());

					// we can close this consumer thread once we've reached the end of the subscribed shard
					break;
				} else {
					startingPosition = StartingPosition.continueFromSequenceNumber(lastSequenceNum);
				}
			}
		} catch (Throwable t) {
			fetcherRef.stopWithError(t);
		} finally {
			if (this.streamInfoList != null) {
				try {
					deregisterStreamConsumer();
				} catch (Throwable t) {
					fetcherRef.stopWithError(t);
				}
			}
		}
	}

	private void deregisterStreamConsumer() throws ExecutionException, InterruptedException {
		checkNotNull(this.streamInfoList);
		checkNotNull(this.kinesis);
		kinesis.deregisterStreamConsumer(this.streamInfoList);
	}

	private List<FanOutStreamInfo> registerStreamConsumers() throws ExecutionException, InterruptedException {
		checkNotNull(this.streams);
		checkNotNull(this.kinesis);

		Map<String, String> streamArns = kinesis.describeStream(streams);
		return kinesis.registerStreamConsumer(streamArns);
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
	 * successfully collected sequence number in this shard consumer is also updated so that a
	 * {@link RecordPublisher} may be able to use the correct sequence number to refresh shard
	 * iterators if necessary.
	 *
	 * <p>Note that the server-side Kinesis timestamp is attached to the record when collected. When the
	 * user programs uses {@link TimeCharacteristic#EventTime}, this timestamp will be used by default.
	 *
	 * @param record record to deserialize and collect
	 */
	private void deserializeRecordForCollectionAndUpdateState(final UserRecord record) {
		ByteBuffer recordData = record.getData();

		byte[] dataBytes = new byte[recordData.remaining()];
		recordData.get(dataBytes);

		final long approxArrivalTimestamp = record.getApproximateArrivalTimestamp().getTime();

		final T value;
		try {
			value = deserializer.deserialize(
				dataBytes,
				record.getPartitionKey(),
				record.getSequenceNumber(),
				approxArrivalTimestamp,
				subscribedShard.getStreamName(),
				subscribedShard.getShard().getShardId());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

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
	 * Filters out aggregated records that have previously been processed.
	 * This method is to support restarting from a partially consumed aggregated sequence number.
	 *
	 * @param record the record to filter
	 * @return {@code true} if the record should be retained
	 */
	private boolean filterDeaggregatedRecord(final UserRecord record) {
		if (lastSequenceNum.isAggregated()) {
			return !record.getSequenceNumber().equals(lastSequenceNum.getSequenceNumber()) ||
				record.getSubSequenceNumber() > lastSequenceNum.getSubSequenceNumber();
		}

		return true;
	}
}
