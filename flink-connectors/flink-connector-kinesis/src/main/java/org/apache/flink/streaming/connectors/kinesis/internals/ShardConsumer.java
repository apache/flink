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
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardConsumerMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Optional.ofNullable;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.COMPLETE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thread that subscribes to the given {@link RecordPublisher}. Each thread is in charge of one
 * Kinesis shard only.
 *
 * <p>A {@link ShardConsumer} is responsible for:
 *
 * <ul>
 *   <li>Running the {@link RecordPublisher} to consume all records from the subscribed shard
 *   <li>Deserializing and deaggregating incoming records from Kinesis
 *   <li>Logging metrics
 *   <li>Passing the records up to the {@link KinesisDataFetcher}
 * </ul>
 */
@Internal
public class ShardConsumer<T> implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

    private final KinesisDeserializationSchema<T> deserializer;

    private final int subscribedShardStateIndex;

    private final KinesisDataFetcher<T> fetcherRef;

    private final StreamShardHandle subscribedShard;

    private final ShardConsumerMetricsReporter shardConsumerMetricsReporter;

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
        this.fetcherRef = checkNotNull(fetcherRef);
        this.recordPublisher = checkNotNull(recordPublisher);
        this.subscribedShardStateIndex = checkNotNull(subscribedShardStateIndex);
        this.subscribedShard = checkNotNull(subscribedShard);
        this.shardConsumerMetricsReporter = checkNotNull(shardConsumerMetricsReporter);
        this.lastSequenceNum = checkNotNull(lastSequenceNum);

        checkArgument(
                !lastSequenceNum.equals(
                        SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get()),
                "Should not start a ShardConsumer if the shard has already been completely read.");

        this.deserializer = shardDeserializer;
    }

    @Override
    public void run() {
        try {
            while (isRunning()) {
                final RecordPublisherRunResult result =
                        recordPublisher.run(
                                batch -> {
                                    if (!batch.getDeaggregatedRecords().isEmpty()) {
                                        LOG.debug(
                                                "stream: {}, shard: {}, millis behind latest: {}, batch size: {}",
                                                subscribedShard.getStreamName(),
                                                subscribedShard.getShard().getShardId(),
                                                batch.getMillisBehindLatest(),
                                                batch.getDeaggregatedRecordSize());
                                    }
                                    for (UserRecord userRecord : batch.getDeaggregatedRecords()) {
                                        if (filterDeaggregatedRecord(userRecord)) {
                                            deserializeRecordForCollectionAndUpdateState(
                                                    userRecord);
                                        }
                                    }

                                    shardConsumerMetricsReporter.setAverageRecordSizeBytes(
                                            batch.getAverageRecordSizeBytes());
                                    shardConsumerMetricsReporter.setNumberOfAggregatedRecords(
                                            batch.getAggregatedRecordSize());
                                    shardConsumerMetricsReporter.setNumberOfDeaggregatedRecords(
                                            batch.getDeaggregatedRecordSize());
                                    ofNullable(batch.getMillisBehindLatest())
                                            .ifPresent(
                                                    shardConsumerMetricsReporter
                                                            ::setMillisBehindLatest);

                                    return lastSequenceNum;
                                });

                if (result == COMPLETE) {
                    fetcherRef.updateState(
                            subscribedShardStateIndex,
                            SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get());
                    // we can close this consumer thread once we've reached the end of the
                    // subscribed shard
                    break;
                }
            }
        } catch (Throwable t) {
            fetcherRef.stopWithError(t);
        }
    }

    /**
     * The loop in run() checks this before fetching next batch of records. Since this runnable will
     * be executed by the ExecutorService {@code KinesisDataFetcher#shardConsumersExecutor}, the
     * only way to close down this thread would be by calling shutdownNow() on {@code
     * KinesisDataFetcher#shardConsumersExecutor} and let the executor service interrupt all
     * currently running {@link ShardConsumer}s.
     */
    private boolean isRunning() {
        return !Thread.interrupted();
    }

    /**
     * Deserializes a record for collection, and accordingly updates the shard state in the fetcher.
     * The last successfully collected sequence number in this shard consumer is also updated so
     * that a {@link RecordPublisher} may be able to use the correct sequence number to refresh
     * shard iterators if necessary.
     *
     * <p>Note that the server-side Kinesis timestamp is attached to the record when collected. When
     * the user programs uses {@link TimeCharacteristic#EventTime}, this timestamp will be used by
     * default.
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
            value =
                    deserializer.deserialize(
                            dataBytes,
                            record.getPartitionKey(),
                            record.getSequenceNumber(),
                            approxArrivalTimestamp,
                            subscribedShard.getStreamName(),
                            subscribedShard.getShard().getShardId());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SequenceNumber collectedSequenceNumber =
                (record.isAggregated())
                        ? new SequenceNumber(
                                record.getSequenceNumber(), record.getSubSequenceNumber())
                        : new SequenceNumber(record.getSequenceNumber());

        fetcherRef.emitRecordAndUpdateState(
                value, approxArrivalTimestamp, subscribedShardStateIndex, collectedSequenceNumber);

        this.lastSequenceNum = collectedSequenceNumber;
    }

    /**
     * Filters out aggregated records that have previously been processed. This method is to support
     * restarting from a partially consumed aggregated sequence number.
     *
     * @param record the record to filter
     * @return true if the record should be retained
     */
    private boolean filterDeaggregatedRecord(final UserRecord record) {
        if (!lastSequenceNum.isAggregated()) {
            return true;
        }

        return !record.getSequenceNumber().equals(lastSequenceNum.getSequenceNumber())
                || record.getSubSequenceNumber() > lastSequenceNum.getSubSequenceNumber();
    }
}
