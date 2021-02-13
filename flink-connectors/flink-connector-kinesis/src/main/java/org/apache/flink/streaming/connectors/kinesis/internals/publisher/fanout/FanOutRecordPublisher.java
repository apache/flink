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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutShardSubscriber.FanOutSubscriberException;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutShardSubscriber.RecoverableFanOutSubscriberException;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.FullJitterBackoff;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.COMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.INCOMPLETE;
import static software.amazon.awssdk.services.kinesis.model.StartingPosition.builder;

/**
 * A {@link RecordPublisher} that will read and forward records from Kinesis using EFO, to the
 * subscriber. Records are consumed via Enhanced Fan Out subscriptions using SubscribeToShard API.
 */
@Internal
public class FanOutRecordPublisher implements RecordPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(FanOutRecordPublisher.class);

    private final FullJitterBackoff backoff;

    private final String consumerArn;

    private final KinesisProxyV2Interface kinesisProxy;

    private final StreamShardHandle subscribedShard;

    private final FanOutRecordPublisherConfiguration configuration;

    /** The current attempt in the case of subsequent recoverable errors. */
    private int attempt = 0;

    private StartingPosition nextStartingPosition;

    /**
     * Instantiate a new FanOutRecordPublisher. Consumes data from KDS using EFO SubscribeToShard
     * over AWS SDK V2.x
     *
     * @param startingPosition the position in the shard to start consuming from
     * @param consumerArn the consumer ARN of the stream consumer
     * @param subscribedShard the shard to consumer from
     * @param kinesisProxy the proxy used to talk to Kinesis services
     * @param configuration the record publisher configuration
     */
    public FanOutRecordPublisher(
            final StartingPosition startingPosition,
            final String consumerArn,
            final StreamShardHandle subscribedShard,
            final KinesisProxyV2Interface kinesisProxy,
            final FanOutRecordPublisherConfiguration configuration,
            final FullJitterBackoff backoff) {
        this.nextStartingPosition = Preconditions.checkNotNull(startingPosition);
        this.consumerArn = Preconditions.checkNotNull(consumerArn);
        this.subscribedShard = Preconditions.checkNotNull(subscribedShard);
        this.kinesisProxy = Preconditions.checkNotNull(kinesisProxy);
        this.configuration = Preconditions.checkNotNull(configuration);
        this.backoff = Preconditions.checkNotNull(backoff);
    }

    @Override
    public RecordPublisherRunResult run(final RecordBatchConsumer recordConsumer)
            throws InterruptedException {
        LOG.info(
                "Running fan out record publisher on {}::{} from {} - {}",
                subscribedShard.getStreamName(),
                subscribedShard.getShard().getShardId(),
                nextStartingPosition.getShardIteratorType(),
                nextStartingPosition.getStartingMarker());

        Consumer<SubscribeToShardEvent> eventConsumer =
                event -> {
                    RecordBatch recordBatch =
                            new RecordBatch(
                                    toSdkV1Records(event.records()),
                                    subscribedShard,
                                    event.millisBehindLatest());
                    SequenceNumber sequenceNumber = recordConsumer.accept(recordBatch);
                    nextStartingPosition =
                            StartingPosition.continueFromSequenceNumber(sequenceNumber);
                };

        RecordPublisherRunResult result = runWithBackoff(eventConsumer);

        LOG.info(
                "Subscription expired {}::{}, with status {}",
                subscribedShard.getStreamName(),
                subscribedShard.getShard().getShardId(),
                result);

        return result;
    }

    /**
     * Runs the record publisher, will sleep for configuration computed jitter period in the case of
     * certain exceptions. Unrecoverable exceptions are thrown to terminate the application.
     *
     * @param eventConsumer the consumer to pass events to
     * @return {@code COMPLETE} if the shard is complete and this shard consumer should exit
     * @throws InterruptedException
     */
    private RecordPublisherRunResult runWithBackoff(
            final Consumer<SubscribeToShardEvent> eventConsumer) throws InterruptedException {
        FanOutShardSubscriber fanOutShardSubscriber =
                new FanOutShardSubscriber(
                        consumerArn, subscribedShard.getShard().getShardId(), kinesisProxy);
        boolean complete;

        try {
            complete =
                    fanOutShardSubscriber.subscribeToShardAndConsumeRecords(
                            toSdkV2StartingPosition(nextStartingPosition), eventConsumer);
            attempt = 0;
        } catch (RecoverableFanOutSubscriberException ex) {
            // Recoverable errors should be reattempted without contributing to the retry policy
            // A recoverable error would not result in the Flink job being cancelled
            backoff(ex);
            return INCOMPLETE;
        } catch (FanOutSubscriberException ex) {
            // We have received an error from the network layer
            // This can be due to limits being exceeded, network timeouts, etc
            // We should backoff, reacquire a subscription and try again
            if (ex.getCause() instanceof ResourceNotFoundException) {
                LOG.warn(
                        "Received ResourceNotFoundException. Either the shard does not exist, or the stream subscriber has been deregistered."
                                + "Marking this shard as complete {} ({})",
                        subscribedShard.getShard().getShardId(),
                        consumerArn);

                return COMPLETE;
            }

            if (attempt == configuration.getSubscribeToShardMaxRetries()) {
                final String errorMessage =
                        "Maximum retries exceeded for SubscribeToShard. "
                                + "Failed "
                                + configuration.getSubscribeToShardMaxRetries()
                                + " times.";
                LOG.error(errorMessage, ex.getCause());
                throw new RuntimeException(errorMessage, ex.getCause());
            }

            attempt++;
            backoff(ex);
            return INCOMPLETE;
        }

        return complete ? COMPLETE : INCOMPLETE;
    }

    private void backoff(final Throwable ex) throws InterruptedException {
        long backoffMillis =
                backoff.calculateFullJitterBackoff(
                        configuration.getSubscribeToShardBaseBackoffMillis(),
                        configuration.getSubscribeToShardMaxBackoffMillis(),
                        configuration.getSubscribeToShardExpConstant(),
                        attempt);

        LOG.warn(
                "Encountered recoverable error {}. Backing off for {} millis {} ({})",
                ex.getCause().getClass().getSimpleName(),
                backoffMillis,
                subscribedShard.getShard().getShardId(),
                consumerArn,
                ex);

        backoff.sleep(backoffMillis);
    }

    /**
     * Records that come from KPL may be aggregated. Records must be deaggregated before they are
     * processed by the application. Deaggregation is performed by KCL. In order to prevent having
     * to import KCL 1.x and 2.x we convert the records to v1 format and use KCL v1.
     *
     * @param records the SDK v2 records
     * @return records converted to SDK v1 format
     */
    private List<com.amazonaws.services.kinesis.model.Record> toSdkV1Records(
            final List<Record> records) {
        final List<com.amazonaws.services.kinesis.model.Record> sdkV1Records = new ArrayList<>();

        for (Record record : records) {
            sdkV1Records.add(toSdkV1Record(record));
        }

        return sdkV1Records;
    }

    private com.amazonaws.services.kinesis.model.Record toSdkV1Record(
            @Nonnull final Record record) {
        final com.amazonaws.services.kinesis.model.Record recordV1 =
                new com.amazonaws.services.kinesis.model.Record()
                        .withData(record.data().asByteBuffer())
                        .withSequenceNumber(record.sequenceNumber())
                        .withPartitionKey(record.partitionKey())
                        .withApproximateArrivalTimestamp(
                                new Date(record.approximateArrivalTimestamp().toEpochMilli()));

        EncryptionType encryptionType = record.encryptionType();
        if (encryptionType != null) {
            recordV1.withEncryptionType(encryptionType.name());
        }

        return recordV1;
    }

    /**
     * Converts a local {@link StartingPosition} to an AWS SDK V2 object representation.
     *
     * @param startingPosition the local {@link StartingPosition}
     * @return an AWS SDK V2 representation
     */
    private software.amazon.awssdk.services.kinesis.model.StartingPosition toSdkV2StartingPosition(
            StartingPosition startingPosition) {
        software.amazon.awssdk.services.kinesis.model.StartingPosition.Builder builder =
                builder().type(startingPosition.getShardIteratorType().toString());

        Object marker = startingPosition.getStartingMarker();

        switch (startingPosition.getShardIteratorType()) {
            case AT_TIMESTAMP:
                {
                    Preconditions.checkNotNull(
                            marker, "StartingPosition AT_TIMESTAMP date marker is null.");
                    builder.timestamp(((Date) marker).toInstant());
                    break;
                }
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                {
                    Preconditions.checkNotNull(
                            marker, "StartingPosition *_SEQUENCE_NUMBER position is null.");
                    builder.sequenceNumber(marker.toString());
                    break;
                }
        }

        return builder.build();
    }
}
