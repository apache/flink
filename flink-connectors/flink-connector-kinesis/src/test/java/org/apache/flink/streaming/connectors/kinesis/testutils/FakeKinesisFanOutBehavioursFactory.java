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

import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;

import com.amazonaws.kinesis.agg.RecordAggregator;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.ConsumerDescription;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static software.amazon.awssdk.services.kinesis.model.ConsumerStatus.ACTIVE;
import static software.amazon.awssdk.services.kinesis.model.ConsumerStatus.CREATING;
import static software.amazon.awssdk.services.kinesis.model.ConsumerStatus.DELETING;

/**
 * Factory for different kinds of fake Kinesis behaviours using the {@link KinesisProxyV2Interface}
 * interface.
 */
public class FakeKinesisFanOutBehavioursFactory {

    public static final String STREAM_ARN = "stream-arn";
    public static final String STREAM_CONSUMER_ARN_EXISTING = "stream-consumer-arn";
    public static final String STREAM_CONSUMER_ARN_NEW = "stream-consumer-arn-new";

    // ------------------------------------------------------------------------
    //  Behaviours related to subscribe to shard and consuming data
    // ------------------------------------------------------------------------

    public static SingleShardFanOutKinesisV2.Builder boundedShard() {
        return new SingleShardFanOutKinesisV2.Builder();
    }

    public static KinesisProxyV2Interface singletonShard(final SubscribeToShardEvent event) {
        return new SingletonEventFanOutKinesisV2(event);
    }

    public static SingleShardFanOutKinesisV2 emptyShard() {
        return new SingleShardFanOutKinesisV2.Builder().withBatchCount(0).build();
    }

    public static KinesisProxyV2Interface resourceNotFoundWhenObtainingSubscription() {
        return new ExceptionalKinesisV2(ResourceNotFoundException.builder().build());
    }

    public static SubscriptionErrorKinesisV2 errorDuringSubscription(
            final Throwable... throwables) {
        return new SubscriptionErrorKinesisV2(throwables);
    }

    public static SubscriptionErrorKinesisV2 alternatingSuccessErrorDuringSubscription() {
        return new AlternatingSubscriptionErrorKinesisV2(LimitExceededException.builder().build());
    }

    public static KinesisProxyV2Interface failsToAcquireSubscription() {
        return new FailsToAcquireSubscriptionKinesis();
    }

    // ------------------------------------------------------------------------
    //  Behaviours related to describing streams
    // ------------------------------------------------------------------------

    public static KinesisProxyV2Interface streamNotFound() {
        return new StreamConsumerFakeKinesis.Builder()
                .withThrowsWhileDescribingStream(ResourceNotFoundException.builder().build())
                .build();
    }

    // ------------------------------------------------------------------------
    //  Behaviours related to stream consumer registration/deregistration
    // ------------------------------------------------------------------------

    public static StreamConsumerFakeKinesis streamConsumerNotFound() {
        return new StreamConsumerFakeKinesis.Builder().withStreamConsumerNotFound(true).build();
    }

    public static StreamConsumerFakeKinesis existingActiveConsumer() {
        return new StreamConsumerFakeKinesis.Builder().build();
    }

    public static StreamConsumerFakeKinesis registerExistingConsumerAndWaitToBecomeActive() {
        return new StreamConsumerFakeKinesis.Builder().withStreamConsumerStatus(CREATING).build();
    }

    /** A dummy EFO implementation that fails to acquire subscription (no response). */
    private static class FailsToAcquireSubscriptionKinesis extends KinesisProxyV2InterfaceAdapter {

        @Override
        public CompletableFuture<Void> subscribeToShard(
                final SubscribeToShardRequest request,
                final SubscribeToShardResponseHandler responseHandler) {

            return CompletableFuture.supplyAsync(() -> null);
        }
    }

    public static AbstractSingleShardFanOutKinesisV2 emptyBatchFollowedBySingleRecord() {
        return new AbstractSingleShardFanOutKinesisV2(2) {
            private int subscription = 0;

            @Override
            void sendEvents(Subscriber<? super SubscribeToShardEventStream> subscriber) {
                SubscribeToShardEvent.Builder builder =
                        SubscribeToShardEvent.builder()
                                .continuationSequenceNumber(subscription == 0 ? "1" : null);

                if (subscription == 1) {
                    builder.records(createRecord(new AtomicInteger(1)));
                }

                subscriber.onNext(builder.build());
                subscription++;
            }
        };
    }

    /**
     * An unbounded fake Kinesis that offers subscriptions with 5 records, alternating throwing the
     * given exception. The first subscription is exceptional, second successful, and so on.
     */
    private static class AlternatingSubscriptionErrorKinesisV2 extends SubscriptionErrorKinesisV2 {

        int index = 0;

        private AlternatingSubscriptionErrorKinesisV2(final Throwable throwable) {
            super(throwable);
        }

        @Override
        void sendEvents(Subscriber<? super SubscribeToShardEventStream> subscriber) {
            if (index % 2 == 0) {
                super.sendEvents(subscriber);
            } else {
                super.sendEventBatch(subscriber);
                subscriber.onComplete();
            }

            index++;
        }
    }

    /**
     * A fake Kinesis that throws the given exception after sending 5 records. A total of 5
     * subscriptions can be acquired.
     */
    public static class SubscriptionErrorKinesisV2 extends AbstractSingleShardFanOutKinesisV2 {

        public static final int NUMBER_OF_SUBSCRIPTIONS = 5;

        public static final int NUMBER_OF_EVENTS_PER_SUBSCRIPTION = 5;

        private final Throwable[] throwables;

        AtomicInteger sequenceNumber = new AtomicInteger();

        private SubscriptionErrorKinesisV2(final Throwable... throwables) {
            super(NUMBER_OF_SUBSCRIPTIONS);
            this.throwables = throwables;
        }

        @Override
        void sendEvents(Subscriber<? super SubscribeToShardEventStream> subscriber) {
            sendEventBatch(subscriber);
            try {
                // Add an artificial delay to allow records to flush
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (Throwable throwable : throwables) {
                subscriber.onError(throwable);
            }
        }

        void sendEventBatch(Subscriber<? super SubscribeToShardEventStream> subscriber) {
            for (int i = 0; i < NUMBER_OF_EVENTS_PER_SUBSCRIPTION; i++) {
                subscriber.onNext(
                        SubscribeToShardEvent.builder()
                                .records(createRecord(sequenceNumber))
                                .continuationSequenceNumber(String.valueOf(i))
                                .build());
            }
        }
    }

    private static class ExceptionalKinesisV2 extends KinesisProxyV2InterfaceAdapter {

        private final RuntimeException exception;

        private ExceptionalKinesisV2(RuntimeException exception) {
            this.exception = exception;
        }

        @Override
        public CompletableFuture<Void> subscribeToShard(
                SubscribeToShardRequest request, SubscribeToShardResponseHandler responseHandler) {
            responseHandler.exceptionOccurred(exception);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class SingletonEventFanOutKinesisV2 extends AbstractSingleShardFanOutKinesisV2 {

        private final SubscribeToShardEvent event;

        private SingletonEventFanOutKinesisV2(SubscribeToShardEvent event) {
            super(1);
            this.event = event;
        }

        @Override
        void sendEvents(Subscriber<? super SubscribeToShardEventStream> subscriber) {
            subscriber.onNext(event);
        }
    }

    /**
     * A fake implementation of KinesisProxyV2 SubscribeToShard that provides dummy records for EFO
     * subscriptions. Aggregated and non-aggregated records are supported with various batch and
     * subscription sizes.
     */
    public static class SingleShardFanOutKinesisV2 extends AbstractSingleShardFanOutKinesisV2 {

        private final int batchesPerSubscription;

        private final int recordsPerBatch;

        private final long millisBehindLatest;

        private final int totalRecords;

        private final int aggregationFactor;

        private final AtomicInteger sequenceNumber = new AtomicInteger();

        private SingleShardFanOutKinesisV2(final Builder builder) {
            super(builder.getSubscriptionCount());
            this.batchesPerSubscription = builder.batchesPerSubscription;
            this.recordsPerBatch = builder.recordsPerBatch;
            this.millisBehindLatest = builder.millisBehindLatest;
            this.aggregationFactor = builder.aggregationFactor;
            this.totalRecords = builder.getTotalRecords();
        }

        @Override
        void sendEvents(final Subscriber<? super SubscribeToShardEventStream> subscriber) {
            SubscribeToShardEvent.Builder eventBuilder =
                    SubscribeToShardEvent.builder().millisBehindLatest(millisBehindLatest);

            for (int batchIndex = 0;
                    batchIndex < batchesPerSubscription && sequenceNumber.get() < totalRecords;
                    batchIndex++) {
                List<Record> records = new ArrayList<>();

                for (int i = 0; i < recordsPerBatch; i++) {
                    final Record record;

                    if (aggregationFactor == 1) {
                        record = createRecord(sequenceNumber);
                    } else {
                        record = createAggregatedRecord(aggregationFactor, sequenceNumber);
                    }

                    records.add(record);
                }

                eventBuilder.records(records);

                String continuation =
                        sequenceNumber.get() < totalRecords
                                ? String.valueOf(sequenceNumber.get() + 1)
                                : null;
                eventBuilder.continuationSequenceNumber(continuation);

                subscriber.onNext(eventBuilder.build());
            }
        }

        /** A convenience builder for {@link SingleShardFanOutKinesisV2}. */
        public static class Builder {
            private int batchesPerSubscription = 100000;
            private int recordsPerBatch = 10;
            private long millisBehindLatest = 0;
            private int batchCount = 1;
            private int aggregationFactor = 1;

            public int getSubscriptionCount() {
                return (int)
                        Math.ceil(
                                (double) getTotalRecords()
                                        / batchesPerSubscription
                                        / recordsPerBatch);
            }

            public int getTotalRecords() {
                return batchCount * recordsPerBatch;
            }

            public Builder withBatchesPerSubscription(final int batchesPerSubscription) {
                this.batchesPerSubscription = batchesPerSubscription;
                return this;
            }

            public Builder withRecordsPerBatch(final int recordsPerBatch) {
                this.recordsPerBatch = recordsPerBatch;
                return this;
            }

            public Builder withBatchCount(final int batchCount) {
                this.batchCount = batchCount;
                return this;
            }

            public Builder withMillisBehindLatest(final long millisBehindLatest) {
                this.millisBehindLatest = millisBehindLatest;
                return this;
            }

            public Builder withAggregationFactor(final int aggregationFactor) {
                this.aggregationFactor = aggregationFactor;
                return this;
            }

            public SingleShardFanOutKinesisV2 build() {
                return new SingleShardFanOutKinesisV2(this);
            }
        }
    }

    /**
     * A single shard dummy EFO implementation that provides basic responses and subscription
     * management. Does not provide any records.
     */
    public abstract static class AbstractSingleShardFanOutKinesisV2
            extends KinesisProxyV2InterfaceAdapter {

        private final List<SubscribeToShardRequest> requests = new ArrayList<>();
        private int remainingSubscriptions;

        private AbstractSingleShardFanOutKinesisV2(final int remainingSubscriptions) {
            this.remainingSubscriptions = remainingSubscriptions;
        }

        public int getNumberOfSubscribeToShardInvocations() {
            return requests.size();
        }

        public StartingPosition getStartingPositionForSubscription(final int subscriptionIndex) {
            assertTrue(subscriptionIndex >= 0);
            assertTrue(subscriptionIndex < getNumberOfSubscribeToShardInvocations());

            return requests.get(subscriptionIndex).startingPosition();
        }

        @Override
        public CompletableFuture<Void> subscribeToShard(
                final SubscribeToShardRequest request,
                final SubscribeToShardResponseHandler responseHandler) {

            requests.add(request);

            return CompletableFuture.supplyAsync(
                    () -> {
                        responseHandler.responseReceived(
                                SubscribeToShardResponse.builder().build());

                        responseHandler.onEventStream(
                                subscriber -> {
                                    subscriber.onSubscribe(mock(Subscription.class));

                                    if (remainingSubscriptions > 0) {
                                        sendEvents(subscriber);
                                        remainingSubscriptions--;
                                    } else {
                                        SubscribeToShardEvent.Builder eventBuilder =
                                                SubscribeToShardEvent.builder()
                                                        .millisBehindLatest(0L)
                                                        .continuationSequenceNumber(null);

                                        subscriber.onNext(eventBuilder.build());
                                    }

                                    subscriber.onComplete();
                                });

                        return null;
                    });
        }

        abstract void sendEvents(final Subscriber<? super SubscribeToShardEventStream> subscriber);
    }

    /** A fake Kinesis Proxy V2 that implements dummy logic for stream consumer related methods. */
    public static class StreamConsumerFakeKinesis extends KinesisProxyV2InterfaceAdapter {

        public static final int NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE = 5;
        public static final int NUMBER_OF_DESCRIBE_REQUESTS_TO_DELETE = 5;

        private final RuntimeException throwsWhileDescribingStream;
        private String streamConsumerArn = STREAM_CONSUMER_ARN_EXISTING;
        private ConsumerStatus streamConsumerStatus;
        private boolean streamConsumerNotFound;
        private int numberOfDescribeStreamConsumerInvocations = 0;

        private StreamConsumerFakeKinesis(final Builder builder) {
            this.throwsWhileDescribingStream = builder.throwsWhileDescribingStream;
            this.streamConsumerStatus = builder.streamConsumerStatus;
            this.streamConsumerNotFound = builder.streamConsumerNotFound;
        }

        public int getNumberOfDescribeStreamConsumerInvocations() {
            return numberOfDescribeStreamConsumerInvocations;
        }

        @Override
        public DescribeStreamSummaryResponse describeStreamSummary(String stream)
                throws InterruptedException, ExecutionException {
            if (throwsWhileDescribingStream != null) {
                throw throwsWhileDescribingStream;
            }

            return DescribeStreamSummaryResponse.builder()
                    .streamDescriptionSummary(
                            StreamDescriptionSummary.builder().streamARN(STREAM_ARN).build())
                    .build();
        }

        @Override
        public RegisterStreamConsumerResponse registerStreamConsumer(
                String streamArn, String consumerName)
                throws InterruptedException, ExecutionException {
            assertEquals(STREAM_ARN, streamArn);

            streamConsumerNotFound = false;
            streamConsumerArn = STREAM_CONSUMER_ARN_NEW;

            return RegisterStreamConsumerResponse.builder()
                    .consumer(
                            Consumer.builder()
                                    .consumerARN(STREAM_CONSUMER_ARN_NEW)
                                    .consumerStatus(streamConsumerStatus)
                                    .build())
                    .build();
        }

        @Override
        public DeregisterStreamConsumerResponse deregisterStreamConsumer(final String consumerArn)
                throws InterruptedException, ExecutionException {
            streamConsumerStatus = DELETING;
            return DeregisterStreamConsumerResponse.builder().build();
        }

        @Override
        public DescribeStreamConsumerResponse describeStreamConsumer(
                final String streamArn, final String consumerName)
                throws InterruptedException, ExecutionException {
            assertEquals(STREAM_ARN, streamArn);

            numberOfDescribeStreamConsumerInvocations++;

            if (streamConsumerStatus == DELETING
                    && numberOfDescribeStreamConsumerInvocations
                            == NUMBER_OF_DESCRIBE_REQUESTS_TO_DELETE) {
                streamConsumerNotFound = true;
            } else if (numberOfDescribeStreamConsumerInvocations
                    == NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE) {
                streamConsumerStatus = ACTIVE;
            }

            if (streamConsumerNotFound) {
                throw new ExecutionException(ResourceNotFoundException.builder().build());
            }

            return DescribeStreamConsumerResponse.builder()
                    .consumerDescription(
                            ConsumerDescription.builder()
                                    .consumerARN(streamConsumerArn)
                                    .consumerName(consumerName)
                                    .consumerStatus(streamConsumerStatus)
                                    .build())
                    .build();
        }

        @Override
        public DescribeStreamConsumerResponse describeStreamConsumer(String streamConsumerArn)
                throws InterruptedException, ExecutionException {
            assertEquals(this.streamConsumerArn, streamConsumerArn);
            return describeStreamConsumer(STREAM_ARN, "consumer-name");
        }

        private static class Builder {

            private RuntimeException throwsWhileDescribingStream;
            private ConsumerStatus streamConsumerStatus = ACTIVE;
            private boolean streamConsumerNotFound = false;

            public StreamConsumerFakeKinesis build() {
                return new StreamConsumerFakeKinesis(this);
            }

            public Builder withStreamConsumerNotFound(final boolean streamConsumerNotFound) {
                this.streamConsumerNotFound = streamConsumerNotFound;
                return this;
            }

            public Builder withThrowsWhileDescribingStream(
                    final RuntimeException throwsWhileDescribingStream) {
                this.throwsWhileDescribingStream = throwsWhileDescribingStream;
                return this;
            }

            public Builder withStreamConsumerStatus(final ConsumerStatus streamConsumerStatus) {
                this.streamConsumerStatus = streamConsumerStatus;
                return this;
            }
        }
    }

    private static class KinesisProxyV2InterfaceAdapter implements KinesisProxyV2Interface {

        @Override
        public DescribeStreamSummaryResponse describeStreamSummary(String stream)
                throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("This method is not implemented.");
        }

        @Override
        public DescribeStreamConsumerResponse describeStreamConsumer(String streamConsumerArn)
                throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("This method is not implemented.");
        }

        @Override
        public DescribeStreamConsumerResponse describeStreamConsumer(
                String streamArn, String consumerName)
                throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("This method is not implemented.");
        }

        @Override
        public RegisterStreamConsumerResponse registerStreamConsumer(
                String streamArn, String consumerName)
                throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("This method is not implemented.");
        }

        @Override
        public DeregisterStreamConsumerResponse deregisterStreamConsumer(String consumerArn)
                throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("This method is not implemented.");
        }

        @Override
        public CompletableFuture<Void> subscribeToShard(
                SubscribeToShardRequest request, SubscribeToShardResponseHandler responseHandler) {
            throw new UnsupportedOperationException("This method is not implemented.");
        }
    }

    private static Record createRecord(final AtomicInteger sequenceNumber) {
        return createRecord(randomAlphabetic(32).getBytes(UTF_8), sequenceNumber);
    }

    private static Record createRecord(final byte[] data, final AtomicInteger sequenceNumber) {
        return Record.builder()
                .approximateArrivalTimestamp(Instant.now())
                .data(SdkBytes.fromByteArray(data))
                .sequenceNumber(String.valueOf(sequenceNumber.incrementAndGet()))
                .partitionKey("pk")
                .build();
    }

    private static Record createAggregatedRecord(
            final int aggregationFactor, final AtomicInteger sequenceNumber) {
        RecordAggregator recordAggregator = new RecordAggregator();

        for (int i = 0; i < aggregationFactor; i++) {
            try {
                recordAggregator.addUserRecord("pk", randomAlphabetic(32).getBytes(UTF_8));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return createRecord(recordAggregator.clearAndGet().toRecordBytes(), sequenceNumber);
    }
}
