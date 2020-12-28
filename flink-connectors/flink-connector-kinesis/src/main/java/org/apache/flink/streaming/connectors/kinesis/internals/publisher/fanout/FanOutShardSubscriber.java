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
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.util.Preconditions;

import io.netty.handler.timeout.ReadTimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class is responsible for acquiring an Enhanced Fan Out subscription and consuming records
 * from a shard. A queue is used to buffer records between the Kinesis Proxy and Flink application.
 * This allows processing to be separated from consumption; errors thrown in the consumption layer
 * do not propagate up to application.
 *
 * <pre>{@code [
 * | ----------- Source Connector Thread ----------- |                      | --- KinesisAsyncClient Thread(s) -- |
 * | FanOutRecordPublisher | FanOutShardSubscription | == blocking queue == | KinesisProxyV2 | KinesisAsyncClient |
 * ]}</pre>
 *
 * <p>Three types of message are passed over the queue for inter-thread communication:
 *
 * <ul>
 *   <li>{@link SubscriptionNextEvent} - passes data from the network to the consumer
 *   <li>{@link SubscriptionCompleteEvent} - indicates a subscription has expired
 *   <li>{@link SubscriptionErrorEvent} - passes an exception from the network to the consumer
 * </ul>
 *
 * <p>The blocking queue has a maximum capacity of 1 record. This allows backpressure to be applied
 * closer to the network stack and results in record prefetch. At maximum capacity we will have
 * three {@link SubscribeToShardEvent} in memory (per instance of this class):
 *
 * <ul>
 *   <li>1 event being processed by the consumer
 *   <li>1 event enqueued in the blocking queue
 *   <li>1 event being added to the queue by the network (blocking)
 * </ul>
 */
@Internal
public class FanOutShardSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(FanOutShardSubscriber.class);

    /**
     * The maximum capacity of the queue between the network and consumption thread. The queue is
     * mainly used to isolate networking from consumption such that errors do not bubble up. This
     * queue also acts as a buffer resulting in a record prefetch and reduced latency.
     */
    private static final int QUEUE_CAPACITY = 1;

    /**
     * Read timeout will occur after 30 seconds, a sanity timeout to prevent lockup in unexpected
     * error states. If the consumer does not receive a new event within the DEQUEUE_WAIT_SECONDS it
     * will backoff and resubscribe. Under normal conditions heartbeat events are received even when
     * there are no records to consume, so it is not expected for this timeout to occur under normal
     * conditions.
     */
    private static final int DEQUEUE_WAIT_SECONDS = 35;

    private final BlockingQueue<FanOutSubscriptionEvent> queue =
            new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    private final AtomicReference<FanOutSubscriptionEvent> subscriptionErrorEvent =
            new AtomicReference<>();

    private final KinesisProxyV2Interface kinesis;

    private final String consumerArn;

    private final String shardId;

    /**
     * Create a new Fan Out subscriber.
     *
     * @param consumerArn the stream consumer ARN
     * @param shardId the shard ID to subscribe to
     * @param kinesis the Kinesis Proxy used to communicate via AWS SDK v2
     */
    FanOutShardSubscriber(
            final String consumerArn, final String shardId, final KinesisProxyV2Interface kinesis) {
        this.kinesis = Preconditions.checkNotNull(kinesis);
        this.consumerArn = Preconditions.checkNotNull(consumerArn);
        this.shardId = Preconditions.checkNotNull(shardId);
    }

    /**
     * Obtains a subscription to the shard from the specified {@code startingPosition}. {@link
     * SubscribeToShardEvent} received from KDS are delivered to the given {@code eventConsumer}.
     * Returns false if there are records left to consume from the shard.
     *
     * @param startingPosition the position in the stream in which to start receiving records
     * @param eventConsumer the consumer to deliver received events to
     * @return true if there are no more messages (complete), false if a subsequent subscription
     *     should be obtained
     * @throws FanOutSubscriberException when an exception is propagated from the networking stack
     * @throws InterruptedException when the thread is interrupted
     */
    boolean subscribeToShardAndConsumeRecords(
            final StartingPosition startingPosition,
            final Consumer<SubscribeToShardEvent> eventConsumer)
            throws InterruptedException, FanOutSubscriberException {
        LOG.debug("Subscribing to shard {} ({})", shardId, consumerArn);

        try {
            openSubscriptionToShard(startingPosition);
        } catch (FanOutSubscriberException ex) {
            // The only exception that should cause a failure is a ResourceNotFoundException
            // Rethrow the exception to trigger the application to terminate
            if (ex.getCause() instanceof ResourceNotFoundException) {
                throw (ResourceNotFoundException) ex.getCause();
            }

            throw ex;
        }

        return consumeAllRecordsFromKinesisShard(eventConsumer);
    }

    /**
     * Calls {@link KinesisProxyV2#subscribeToShard} and waits to acquire a subscription. In the
     * event a non-recoverable error occurs this method will rethrow the exception. Once the
     * subscription is acquired the client signals to the producer that we are ready to receive
     * records.
     *
     * @param startingPosition the position in which to start consuming from
     * @throws FanOutSubscriberException when an exception is propagated from the networking stack
     */
    private void openSubscriptionToShard(final StartingPosition startingPosition)
            throws FanOutSubscriberException, InterruptedException {
        SubscribeToShardRequest request =
                SubscribeToShardRequest.builder()
                        .consumerARN(consumerArn)
                        .shardId(shardId)
                        .startingPosition(startingPosition)
                        .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch waitForSubscriptionLatch = new CountDownLatch(1);
        FanOutShardSubscription subscription =
                new FanOutShardSubscription(waitForSubscriptionLatch);

        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder()
                        .onError(
                                e -> {
                                    // Errors that occur while trying to acquire a subscription are
                                    // only thrown from here
                                    // Errors that occur during the subscription are surfaced here
                                    // and to the FanOutShardSubscription
                                    //	(errors are ignored here once the subscription is open)
                                    if (waitForSubscriptionLatch.getCount() > 0) {
                                        exception.set(e);
                                        waitForSubscriptionLatch.countDown();
                                    }
                                })
                        .subscriber(() -> subscription)
                        .build();

        kinesis.subscribeToShard(request, responseHandler);

        waitForSubscriptionLatch.await();

        Throwable throwable = exception.get();
        if (throwable != null) {
            handleError(throwable);
        }

        LOG.debug("Acquired subscription - {} ({})", shardId, consumerArn);

        // Request the first record to kick off consumption
        // Following requests are made by the FanOutShardSubscription on the netty thread
        subscription.requestRecord();
    }

    /**
     * Update the reference to the latest networking error in this object. Parent caller can
     * interrogate to decide how to handle error.
     *
     * @param throwable the exception that has occurred
     */
    private void handleError(final Throwable throwable) throws FanOutSubscriberException {
        Throwable cause;
        if (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
            cause = throwable.getCause();
        } else {
            cause = throwable;
        }

        LOG.warn(
                "Error occurred on EFO subscription: {} - ({}).  {} ({})",
                throwable.getClass().getName(),
                throwable.getMessage(),
                shardId,
                consumerArn,
                cause);

        if (cause instanceof ReadTimeoutException) {
            // ReadTimeoutException occurs naturally under backpressure scenarios when full batches
            // take longer to
            // process than standard read timeout (default 30s). Recoverable exceptions are intended
            // to be retried
            // indefinitely to avoid system degradation under backpressure. The EFO connection
            // (subscription) to Kinesis
            // is closed, and reacquired once the queue of records has been processed.
            throw new RecoverableFanOutSubscriberException(cause);
        } else {
            throw new RetryableFanOutSubscriberException(cause);
        }
    }

    /**
     * Once the subscription is open, records will be delivered to the {@link BlockingQueue}. Queue
     * capacity is hardcoded to 1 record, the queue is used solely to separate consumption and
     * processing. However, this buffer will result in latency reduction as records are pre-fetched
     * as a result. This method will poll the queue and exit under any of these conditions: - {@code
     * continuationSequenceNumber} is {@code null}, indicating the shard is complete - The
     * subscription expires, indicated by a {@link SubscriptionCompleteEvent} - There is an error
     * while consuming records, indicated by a {@link SubscriptionErrorEvent}
     *
     * @param eventConsumer the event consumer to deliver records to
     * @return true if there are no more messages (complete), false if a subsequent subscription
     *     should be obtained
     * @throws FanOutSubscriberException when an exception is propagated from the networking stack
     * @throws InterruptedException when the thread is interrupted
     */
    private boolean consumeAllRecordsFromKinesisShard(
            final Consumer<SubscribeToShardEvent> eventConsumer)
            throws InterruptedException, FanOutSubscriberException {
        String continuationSequenceNumber;

        do {
            FanOutSubscriptionEvent subscriptionEvent;
            if (queue.isEmpty() && subscriptionErrorEvent.get() != null) {
                subscriptionEvent = subscriptionErrorEvent.get();
            } else {
                // Read timeout will occur after 30 seconds, add a sanity timeout here to prevent
                // lockup
                subscriptionEvent = queue.poll(DEQUEUE_WAIT_SECONDS, SECONDS);
            }

            if (subscriptionEvent == null) {
                LOG.debug(
                        "Timed out polling events from network, reacquiring subscription - {} ({})",
                        shardId,
                        consumerArn);
                return false;
            } else if (subscriptionEvent.isSubscribeToShardEvent()) {
                SubscribeToShardEvent event = subscriptionEvent.getSubscribeToShardEvent();
                continuationSequenceNumber = event.continuationSequenceNumber();
                if (!event.records().isEmpty()) {
                    eventConsumer.accept(event);
                }
            } else if (subscriptionEvent.isSubscriptionComplete()) {
                if (subscriptionErrorEvent.get() != null) {
                    handleError(subscriptionErrorEvent.get().getThrowable());
                }

                // The subscription is complete, but the shard might not be, so we return incomplete
                return false;
            } else {
                handleError(subscriptionEvent.getThrowable());
                return false;
            }
        } while (continuationSequenceNumber != null);

        return true;
    }

    /**
     * The {@link FanOutShardSubscription} subscribes to the events coming from KDS and adds them to
     * the {@link BlockingQueue}. Backpressure is applied based on the maximum capacity of the
     * queue. The {@link Subscriber} methods of this class are invoked by a thread from the {@link
     * KinesisAsyncClient}.
     */
    private class FanOutShardSubscription implements Subscriber<SubscribeToShardEventStream> {

        private Subscription subscription;

        private volatile boolean cancelled = false;

        private final CountDownLatch waitForSubscriptionLatch;

        private FanOutShardSubscription(final CountDownLatch waitForSubscriptionLatch) {
            this.waitForSubscriptionLatch = waitForSubscriptionLatch;
        }

        /** Flag to the producer that we are ready to receive more events. */
        void requestRecord() {
            if (!cancelled) {
                LOG.debug(
                        "Requesting more records from EFO subscription - {} ({})",
                        shardId,
                        consumerArn);
                subscription.request(1);
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            waitForSubscriptionLatch.countDown();
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(
                    new SubscribeToShardResponseHandler.Visitor() {
                        @Override
                        public void visit(SubscribeToShardEvent event) {
                            enqueueEvent(new SubscriptionNextEvent(event));
                            requestRecord();
                        }
                    });
        }

        @Override
        public void onError(Throwable throwable) {
            LOG.debug(
                    "Error occurred on EFO subscription: {} - ({}).  {} ({})",
                    throwable.getClass().getName(),
                    throwable.getMessage(),
                    shardId,
                    consumerArn,
                    throwable);

            // Cancel the subscription to signal the onNext to stop requesting data
            cancelSubscription();

            if (subscriptionErrorEvent.get() == null) {
                subscriptionErrorEvent.set(new SubscriptionErrorEvent(throwable));
            } else {
                LOG.warn(
                        "Previous error passed to consumer for processing. Ignoring subsequent exception.",
                        throwable);
            }
        }

        @Override
        public void onComplete() {
            LOG.debug("EFO subscription complete - {} ({})", shardId, consumerArn);
            enqueueEvent(new SubscriptionCompleteEvent());
        }

        private void cancelSubscription() {
            if (!cancelled) {
                cancelled = true;
                subscription.cancel();
            }
        }

        /**
         * Adds the event to the queue blocking until complete.
         *
         * @param event the event to enqueue
         */
        private void enqueueEvent(final FanOutSubscriptionEvent event) {
            try {
                queue.put(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /** An exception wrapper to indicate an error has been thrown from the networking stack. */
    abstract static class FanOutSubscriberException extends Exception {

        private static final long serialVersionUID = -3899472233945299730L;

        public FanOutSubscriberException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception wrapper to indicate a retryable error has been thrown from the networking stack.
     * Retryable errors are subject to the Subscribe to Shard retry policy. If the configured number
     * of retries are exceeded the application will terminate.
     */
    static class RetryableFanOutSubscriberException extends FanOutSubscriberException {

        private static final long serialVersionUID = -2967281117554404883L;

        public RetryableFanOutSubscriberException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception wrapper to indicate a recoverable error has been thrown from the networking
     * stack. Recoverable errors are not counted in the retry policy.
     */
    static class RecoverableFanOutSubscriberException extends FanOutSubscriberException {

        private static final long serialVersionUID = -3223347557038294482L;

        public RecoverableFanOutSubscriberException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An interface used to pass messages between {@link FanOutShardSubscription} and {@link
     * FanOutShardSubscriber} via the {@link BlockingQueue}.
     */
    private interface FanOutSubscriptionEvent {

        default boolean isSubscribeToShardEvent() {
            return false;
        }

        default boolean isSubscriptionComplete() {
            return false;
        }

        default SubscribeToShardEvent getSubscribeToShardEvent() {
            throw new UnsupportedOperationException(
                    "This event does not support getSubscribeToShardEvent()");
        }

        default Throwable getThrowable() {
            throw new UnsupportedOperationException("This event does not support getThrowable()");
        }
    }

    /** Indicates that an EFO subscription has completed/expired. */
    private static class SubscriptionCompleteEvent implements FanOutSubscriptionEvent {

        @Override
        public boolean isSubscriptionComplete() {
            return true;
        }
    }

    /** Poison pill, indicates that an error occurred while consuming from KDS. */
    private static class SubscriptionErrorEvent implements FanOutSubscriptionEvent {
        private final Throwable throwable;

        private SubscriptionErrorEvent(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        public Throwable getThrowable() {
            return throwable;
        }
    }

    /** A wrapper to pass the next {@link SubscribeToShardEvent} between threads. */
    private static class SubscriptionNextEvent implements FanOutSubscriptionEvent {
        private final SubscribeToShardEvent subscribeToShardEvent;

        private SubscriptionNextEvent(SubscribeToShardEvent subscribeToShardEvent) {
            this.subscribeToShardEvent = subscribeToShardEvent;
        }

        @Override
        public boolean isSubscribeToShardEvent() {
            return true;
        }

        @Override
        public SubscribeToShardEvent getSubscribeToShardEvent() {
            return subscribeToShardEvent;
        }
    }
}
