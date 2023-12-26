/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.UnorderedStreamElementQueue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.apache.flink.streaming.util.retryable.AsyncRetryStrategies.NO_RETRY_STRATEGY;

/**
 * The {@link AsyncWaitOperator} allows to asynchronously process incoming stream records. For that
 * the operator creates an {@link ResultFuture} which is passed to an {@link AsyncFunction}. Within
 * the async function, the user can complete the async collector arbitrarily. Once the async
 * collector has been completed, the result is emitted by the operator's emitter to downstream
 * operators.
 *
 * <p>The operator offers different output modes depending on the chosen {@link OutputMode}. In
 * order to give exactly once processing guarantees, the operator stores all currently in-flight
 * {@link StreamElement} in it's operator state. Upon recovery the recorded set of stream elements
 * is replayed.
 *
 * <p>The operator also support retry on predefined condition and strategy {@link
 * AsyncRetryStrategy}. If given strategy is NO_RETRY_STRATEGY or a custom implemented {@link
 * org.apache.flink.streaming.api.functions.async.AsyncRetryPredicate} which defines empty retry
 * predicate, then no retry will happen, everything keeps the same as the {@link AsyncWaitOperator}
 * without retry support.
 *
 * <p>In case of chaining of this operator, it has to be made sure that the operators in the chain
 * are opened tail to head. The reason for this is that an opened {@link AsyncWaitOperator} starts
 * already emitting recovered {@link StreamElement} to downstream operators.
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class AsyncWaitOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
    private static final long serialVersionUID = 1L;

    private static final String STATE_NAME = "_async_wait_operator_state_";

    /** Capacity of the stream element queue. */
    private final int capacity;

    /** Output mode for this operator. */
    private final AsyncDataStream.OutputMode outputMode;

    /** Timeout for the async collectors. */
    private final long timeout;

    /** AsyncRetryStrategy for the async function. */
    private final AsyncRetryStrategy<OUT> asyncRetryStrategy;

    /** If the retry strategy is not no_retry. */
    private final boolean retryEnabled;

    /** {@link TypeSerializer} for inputs while making snapshots. */
    private transient StreamElementSerializer<IN> inStreamElementSerializer;

    /** Recovered input stream elements. */
    private transient ListState<StreamElement> recoveredStreamElements;

    /** Queue, into which to store the currently in-flight stream elements. */
    private transient StreamElementQueue<OUT> queue;

    /** In-flight delay retry handlers, will be removed when a retry has been triggered. */
    private transient Set<RetryableResultHandlerDelegator> inFlightDelayRetryHandlers;

    /** Mailbox executor used to yield while waiting for buffers to empty. */
    private final transient MailboxExecutor mailboxExecutor;

    private transient TimestampedCollector<OUT> timestampedCollector;

    /** Whether object reuse has been enabled or disabled. */
    private transient boolean isObjectReuseEnabled;

    private transient Predicate<Collection<OUT>> retryResultPredicate;

    private transient Predicate<Throwable> retryExceptionPredicate;

    /** Whether retry is disabled due to task finish, initially set to false. */
    private transient AtomicBoolean retryDisabledOnFinish;

    public AsyncWaitOperator(
            @Nonnull AsyncFunction<IN, OUT> asyncFunction,
            long timeout,
            int capacity,
            @Nonnull AsyncDataStream.OutputMode outputMode,
            @Nonnull AsyncRetryStrategy<OUT> asyncRetryStrategy,
            @Nonnull ProcessingTimeService processingTimeService,
            @Nonnull MailboxExecutor mailboxExecutor) {
        super(asyncFunction);

        setChainingStrategy(ChainingStrategy.ALWAYS);

        Preconditions.checkArgument(
                capacity > 0, "The number of concurrent async operation should be greater than 0.");
        this.capacity = capacity;

        this.outputMode = Preconditions.checkNotNull(outputMode, "outputMode");

        this.timeout = timeout;

        this.asyncRetryStrategy = asyncRetryStrategy;

        // enables retry only when the resultPredicate or exceptionPredicate is present.
        this.retryEnabled =
                asyncRetryStrategy != NO_RETRY_STRATEGY
                        && (asyncRetryStrategy.getRetryPredicate().resultPredicate().isPresent()
                                || asyncRetryStrategy
                                        .getRetryPredicate()
                                        .exceptionPredicate()
                                        .isPresent());

        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);

        this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        this.inStreamElementSerializer =
                new StreamElementSerializer<>(
                        getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));
        switch (outputMode) {
            case ORDERED:
                queue = new OrderedStreamElementQueue<>(capacity);
                break;
            case UNORDERED:
                queue = new UnorderedStreamElementQueue<>(capacity);
                break;
            default:
                throw new IllegalStateException("Unknown async mode: " + outputMode + '.');
        }
        if (retryEnabled) {
            this.retryResultPredicate =
                    asyncRetryStrategy
                            .getRetryPredicate()
                            .resultPredicate()
                            .orElse(ignore -> false);
            this.retryExceptionPredicate =
                    asyncRetryStrategy
                            .getRetryPredicate()
                            .exceptionPredicate()
                            .orElse(ignore -> false);
        }

        this.timestampedCollector = new TimestampedCollector<>(super.output);
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.isObjectReuseEnabled = getExecutionConfig().isObjectReuseEnabled();
        if (retryEnabled) {
            this.inFlightDelayRetryHandlers = new HashSet<>();
            this.retryDisabledOnFinish = new AtomicBoolean(false);
        }

        if (recoveredStreamElements != null) {
            for (StreamElement element : recoveredStreamElements.get()) {
                if (element.isRecord()) {
                    processElement(element.<IN>asRecord());
                } else if (element.isWatermark()) {
                    processWatermark(element.asWatermark());
                } else if (element.isLatencyMarker()) {
                    processLatencyMarker(element.asLatencyMarker());
                } else {
                    throw new IllegalStateException(
                            "Unknown record type "
                                    + element.getClass()
                                    + " encountered while opening the operator.");
                }
            }
            recoveredStreamElements = null;
        }
    }

    @Override
    public void processElement(StreamRecord<IN> record) throws Exception {
        StreamRecord<IN> element;
        // copy the element avoid the element is reused
        if (isObjectReuseEnabled) {
            //noinspection unchecked
            element = (StreamRecord<IN>) inStreamElementSerializer.copy(record);
        } else {
            element = record;
        }

        // add element first to the queue
        final ResultFuture<OUT> entry = addToWorkQueue(element);

        if (retryEnabled) {
            final RetryableResultHandlerDelegator resultHandler =
                    new RetryableResultHandlerDelegator(element, entry, getProcessingTimeService());

            // register a timeout for the entry
            assert timeout > 0L;
            resultHandler.registerTimeout(timeout);

            userFunction.asyncInvoke(element.getValue(), resultHandler);

        } else {
            final ResultHandler resultHandler = new ResultHandler(element, entry);

            // register a timeout for the entry if timeout is configured
            if (timeout > 0L) {
                resultHandler.registerTimeout(getProcessingTimeService(), timeout);
            }

            userFunction.asyncInvoke(element.getValue(), resultHandler);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        addToWorkQueue(mark);

        // watermarks are always completed
        // if there is no prior element, we can directly emit them
        // this also avoids watermarks being held back until the next element has been processed
        outputCompletedElement();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        ListState<StreamElement> partitionableState =
                getOperatorStateBackend()
                        .getListState(
                                new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));

        try {
            partitionableState.update(queue.values());
        } catch (Exception e) {
            partitionableState.clear();

            throw new Exception(
                    "Could not add stream element queue entries to operator state "
                            + "backend of operator "
                            + getOperatorName()
                            + '.',
                    e);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        recoveredStreamElements =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
    }

    @Override
    public void endInput() throws Exception {
        // we should finish all in fight delayed retry immediately.
        finishInFlightDelayedRetry();

        // we should wait here for the data in flight to be finished. the reason is that the
        // timer not in running will be forbidden to fire after this, so that when the async
        // operation is stuck, it results in deadlock due to what the timeout timer is not fired
        waitInFlightInputsFinished();
    }

    /**
     * Add the given stream element to the operator's stream element queue. This operation blocks
     * until the element has been added.
     *
     * <p>Between two insertion attempts, this method yields the execution to the mailbox, such that
     * events as well as asynchronous results can be processed.
     *
     * @param streamElement to add to the operator's queue
     * @throws InterruptedException if the current thread has been interrupted while yielding to
     *     mailbox
     * @return a handle that allows to set the result of the async computation for the given
     *     element.
     */
    private ResultFuture<OUT> addToWorkQueue(StreamElement streamElement)
            throws InterruptedException {

        Optional<ResultFuture<OUT>> queueEntry;
        while (!(queueEntry = queue.tryPut(streamElement)).isPresent()) {
            mailboxExecutor.yield();
        }

        return queueEntry.get();
    }

    private void finishInFlightDelayedRetry() throws Exception {
        if (retryEnabled) {
            // disable delayed retry, after which all data will give up retry and complete normally.
            this.retryDisabledOnFinish.set(true);
            if (inFlightDelayRetryHandlers.size() > 0) {
                for (RetryableResultHandlerDelegator delegator : inFlightDelayRetryHandlers) {
                    assert delegator.delayedRetryTimer != null;
                    // cancel delayedRetryTimer timer first
                    delegator.cancelRetryTimer();

                    // fire an attempt intermediately not rely on successfully canceling the retry
                    // timer for two reasons: 1. cancel retry timer can not be 100% safe 2. there's
                    // protection for repeated retries
                    tryOnce(delegator);
                }
                inFlightDelayRetryHandlers.clear();
            }
        }
    }

    private void waitInFlightInputsFinished() throws InterruptedException {

        while (!queue.isEmpty()) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Outputs one completed element. Watermarks are always completed if it's their turn to be
     * processed.
     *
     * <p>This method will be called from {@link #processWatermark(Watermark)} and from a mail
     * processing the result of an async function call.
     */
    private void outputCompletedElement() {
        if (queue.hasCompletedElements()) {
            // emit only one element to not block the mailbox thread unnecessarily
            queue.emitCompletedElement(timestampedCollector);
            // if there are more completed elements, emit them with subsequent mails
            if (queue.hasCompletedElements()) {
                try {
                    mailboxExecutor.execute(
                            this::outputCompletedElement,
                            "AsyncWaitOperator#outputCompletedElement");
                } catch (RejectedExecutionException mailboxClosedException) {
                    // This exception can only happen if the operator is cancelled which means all
                    // pending records can be safely ignored since they will be processed one more
                    // time after recovery.
                    LOG.debug(
                            "Attempt to complete element is ignored since the mailbox rejected the execution.",
                            mailboxClosedException);
                }
            }
        }
    }

    /** Increments number of attempts and fire the attempt. */
    private void tryOnce(RetryableResultHandlerDelegator resultHandlerDelegator) throws Exception {
        // increment current attempt number
        resultHandlerDelegator.currentAttempts++;
        // fire a new attempt
        userFunction.asyncInvoke(
                resultHandlerDelegator.resultHandler.inputRecord.getValue(),
                resultHandlerDelegator);
    }

    /** Utility method to register timeout timer. */
    private ScheduledFuture<?> registerTimer(
            ProcessingTimeService processingTimeService,
            long timeout,
            ThrowingConsumer<Void, Exception> callback) {
        final long timeoutTimestamp = timeout + processingTimeService.getCurrentProcessingTime();

        return processingTimeService.registerTimer(
                timeoutTimestamp, timestamp -> callback.accept(null));
    }

    /** A delegator holds the real {@link ResultHandler} to handle retries. */
    private class RetryableResultHandlerDelegator implements ResultFuture<OUT> {

        private final ResultHandler resultHandler;
        private final ProcessingTimeService processingTimeService;

        private ScheduledFuture<?> delayedRetryTimer;

        /** start from 1, when this entry created, the first attempt will happen. */
        private int currentAttempts = 1;

        /**
         * A guard similar to ResultHandler#complete to prevent repeated complete calls from
         * ill-written AsyncFunction. This flag indicates a retry is in-flight, new retry will be
         * rejected if it is true, and it will be reset to false after the retry fired.
         */
        private final AtomicBoolean retryAwaiting = new AtomicBoolean(false);

        public RetryableResultHandlerDelegator(
                StreamRecord<IN> inputRecord,
                ResultFuture<OUT> resultFuture,
                ProcessingTimeService processingTimeService) {
            this.resultHandler = new ResultHandler(inputRecord, resultFuture);
            this.processingTimeService = processingTimeService;
        }

        private void registerTimeout(long timeout) {
            resultHandler.timeoutTimer =
                    registerTimer(processingTimeService, timeout, t -> timerTriggered());
        }

        private void cancelRetryTimer() {
            if (delayedRetryTimer != null) {
                // do not interrupt task thread, just try to cancel the timer
                delayedRetryTimer.cancel(false);
            }
        }

        /** Rewrite the timeout process to deal with retry state. */
        private void timerTriggered() throws Exception {
            if (!resultHandler.completed.get()) {
                // cancel delayed retry timer first
                cancelRetryTimer();

                // force reset retryAwaiting to prevent the handler to trigger retry unnecessarily
                retryAwaiting.set(false);

                userFunction.timeout(resultHandler.inputRecord.getValue(), this);
            }
        }

        @Override
        public void complete(Collection<OUT> results) {
            Preconditions.checkNotNull(
                    results, "Results must not be null, use empty collection to emit nothing");
            if (!retryDisabledOnFinish.get() && resultHandler.inputRecord.isRecord()) {
                processRetryInMailBox(results, null);
            } else {
                cancelRetryTimer();

                resultHandler.complete(results);
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            if (!retryDisabledOnFinish.get() && resultHandler.inputRecord.isRecord()) {
                processRetryInMailBox(null, error);
            } else {
                cancelRetryTimer();

                resultHandler.completeExceptionally(error);
            }
        }

        private void processRetryInMailBox(Collection<OUT> results, Throwable error) {
            mailboxExecutor.submit(() -> processRetry(results, error), "delayed retry or complete");
        }

        private void processRetry(Collection<OUT> results, Throwable error) {
            // ignore repeated call(s) and only called in main thread can be safe
            if (!retryAwaiting.compareAndSet(false, true)) {
                return;
            }

            boolean satisfy =
                    (null != results && retryResultPredicate.test(results))
                            || (null != error && retryExceptionPredicate.test(error));

            if (satisfy
                    && asyncRetryStrategy.canRetry(currentAttempts)
                    && !retryDisabledOnFinish.get()) {
                long nextBackoffTimeMillis =
                        asyncRetryStrategy.getBackoffTimeMillis(currentAttempts);
                final long delayedRetry =
                        nextBackoffTimeMillis
                                + getProcessingTimeService().getCurrentProcessingTime();

                // timer thread will finally dispatch the task to mailbox executor,
                // and it can only be submitted once for one attempt.
                delayedRetryTimer =
                        processingTimeService.registerTimer(delayedRetry, timestamp -> doRetry());

                // add to incomplete retry handlers only for first time
                if (currentAttempts == 1) {
                    inFlightDelayRetryHandlers.add(this);
                }
            } else {
                // remove handle that has been tried from incomplete retry handlers, and ignore the
                // retryAwaiting flag due to no more retry will happen.
                if (currentAttempts > 1) {
                    inFlightDelayRetryHandlers.remove(this);
                }
                // retry unsatisfied, complete it
                if (null != results) {
                    resultHandler.complete(results);
                } else {
                    resultHandler.completeExceptionally(error);
                }
            }
        }

        private void doRetry() throws Exception {
            // fire a retry only when it is in awaiting state, otherwise timeout may already happen
            if (retryAwaiting.compareAndSet(true, false)) {
                tryOnce(this);
            }
        }
    }

    /** A handler for the results of a specific input record. */
    private class ResultHandler implements ResultFuture<OUT> {
        /** Optional timeout timer used to signal the timeout to the AsyncFunction. */
        private ScheduledFuture<?> timeoutTimer;
        /** Record for which this result handler exists. Used only to report errors. */
        private final StreamRecord<IN> inputRecord;
        /**
         * The handle received from the queue to update the entry. Should only be used to inject the
         * result; exceptions are handled here.
         */
        private final ResultFuture<OUT> resultFuture;
        /**
         * A guard against ill-written AsyncFunction. Additional (parallel) invokations of {@link
         * #complete(Collection)} or {@link #completeExceptionally(Throwable)} will be ignored. This
         * guard also helps for cases where proper results and timeouts happen at the same time.
         */
        private final AtomicBoolean completed = new AtomicBoolean(false);

        ResultHandler(StreamRecord<IN> inputRecord, ResultFuture<OUT> resultFuture) {
            this.inputRecord = inputRecord;
            this.resultFuture = resultFuture;
        }

        @Override
        public void complete(Collection<OUT> results) {

            // already completed (exceptionally or with previous complete call from ill-written
            // AsyncFunction), so
            // ignore additional result
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            processInMailbox(results);
        }

        private void processInMailbox(Collection<OUT> results) {
            // move further processing into the mailbox thread
            mailboxExecutor.execute(
                    () -> processResults(results),
                    "Result in AsyncWaitOperator of input %s",
                    results);
        }

        private void processResults(Collection<OUT> results) {
            // Cancel the timer once we've completed the stream record buffer entry. This will
            // remove the registered
            // timer task
            if (timeoutTimer != null) {
                // canceling in mailbox thread avoids
                // https://issues.apache.org/jira/browse/FLINK-13635
                timeoutTimer.cancel(true);
            }

            // update the queue entry with the result
            resultFuture.complete(results);
            // now output all elements from the queue that have been completed (in the correct
            // order)
            outputCompletedElement();
        }

        @Override
        public void completeExceptionally(Throwable error) {
            // already completed, so ignore exception
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // signal failure through task
            getContainingTask()
                    .getEnvironment()
                    .failExternally(
                            new Exception(
                                    "Could not complete the stream element: " + inputRecord + '.',
                                    error));

            // complete with empty result, so that we remove timer and move ahead processing (to
            // leave potentially
            // blocking section in #addToWorkQueue or #waitInFlightInputsFinished)
            processInMailbox(Collections.emptyList());
        }

        private void registerTimeout(ProcessingTimeService processingTimeService, long timeout) {
            timeoutTimer = registerTimer(processingTimeService, timeout, t -> timerTriggered());
        }

        private void timerTriggered() throws Exception {
            if (!completed.get()) {
                userFunction.timeout(inputRecord.getValue(), this);
            }
        }
    }
}
