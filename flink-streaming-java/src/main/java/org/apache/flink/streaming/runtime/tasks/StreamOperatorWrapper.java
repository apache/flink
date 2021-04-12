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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class handles the close, endInput and other related logic of a {@link StreamOperator}. It
 * also automatically propagates the close operation to the next wrapper that the {@link #next}
 * points to, so we can use {@link #next} to link all operator wrappers in the operator chain and
 * close all operators only by calling the {@link #close(StreamTaskActionExecutor, boolean)} method
 * of the header operator wrapper.
 */
@Internal
public class StreamOperatorWrapper<OUT, OP extends StreamOperator<OUT>> {

    private final OP wrapped;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<ProcessingTimeService> processingTimeService;

    private final MailboxExecutor mailboxExecutor;

    private final boolean isHead;

    private StreamOperatorWrapper<?, ?> previous;

    private StreamOperatorWrapper<?, ?> next;

    private boolean closed;

    StreamOperatorWrapper(
            OP wrapped,
            Optional<ProcessingTimeService> processingTimeService,
            MailboxExecutor mailboxExecutor,
            boolean isHead) {

        this.wrapped = checkNotNull(wrapped);
        this.processingTimeService = checkNotNull(processingTimeService);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.isHead = isHead;
    }

    /**
     * Checks if the wrapped operator has been closed.
     *
     * <p>Note that this method must be called in the task thread.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Ends an input of the operator contained by this wrapper.
     *
     * @param inputId the input ID starts from 1 which indicates the first input.
     */
    public void endOperatorInput(int inputId) throws Exception {
        if (wrapped instanceof BoundedOneInput) {
            ((BoundedOneInput) wrapped).endInput();
        } else if (wrapped instanceof BoundedMultiInput) {
            ((BoundedMultiInput) wrapped).endInput(inputId);
        }
    }

    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!closed) {
            wrapped.notifyCheckpointComplete(checkpointId);
        }
    }

    public OP getStreamOperator() {
        return wrapped;
    }

    void setPrevious(StreamOperatorWrapper previous) {
        this.previous = previous;
    }

    void setNext(StreamOperatorWrapper next) {
        this.next = next;
    }

    /**
     * Closes the wrapped operator and propagates the close operation to the next wrapper that the
     * {@link #next} points to.
     *
     * <p>Note that this method must be called in the task thread, because we need to call {@link
     * MailboxExecutor#yield()} to take the mails of closing operator and running timers and run
     * them.
     */
    public void close(StreamTaskActionExecutor actionExecutor, boolean isStoppingBySyncSavepoint)
            throws Exception {
        if (!isHead && !isStoppingBySyncSavepoint) {
            // NOTE: This only do for the case where the operator is one-input operator. At present,
            // any non-head operator on the operator chain is one-input operator.
            actionExecutor.runThrowing(() -> endOperatorInput(1));
        }

        quiesceTimeServiceAndCloseOperator(actionExecutor);

        // propagate the close operation to the next wrapper
        if (next != null) {
            next.close(actionExecutor, isStoppingBySyncSavepoint);
        }
    }

    private void quiesceTimeServiceAndCloseOperator(StreamTaskActionExecutor actionExecutor)
            throws InterruptedException, ExecutionException {

        // step 1. to ensure that there is no longer output triggered by the timers before invoking
        // the "close()"
        //         method of the operator, we quiesce the processing time service to prevent the
        // pending timers
        //         from firing, but wait the timers in running to finish
        // step 2. invoke the "close()" method of the operator. executing the close operation must
        // be deferred
        //         to the mailbox to ensure that mails already in the mailbox are finished before
        // closing the
        //         operator
        // step 3. send a closed mail to ensure that the mails that are from the operator and still
        // in the mailbox
        //         are completed before exiting the following mailbox processing loop
        CompletableFuture<Void> closedFuture =
                quiesceProcessingTimeService()
                        .thenCompose(unused -> deferCloseOperatorToMailbox(actionExecutor))
                        .thenCompose(unused -> sendClosedMail());

        // run the mailbox processing loop until all operations are finished
        while (!closedFuture.isDone()) {
            while (mailboxExecutor.tryYield()) {}

            // we wait a little bit to avoid unnecessary CPU occupation due to empty loops,
            // such as when all mails of the operator have been processed but the closed future
            // has not been set to completed state
            try {
                closedFuture.get(1, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                // do nothing
            }
        }

        // expose the exception thrown when closing
        closedFuture.get();
    }

    private CompletableFuture<Void> deferCloseOperatorToMailbox(
            StreamTaskActionExecutor actionExecutor) {
        final CompletableFuture<Void> closeOperatorFuture = new CompletableFuture<>();

        mailboxExecutor.execute(
                () -> {
                    try {
                        closeOperator(actionExecutor);
                        closeOperatorFuture.complete(null);
                    } catch (Throwable t) {
                        closeOperatorFuture.completeExceptionally(t);
                    }
                },
                "StreamOperatorWrapper#closeOperator for " + wrapped);
        return closeOperatorFuture;
    }

    private CompletableFuture<Void> quiesceProcessingTimeService() {
        return processingTimeService
                .map(ProcessingTimeService::quiesce)
                .orElse(CompletableFuture.completedFuture(null));
    }

    private CompletableFuture<Void> sendClosedMail() {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        mailboxExecutor.execute(
                () -> future.complete(null), "StreamOperatorWrapper#sendClosedMail for " + wrapped);
        return future;
    }

    private void closeOperator(StreamTaskActionExecutor actionExecutor) throws Exception {
        actionExecutor.runThrowing(
                () -> {
                    closed = true;
                    wrapped.close();
                });
    }

    static class ReadIterator
            implements Iterator<StreamOperatorWrapper<?, ?>>,
                    Iterable<StreamOperatorWrapper<?, ?>> {

        private final boolean reverse;

        private StreamOperatorWrapper<?, ?> current;

        ReadIterator(StreamOperatorWrapper<?, ?> first, boolean reverse) {
            this.current = first;
            this.reverse = reverse;
        }

        @Override
        public boolean hasNext() {
            return this.current != null;
        }

        @Override
        public StreamOperatorWrapper<?, ?> next() {
            if (hasNext()) {
                StreamOperatorWrapper<?, ?> next = current;
                current = reverse ? current.previous : current.next;
                return next;
            }

            throw new NoSuchElementException();
        }

        @Nonnull
        @Override
        public Iterator<StreamOperatorWrapper<?, ?>> iterator() {
            return this;
        }
    }
}
