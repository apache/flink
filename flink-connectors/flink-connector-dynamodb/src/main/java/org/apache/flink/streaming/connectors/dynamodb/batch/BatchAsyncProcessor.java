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

package org.apache.flink.streaming.connectors.dynamodb.batch;

import org.apache.flink.streaming.connectors.dynamodb.ProducerException;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteResponse;
import org.apache.flink.streaming.connectors.dynamodb.batch.concurrent.CountingCompletionService;
import org.apache.flink.streaming.connectors.dynamodb.batch.concurrent.UnboundedTaskExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Batch processor receives batched write requests and submits new tasks to write them. */
public class BatchAsyncProcessor implements Consumer<ProducerWriteRequest<DynamoDbRequest>> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchAsyncProcessor.class);

    private final BlockingQueue<ProducerWriteRequest<DynamoDbRequest>> outgoingMessagesQueue;

    private final ExecutorService executor;
    private final UnboundedTaskExecutor taskExecutor;
    private final CountingCompletionService<ProducerWriteResponse> callbackCompletionService;
    private final BatchWriterProvider writerProvider;
    private final ResponseHandler outputMessageHandler;

    private final boolean shutdownOnError;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public BatchAsyncProcessor(
            int internalQueueLimit,
            boolean shutdownOnError,
            ExecutorService completionExecutor,
            BatchWriterProvider writerProvider,
            ResponseHandler outputMessageHandler) {
        this.shutdownOnError = shutdownOnError;
        this.outgoingMessagesQueue = new LinkedBlockingQueue<>(internalQueueLimit);
        this.executor = completionExecutor;
        this.taskExecutor = new UnboundedTaskExecutor();
        this.callbackCompletionService = new CountingCompletionService<>(this.taskExecutor);
        this.writerProvider = writerProvider;
        this.outputMessageHandler = outputMessageHandler;
    }

    public boolean isStarted() {
        return started.get();
    }

    @Override
    public void accept(ProducerWriteRequest<DynamoDbRequest> request) {
        if (!started.get() || taskExecutor.isShutdown() || executor.isShutdown()) {
            throw new ProducerException(
                    "BatchAsyncProcessor not started or has been shutdown and can not longer process new messages");
        }
        try {
            // here we are blocking the main thread until queue is freed
            outgoingMessagesQueue.put(request);
        } catch (InterruptedException e) {
            fatalError("Could not put new message", e);
        }
    }

    private synchronized void fatalError(String message, Throwable error) {
        LOG.error(message, error);
        outputMessageHandler.onException(error);

        if (shutdownOnError) {
            shutdown();
        }
    }

    /** Start processing and completion loops. */
    public void start() {
        started.getAndSet(true); // signal that processor is started
        executor.execute(
                () -> {
                    while (started.get()) {
                        processBatchRequests();
                    }
                });
        executor.execute(
                () -> {
                    while (started.get()) {
                        receiveCompletionMessages();
                    }
                });
    }

    public long getOutstandingRecordsCount() {
        return outgoingMessagesQueue.size()
                + callbackCompletionService.getNumberOfTasksInProgress();
    }

    /**
     * Gracefully shutdown batch async processor by waiting all the executors to finish job in
     * progress.
     */
    public void shutdown() {
        if (started.getAndSet(false)) {
            try {
                LOG.info("Shutdown has been initiated for BatchAsyncProcessor.");

                completeMessagesInTransit(60, TimeUnit.SECONDS);

                executor.shutdown();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }

                taskExecutor.shutdown();
                if (!taskExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    List<Runnable> dropped = taskExecutor.shutdownNow();
                    LOG.error(
                            "Task executor was abruptly shut down. "
                                    + dropped.size()
                                    + " tasks will not be executed.");

                    throw new ProducerException(
                            "Abruptly shutdown the producer. "
                                    + dropped.size()
                                    + " tasks in progress will not be executed. Messages might have been lost.");
                }

                if (callbackCompletionService.getNumberOfTasksInProgress() > 0) {
                    throw new ProducerException(
                            "Abruptly shutdown the producer. "
                                    + callbackCompletionService.getNumberOfTasksInProgress()
                                    + " tasks in progress will not be executed. Messages might have been lost.");
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                taskExecutor.shutdownNow();
                throw new ProducerException(
                        "Abruptly shutdown the producer. "
                                + callbackCompletionService.getNumberOfTasksInProgress()
                                + " tasks in progress will not be executed. Messages might have been lost.",
                        e);
            }
        }
    }

    /**
     * Attempts to process messages in transit. Timeout is a best effort and is not guaranteed if
     * task completion code takes too long time between iterations.
     */
    private void completeMessagesInTransit(long timeout, TimeUnit unit) {
        // submit all remaining messages to the task executor
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (outgoingMessagesQueue.size() > 0 && deadline - System.nanoTime() > 0) {
            LOG.info(
                    "Attempting to gracefully shutdown batch processor. Still {} messages in progress.",
                    outgoingMessagesQueue.size());
            processBatchRequests();
        }

        while (callbackCompletionService.getNumberOfTasksInProgress() > 0
                && deadline - System.nanoTime() > 0) {
            LOG.info(
                    "Attempting to gracefully shutdown completion processor. Still {} tasks in progress. Time remaining until forced shutdown {} ms.",
                    callbackCompletionService.getNumberOfTasksInProgress(),
                    TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
            receiveCompletionMessages();
        }
    }

    private void processBatchRequests() {
        try {
            // non-blocking so we can check for the shutdown flag
            ProducerWriteRequest<DynamoDbRequest> request =
                    outgoingMessagesQueue.poll(1, TimeUnit.SECONDS);
            if (request != null) {
                callbackCompletionService.submit(writerProvider.createWriter(request));
            }
        } catch (InterruptedException ex) {
            fatalError("Could not schedule new tasks", ex);
        }
    }

    private void receiveCompletionMessages() {
        try {
            // non-blocking so we can check for the shutdown flag
            Future<ProducerWriteResponse> future =
                    callbackCompletionService.poll(1, TimeUnit.SECONDS);
            if (future != null) {
                ProducerWriteResponse response = future.get();
                outputMessageHandler.onCompletion(response);
            }
        } catch (InterruptedException | ExecutionException ex) {
            fatalError("Could not receive completion message", ex);
        }
    }

    /** Handles completion messages. */
    public interface ResponseHandler {

        /* Called if writer could execute the task without exceptions (both if the write was successful or not). */
        void onCompletion(ProducerWriteResponse response);

        /* Called if writer thrown an exception while executing the task. */
        void onException(Throwable error);
    }
}
