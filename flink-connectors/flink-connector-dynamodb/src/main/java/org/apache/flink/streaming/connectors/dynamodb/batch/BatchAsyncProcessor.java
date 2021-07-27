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
import org.apache.flink.streaming.connectors.dynamodb.batch.concurrent.TimeoutLatch;
import org.apache.flink.streaming.connectors.dynamodb.batch.concurrent.UnboundedTaskExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Batch processor receives batched write requests and submits new tasks to write them. */
public class BatchAsyncProcessor implements Consumer<ProducerWriteRequest<DynamoDbRequest>> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchAsyncProcessor.class);

    private final BlockingQueue<ProducerWriteRequest<DynamoDbRequest>> outgoingMessagesQueue =
            new LinkedBlockingQueue<>();

    private final ExecutorService executor;
    private final UnboundedTaskExecutor taskExecutor;
    private final CountingCompletionService<ProducerWriteResponse> callbackCompletionService;
    private final BatchWriterProvider writerProvider;
    private final ResponseHandler outputMessageHandler;
    private transient TimeoutLatch shutdownLatch;

    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean shutdownLoops = new AtomicBoolean(false);

    public BatchAsyncProcessor(
            ExecutorService completionExecutor,
            ExecutorService taskExecutor,
            BatchWriterProvider writerProvider,
            ResponseHandler outputMessageHandler) {
        this.executor = completionExecutor;
        this.taskExecutor = new UnboundedTaskExecutor();
        this.callbackCompletionService = new CountingCompletionService<>(taskExecutor);
        this.writerProvider = writerProvider;
        this.outputMessageHandler = outputMessageHandler;
    }

    @Override
    public void accept(ProducerWriteRequest<DynamoDbRequest> request) {
        if (shutdown.get() || taskExecutor.isShutdown() || executor.isShutdown()) {
            throw new ProducerException(
                    "BatchAsyncProcessor has been shutdown and can not longer process new messages");
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
        shutdown(); // TODO check shutdown on error parameter
    }

    /** Start processing and completion loops. */
    public void start() {
        executor.execute(
                () -> {
                    while (!shutdownLoops.get()) {
                        processBatchRequests();
                    }
                });
        executor.execute(
                () -> {
                    while (!shutdownLoops.get()) {
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
        if (!shutdown.getAndSet(true)) {
            try {
                LOG.warn("Shutdown has been initiated for BatchAsyncProcessor");
                terminateLoopExecutor(10, TimeUnit.MINUTES);
                terminateTaskExecutor(5, TimeUnit.MINUTES);

                if (callbackCompletionService.getNumberOfTasksInProgress() > 0) {
                    LOG.error(
                            "Task executor was abruptly shut down. "
                                    + callbackCompletionService.getNumberOfTasksInProgress()
                                    + " tasks will not be executed.");
                    throw new ProducerException(
                            "Abruptly shutdown the producer. "
                                    + callbackCompletionService.getNumberOfTasksInProgress()
                                    + " messages in transit might have been lost.");
                }
            } catch (InterruptedException | ExecutionException e) {
                executor.shutdownNow();
                taskExecutor.shutdownNow();
                throw new ProducerException(
                        "Abruptly shutdown the producer. "
                                + callbackCompletionService.getNumberOfTasksInProgress()
                                + " messages in transit might have been lost.",
                        e);
            }
        }
    }

    private void terminateTaskExecutor(long timeout, TimeUnit unit) throws InterruptedException {
        taskExecutor.shutdown();
        if (!taskExecutor.awaitTermination(timeout, unit)) {
            List<Runnable> dropped = taskExecutor.shutdownNow();
            LOG.error(
                    "Task executor was abruptly shut down. "
                            + dropped.size()
                            + " tasks will not be executed.");
        }
    }

    private void terminateLoopExecutor(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException {
        long start = System.nanoTime();
        long timeoutNanos = unit.toNanos(timeout);
        do {
            shutdownLatch.await(100);
            LOG.warn(
                    "Waiting to shutdown loop executor. still {} tasks in progress",
                    callbackCompletionService.getNumberOfTasksInProgress());
        } while (callbackCompletionService.getNumberOfTasksInProgress() > 0
                && (System.nanoTime() - start < timeoutNanos));

        shutdownLoops.getAndSet(true);
        executor.shutdown();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
    }

    private void processBatchRequests() {
        try {
            ProducerWriteRequest<DynamoDbRequest> request = outgoingMessagesQueue.take();
            taskExecutor.submit(writerProvider.createWriter(request));
        } catch (InterruptedException ex) {
            fatalError("Batch request processing failed", ex);
        }
    }

    private void receiveCompletionMessages() {
        try {
            ProducerWriteResponse response = callbackCompletionService.take().get();
            outputMessageHandler.onCompletion(response);
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
