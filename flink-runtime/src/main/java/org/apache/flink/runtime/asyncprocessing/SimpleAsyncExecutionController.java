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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.core.asyncprocessing.AsyncFutureImpl;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.util.function.CheckedSupplier;

import javax.annotation.Nullable;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/** The {@link SimpleAsyncExecutionController} is responsible for handling simple runnable tasks. */
public class SimpleAsyncExecutionController<K>
        extends AsyncExecutionController<K, SimpleAsyncExecutionController.RunnableTask<K, ?>> {

    public SimpleAsyncExecutionController(
            MailboxExecutor mailboxExecutor,
            AsyncFutureImpl.AsyncFrameworkExceptionHandler exceptionHandler,
            ExecutorService asyncThreadPool,
            DeclarationManager declarationManager,
            EpochManager.ParallelMode epochParallelMode,
            int maxParallelism,
            int batchSize,
            long bufferTimeout,
            int maxInFlightRecords,
            @Nullable SwitchContextListener<K> switchContextListener,
            @Nullable MetricGroup metricGroup) {
        super(
                mailboxExecutor,
                exceptionHandler,
                new TaskExecutor<>(asyncThreadPool),
                declarationManager,
                epochParallelMode,
                maxParallelism,
                batchSize,
                bufferTimeout,
                maxInFlightRecords,
                switchContextListener,
                metricGroup);
    }

    /**
     * Submit a {@link AsyncRequest} to this AsyncExecutionController and trigger it if needed.
     *
     * @param checkedSupplier the runnable to execute.
     * @param allowOverdraft whether to allow overdraft.
     * @return the state future.
     */
    public <R> InternalAsyncFuture<R> handleRequest(
            CheckedSupplier<R> checkedSupplier, boolean allowOverdraft) {
        InternalAsyncFuture<R> asyncFuture = asyncFutureFactory.create(currentContext);
        RunnableTask<K, R> request =
                new RunnableTask<>(currentContext, false, asyncFuture, checkedSupplier);

        handleRequest(request, allowOverdraft);
        return asyncFuture;
    }

    public static class RunnableTask<K, RET> extends AsyncRequest<K> {

        final CheckedSupplier<RET> runnable;

        public RunnableTask(
                RecordContext<K> context,
                boolean sync,
                InternalAsyncFuture<RET> asyncFuture,
                CheckedSupplier<RET> runnable) {
            super(context, sync, asyncFuture);
            this.runnable = runnable;
        }

        private void run() throws Exception {
            asyncFuture.complete(runnable.get());
        }
    }

    static class TaskExecutor<K> implements AsyncExecutor<RunnableTask<K, ?>> {

        private final ExecutorService taskExecutorService;
        private final boolean managedExecutor;

        public TaskExecutor(ExecutorService taskExecutorService) {
            this.taskExecutorService = taskExecutorService;
            this.managedExecutor = false;
        }

        @Override
        public CompletableFuture<Void> executeBatchRequests(
                AsyncRequestContainer<RunnableTask<K, ?>> asyncRequestContainer) {
            if (asyncRequestContainer.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            RunnableContainer<K> container = (RunnableContainer<K>) asyncRequestContainer;
            LinkedList<RunnableTask<K, ?>> requests = container.requests;
            while (!requests.isEmpty()) {
                RunnableTask<K, ?> request = requests.poll();
                if (request != null) {
                    taskExecutorService.submit(
                            () -> {
                                try {
                                    request.run();
                                } catch (Exception e) {
                                    request.getFuture()
                                            .completeExceptionally("Async task failed.", e);
                                }
                            });
                }
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public AsyncRequestContainer<RunnableTask<K, ?>> createRequestContainer() {
            return new RunnableContainer<>();
        }

        @Override
        public void executeRequestSync(RunnableTask<K, ?> asyncRequest) {
            try {
                asyncRequest.run();
            } catch (Exception e) {
                asyncRequest.getFuture().completeExceptionally("Task failed.", e);
            }
        }

        @Override
        public boolean fullyLoaded() {
            // Implementation for checking if fully loaded
            return false;
        }

        @Override
        public void shutdown() {
            if (managedExecutor) {
                taskExecutorService.shutdown();
            }
        }

        static class RunnableContainer<K> implements AsyncRequestContainer<RunnableTask<K, ?>> {

            LinkedList<RunnableTask<K, ?>> requests = new LinkedList<>();

            @Override
            public void offer(RunnableTask<K, ?> stateRequest) {
                requests.add(stateRequest);
            }

            @Override
            public boolean isEmpty() {
                return requests.isEmpty();
            }
        }
    }
}
