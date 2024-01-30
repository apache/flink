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

package org.apache.flink.table.gateway.service.operation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.results.FetchOrientation;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.NotReadyResult;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlCancelException;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.util.IOUtils;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

/** Manager for the {@link Operation}. */
@Internal
public class OperationManager {

    private static final Logger LOG = LoggerFactory.getLogger(OperationManager.class);

    /** The lock that controls the visit of the {@link OperationManager}'s state. */
    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();

    private final Map<OperationHandle, Operation> submittedOperations;
    private final ExecutorService service;
    /**
     * Operation lock is used to control the execution among the {@link Operation}s. The reason why
     * using the lock to control the execution in sequence is the managers, e.g. CatalogManager is
     * not thread safe.
     */
    private final Semaphore operationLock;

    private boolean isRunning;

    public OperationManager(ExecutorService service) {
        this.service = service;
        this.submittedOperations = new HashMap<>();
        this.operationLock = new Semaphore(1);
        this.isRunning = true;
    }

    /**
     * Submit the operation to the {@link OperationManager}. The {@link OperationManager} manages
     * the lifecycle of the {@link Operation}, including register resources, fire the execution and
     * so on.
     *
     * @param executor Worker to execute.
     * @return OperationHandle to fetch the results or check the status.
     */
    public OperationHandle submitOperation(Callable<ResultSet> executor) {
        OperationHandle handle = OperationHandle.create();
        Operation operation =
                new Operation(
                        handle,
                        () -> {
                            ResultSet resultSet = executor.call();
                            return ResultFetcher.fromResults(
                                    handle, resultSet.getResultSchema(), resultSet.getData());
                        });

        submitOperationInternal(handle, operation);
        return handle;
    }

    /**
     * Submit the operation to the {@link OperationManager}. The {@link OperationManager} manges the
     * lifecycle of the {@link Operation}, including register resources, fire the execution and so
     * on.
     *
     * @param fetcherSupplier offer the fetcher to get the results.
     * @return OperationHandle to fetch the results or check the status.
     */
    public OperationHandle submitOperation(
            Function<OperationHandle, ResultFetcher> fetcherSupplier) {
        OperationHandle handle = OperationHandle.create();
        Operation operation = new Operation(handle, () -> fetcherSupplier.apply(handle));
        submitOperationInternal(handle, operation);
        return handle;
    }

    /**
     * Cancel the execution of the operation.
     *
     * @param operationHandle identifies the {@link Operation}.
     */
    public void cancelOperation(OperationHandle operationHandle) {
        getOperation(operationHandle).cancel();
    }

    /**
     * Close the operation and release all resources used by the {@link Operation}.
     *
     * @param operationHandle identifies the {@link Operation}.
     */
    public void closeOperation(OperationHandle operationHandle) {
        writeLock(
                () -> {
                    Operation opToRemove = submittedOperations.remove(operationHandle);
                    if (opToRemove != null) {
                        opToRemove.close();
                    }
                });
    }

    public void awaitOperationTermination(OperationHandle operationHandle) throws Exception {
        getOperation(operationHandle).awaitTermination();
    }

    /**
     * Get the {@link OperationInfo} of the operation.
     *
     * @param operationHandle identifies the {@link Operation}.
     */
    public OperationInfo getOperationInfo(OperationHandle operationHandle) {
        return getOperation(operationHandle).getOperationInfo();
    }

    /**
     * Get the {@link ResolvedSchema} of the operation.
     *
     * @param operationHandle identifies the {@link Operation}.
     */
    public ResolvedSchema getOperationResultSchema(OperationHandle operationHandle)
            throws Exception {
        return getOperation(operationHandle).getResultSchema();
    }

    /**
     * Get the results of the operation.
     *
     * @param operationHandle identifies the {@link Operation}.
     * @param token identifies which batch of data to fetch.
     * @param maxRows the maximum number of rows to fetch.
     * @return ResultSet contains the results.
     */
    public ResultSet fetchResults(OperationHandle operationHandle, long token, int maxRows) {
        return getOperation(operationHandle).fetchResults(token, maxRows);
    }

    public ResultSet fetchResults(
            OperationHandle operationHandle, FetchOrientation orientation, int maxRows) {
        return getOperation(operationHandle).fetchResults(orientation, maxRows);
    }

    /** Closes the {@link OperationManager} and all operations. */
    public void close() {
        stateLock.writeLock().lock();
        Exception closeException = null;
        try {
            isRunning = false;
            IOUtils.closeAll(submittedOperations.values(), Throwable.class);
        } catch (Exception e) {
            closeException = e;
        } finally {
            submittedOperations.clear();
            stateLock.writeLock().unlock();
        }
        // wait all operations closed
        try {
            operationLock.acquire();
        } catch (Exception e) {
            LOG.error("Failed to wait all operation closed.", e);
        } finally {
            operationLock.release();
        }
        LOG.debug("Closes the Operation Manager.");
        if (closeException != null) {
            throw new SqlExecutionException(
                    "Failed to close the OperationManager.", closeException);
        }
    }

    // -------------------------------------------------------------------------------------------

    /** Operation to manage the execution, results and so on. */
    @VisibleForTesting
    public class Operation implements AutoCloseable {

        private static final long WAIT_CLEAN_UP_MILLISECONDS = 5_000;

        private final OperationHandle operationHandle;

        private final AtomicReference<OperationStatus> status;
        private final Callable<ResultFetcher> resultSupplier;

        private volatile FutureTask<?> invocation;
        private volatile ResultFetcher resultFetcher;
        private volatile SqlExecutionException operationError;

        public Operation(OperationHandle operationHandle, Callable<ResultFetcher> resultSupplier) {
            this.operationHandle = operationHandle;
            this.status = new AtomicReference<>(OperationStatus.INITIALIZED);
            this.resultSupplier = resultSupplier;
        }

        void runBefore() {
            updateState(OperationStatus.RUNNING);
        }

        void runAfter() {
            updateState(OperationStatus.FINISHED);
        }

        public void run() {
            try {
                operationLock.acquire();
                LOG.debug(
                        String.format(
                                "Operation %s acquires the operation lock.", operationHandle));
                updateState(OperationStatus.PENDING);
                Runnable work =
                        () -> {
                            try {
                                runBefore();
                                resultFetcher = resultSupplier.call();
                                runAfter();
                            } catch (InterruptedException e) {
                                // User cancel the execution.
                                LOG.error(
                                        String.format(
                                                "Operation %s is interrupted.", operationHandle),
                                        e);
                            } catch (Throwable t) {
                                processThrowable(t);
                            }
                        };
                // The returned future by the ExecutorService will not wrap the
                // done method.
                FutureTask<Void> copiedTask =
                        new FutureTask<Void>(work, null) {
                            @Override
                            protected void done() {
                                LOG.debug(
                                        String.format(
                                                "Release the operation lock: %s when task completes.",
                                                operationHandle));
                                operationLock.release();
                            }
                        };
                service.submit(copiedTask);
                invocation = copiedTask;
                // If it is canceled or closed, terminate the invocation.
                OperationStatus current = status.get();
                if (current == OperationStatus.CLOSED || current == OperationStatus.CANCELED) {
                    LOG.debug(
                            String.format(
                                    "The current status is %s after updating the operation %s status to %s. Close the resources.",
                                    current, operationHandle, OperationStatus.PENDING));
                    closeResources();
                }
            } catch (Throwable t) {
                processThrowable(t);
                throw new SqlGatewayException(
                        "Failed to submit the operation to the thread pool.", t);
            } finally {
                if (invocation == null) {
                    // failed to submit to the thread pool and release the lock.
                    LOG.debug(
                            String.format(
                                    "Operation %s releases the operation lock when failed to submit the operation to the pool.",
                                    operationHandle));
                    operationLock.release();
                }
            }
        }

        public void cancel() {
            updateState(OperationStatus.CANCELED);
            closeResources();
        }

        public void close() {
            updateState(OperationStatus.CLOSED);
            closeResources();
        }

        public ResultSet fetchResults(long token, int maxRows) {
            return fetchResultsInternal(() -> resultFetcher.fetchResults(token, maxRows));
        }

        public ResultSet fetchResults(FetchOrientation orientation, int maxRows) {
            return fetchResultsInternal(() -> resultFetcher.fetchResults(orientation, maxRows));
        }

        public ResolvedSchema getResultSchema() throws Exception {
            awaitTermination();
            OperationStatus current = status.get();
            if (current != OperationStatus.FINISHED) {
                throw new IllegalStateException(
                        String.format(
                                "The result schema is available when the Operation is in FINISHED state but the current status is %s.",
                                status));
            }
            return resultFetcher.getResultSchema();
        }

        public OperationInfo getOperationInfo() {
            return new OperationInfo(status.get(), operationError);
        }

        public void awaitTermination() throws Exception {
            synchronized (status) {
                while (!status.get().isTerminalStatus()) {
                    status.wait();
                }
            }
            OperationStatus current = status.get();
            if (current == OperationStatus.ERROR) {
                throw operationError;
            }
        }

        private ResultSet fetchResultsInternal(Supplier<ResultSet> results) {
            OperationStatus currentStatus = status.get();

            if (currentStatus == OperationStatus.ERROR) {
                throw operationError;
            } else if (currentStatus == OperationStatus.FINISHED) {
                return results.get();
            } else if (currentStatus == OperationStatus.RUNNING
                    || currentStatus == OperationStatus.PENDING
                    || currentStatus == OperationStatus.INITIALIZED) {
                return NotReadyResult.INSTANCE;
            } else {
                throw new SqlGatewayException(
                        String.format(
                                "Can not fetch results from the %s in %s status.",
                                operationHandle, currentStatus));
            }
        }

        private void updateState(OperationStatus toStatus) {
            OperationStatus currentStatus;
            do {
                currentStatus = status.get();
                boolean isValid = OperationStatus.isValidStatusTransition(currentStatus, toStatus);
                if (!isValid) {
                    String message =
                            String.format(
                                    "Failed to convert the Operation Status from %s to %s for %s.",
                                    currentStatus, toStatus, operationHandle);
                    throw new SqlGatewayException(message);
                }
            } while (!status.compareAndSet(currentStatus, toStatus));

            synchronized (status) {
                status.notifyAll();
            }

            LOG.debug(
                    String.format(
                            "Convert operation %s from %s to %s.",
                            operationHandle, currentStatus, toStatus));
        }

        private void closeResources() {
            if (invocation != null && !invocation.isDone()) {
                invocation.cancel(true);
                waitTaskCleanup(invocation);
                LOG.debug(String.format("Cancel the operation %s.", operationHandle));
            }

            if (resultFetcher != null) {
                resultFetcher.close();
            }
        }

        private void processThrowable(Throwable t) {
            String msg = String.format("Failed to execute the operation %s.", operationHandle);
            LOG.error(msg, t);
            operationError = new SqlExecutionException(msg, t);
            // Update status should be placed at last. Because the client is able to fetch exception
            // when status is error.
            updateState(OperationStatus.ERROR);
        }

        private void waitTaskCleanup(FutureTask<?> invocation) {
            // thread is cleaned async, waiting for a while
            Deadline deadline = Deadline.fromNow(Duration.ofMillis(WAIT_CLEAN_UP_MILLISECONDS));
            while (deadline.hasTimeLeft()) {
                Optional<Thread> threadOptional = getThreadInFuture(invocation);
                if (!threadOptional.isPresent()) {
                    // thread has been cleaned up
                    return;
                }
                // try to release the use of the processor to let the task finish its cleanup.
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            }
            Optional<Thread> threadOptional = getThreadInFuture(invocation);
            // Currently, SQL Gateway still doesn't have health reporter to notify the users the
            // resource leak or HA to restart the running process. So we just dump the thread and
            // throw an exception to notify the users.
            threadOptional.ifPresent(this::throwExceptionWithThreadStackTrace);
        }

        private Optional<Thread> getThreadInFuture(FutureTask<?> invocation) {
            try {
                Class<?> k = FutureTask.class;
                Field runnerField = k.getDeclaredField("runner");
                runnerField.setAccessible(true);
                Thread t = (Thread) runnerField.get(invocation);
                return Optional.of(t);
            } catch (Throwable e) {
                // can't get thread
                return Optional.empty();
            }
        }

        private void throwExceptionWithThreadStackTrace(Thread thread) {
            StackTraceElement[] stack = thread.getStackTrace();
            StringBuilder stackTraceStr = new StringBuilder();
            for (StackTraceElement e : stack) {
                stackTraceStr.append("\tat ").append(e).append("\n");
            }

            String msg =
                    String.format(
                            "Operation '%s' did not react to \"Future.cancel(true)\" and "
                                    + "is stuck for %s seconds in method.\n"
                                    + "Thread name: %s, thread state: %s, thread stacktrace:\n%s",
                            operationHandle,
                            WAIT_CLEAN_UP_MILLISECONDS / 1000,
                            thread.getName(),
                            thread.getState(),
                            stackTraceStr);
            throw new SqlCancelException(msg);
        }
    }

    // -------------------------------------------------------------------------------------------

    @VisibleForTesting
    public int getOperationCount() {
        return submittedOperations.size();
    }

    @VisibleForTesting
    public Operation getOperation(OperationHandle operationHandle) {
        return readLock(
                () -> {
                    Operation operation = submittedOperations.get(operationHandle);
                    if (operation == null) {
                        throw new SqlGatewayException(
                                String.format(
                                        "Can not find the submitted operation in the OperationManager with the %s.",
                                        operationHandle));
                    }
                    return operation;
                });
    }

    private void submitOperationInternal(OperationHandle handle, Operation operation) {
        writeLock(() -> submittedOperations.put(handle, operation));
        operation.run();
    }

    private void writeLock(Runnable runner) {
        stateLock.writeLock().lock();
        try {
            if (!isRunning) {
                throw new SqlGatewayException("The OperationManager is closed.");
            }
            runner.run();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private <T> T readLock(Supplier<T> supplier) {
        stateLock.readLock().lock();
        try {
            if (!isRunning) {
                throw new SqlGatewayException("The OperationManager is closed.");
            }
            return supplier.get();
        } finally {
            stateLock.readLock().unlock();
        }
    }
}
