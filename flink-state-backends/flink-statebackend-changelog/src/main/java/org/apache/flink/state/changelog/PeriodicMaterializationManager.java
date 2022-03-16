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

package org.apache.flink.state.changelog;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stateless Materialization Manager. */
class PeriodicMaterializationManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicMaterializationManager.class);

    /** task mailbox executor, execute from Task Thread. */
    private final MailboxExecutor mailboxExecutor;

    /** Async thread pool, to complete async phase of materialization. */
    private final ExecutorService asyncOperationsThreadPool;

    /** scheduled executor, periodically trigger materialization. */
    private final ScheduledExecutorService periodicExecutor;

    private final AsyncExceptionHandler asyncExceptionHandler;

    private final String subtaskName;

    private final long periodicMaterializeDelay;

    /** Allowed number of consecutive materialization failures. */
    private final int allowedNumberOfFailures;

    /** Number of consecutive materialization failures. */
    private final AtomicInteger numberOfConsecutiveFailures;

    private final ChangelogKeyedStateBackend<?> keyedStateBackend;

    /** Whether PeriodicMaterializationManager is started. */
    private boolean started = false;

    private final long initialDelay;

    PeriodicMaterializationManager(
            MailboxExecutor mailboxExecutor,
            ExecutorService asyncOperationsThreadPool,
            String subtaskName,
            AsyncExceptionHandler asyncExceptionHandler,
            ChangelogKeyedStateBackend<?> keyedStateBackend,
            long periodicMaterializeDelay,
            int allowedNumberOfFailures,
            String operatorSubtaskId) {
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.asyncOperationsThreadPool = checkNotNull(asyncOperationsThreadPool);
        this.subtaskName = checkNotNull(subtaskName);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
        this.keyedStateBackend = checkNotNull(keyedStateBackend);

        this.periodicMaterializeDelay = periodicMaterializeDelay;
        this.allowedNumberOfFailures = allowedNumberOfFailures;
        this.numberOfConsecutiveFailures = new AtomicInteger(0);

        this.periodicExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                "periodic-materialization-scheduler-" + subtaskName));

        this.initialDelay =
                // randomize initial delay to avoid thundering herd problem
                MathUtils.murmurHash(operatorSubtaskId.hashCode()) % periodicMaterializeDelay;
    }

    public void start() {
        if (!started) {

            started = true;

            LOG.info("Task {} starts periodic materialization", subtaskName);

            scheduleNextMaterialization(initialDelay);
        }
    }

    // task thread and task canceler can access this method
    public synchronized void close() {

        LOG.info("Shutting down PeriodicMaterializationManager.");

        if (!periodicExecutor.isShutdown()) {
            periodicExecutor.shutdownNow();
        }
    }

    @VisibleForTesting
    public void triggerMaterialization() {
        mailboxExecutor.execute(
                () -> {
                    Optional<MaterializationRunnable> materializationRunnableOptional =
                            keyedStateBackend.initMaterialization();

                    if (materializationRunnableOptional.isPresent()) {
                        MaterializationRunnable runnable = materializationRunnableOptional.get();
                        asyncOperationsThreadPool.execute(
                                () ->
                                        asyncMaterializationPhase(
                                                runnable.getMaterializationRunnable(),
                                                runnable.getMaterializationID(),
                                                runnable.getMaterializedTo()));
                    } else {
                        scheduleNextMaterialization();

                        LOG.info(
                                "Task {} has no state updates since last materialization, "
                                        + "skip this one and schedule the next one in {} seconds",
                                subtaskName,
                                periodicMaterializeDelay / 1000);
                    }
                },
                "materialization");
    }

    private void asyncMaterializationPhase(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> materializedRunnableFuture,
            long materializationID,
            SequenceNumber upTo) {

        uploadSnapshot(materializedRunnableFuture)
                .whenComplete(
                        (snapshotResult, throwable) -> {
                            // if succeed
                            if (throwable == null) {
                                numberOfConsecutiveFailures.set(0);

                                mailboxExecutor.execute(
                                        () ->
                                                keyedStateBackend.updateChangelogSnapshotState(
                                                        snapshotResult, materializationID, upTo),
                                        "Task {} update materializedSnapshot up to changelog sequence number: {}",
                                        subtaskName,
                                        upTo);

                                scheduleNextMaterialization();
                            } else if (throwable instanceof CancellationException) {
                                // can happen e.g. due to task cancellation
                                LOG.info("materialization cancelled", throwable);
                                scheduleNextMaterialization();
                            } else {
                                // if failed
                                int retryTime = numberOfConsecutiveFailures.incrementAndGet();

                                if (retryTime <= allowedNumberOfFailures) {

                                    LOG.info(
                                            "Task {} asynchronous part of materialization is not completed for the {} time.",
                                            subtaskName,
                                            retryTime,
                                            throwable);

                                    scheduleNextMaterialization();
                                } else {
                                    // Fail the task externally, this causes task failover
                                    asyncExceptionHandler.handleAsyncException(
                                            "Task "
                                                    + subtaskName
                                                    + " fails to complete the asynchronous part of materialization",
                                            throwable);
                                }
                            }
                        });
    }

    private CompletableFuture<SnapshotResult<KeyedStateHandle>> uploadSnapshot(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> materializedRunnableFuture) {

        FileSystemSafetyNet.initializeSafetyNetForThread();
        CompletableFuture<SnapshotResult<KeyedStateHandle>> result = new CompletableFuture<>();
        try {
            FutureUtils.runIfNotDoneAndGet(materializedRunnableFuture);

            LOG.debug("Task {} finishes asynchronous part of materialization.", subtaskName);

            result.complete(materializedRunnableFuture.get());
        } catch (Exception e) {

            result.completeExceptionally(e);
            discardFailedUploads(materializedRunnableFuture);
        } finally {
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }

        return result;
    }

    private void discardFailedUploads(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> materializedRunnableFuture) {

        LOG.info("Task {} cleanup asynchronous runnable for materialization.", subtaskName);

        if (materializedRunnableFuture != null) {
            // materialization has started
            if (!materializedRunnableFuture.cancel(true)) {
                try {
                    StateObject stateObject = materializedRunnableFuture.get();
                    if (stateObject != null) {
                        stateObject.discardState();
                    }
                } catch (Exception ex) {
                    LOG.debug(
                            "Task "
                                    + subtaskName
                                    + " cancelled execution of snapshot future runnable. "
                                    + "Cancellation produced the following "
                                    + "exception, which is expected and can be ignored.",
                            ex);
                }
            }
        }
    }

    private void scheduleNextMaterialization() {
        scheduleNextMaterialization(0);
    }

    // task thread and asyncOperationsThreadPool can access this method
    private synchronized void scheduleNextMaterialization(long offset) {
        if (started && !periodicExecutor.isShutdown()) {

            LOG.info(
                    "Task {} schedules the next materialization in {} seconds",
                    subtaskName,
                    periodicMaterializeDelay / 1000);

            periodicExecutor.schedule(
                    this::triggerMaterialization,
                    periodicMaterializeDelay + offset,
                    TimeUnit.MILLISECONDS);
        }
    }

    static class MaterializationRunnable {
        private final RunnableFuture<SnapshotResult<KeyedStateHandle>> materializationRunnable;

        private final long materializationID;

        /**
         * The {@link SequenceNumber} up to which the state is materialized, exclusive. This
         * indicates the non-materialized part of the current changelog.
         */
        private final SequenceNumber materializedTo;

        public MaterializationRunnable(
                RunnableFuture<SnapshotResult<KeyedStateHandle>> materializationRunnable,
                long materializationID,
                SequenceNumber materializedTo) {
            this.materializationRunnable = materializationRunnable;
            this.materializedTo = materializedTo;
            this.materializationID = materializationID;
        }

        RunnableFuture<SnapshotResult<KeyedStateHandle>> getMaterializationRunnable() {
            return materializationRunnable;
        }

        SequenceNumber getMaterializedTo() {
            return materializedTo;
        }

        public long getMaterializationID() {
            return materializationID;
        }
    }
}
