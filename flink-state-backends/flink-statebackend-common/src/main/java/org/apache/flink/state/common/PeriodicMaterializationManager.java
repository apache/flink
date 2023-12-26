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

package org.apache.flink.state.common;

import org.apache.flink.annotation.Internal;
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

import javax.annotation.concurrent.NotThreadSafe;

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
@Internal
public class PeriodicMaterializationManager implements Closeable {

    /** {@link MaterializationRunnable} provider and consumer, i.e. state backend. */
    @NotThreadSafe
    public interface MaterializationTarget {

        /**
         * Initialize state materialization so that materialized data can be persisted durably and
         * included into the checkpoint.
         *
         * @return a tuple of - future snapshot result from the underlying state backend - a {@link
         *     SequenceNumber} identifying the latest change in the changelog
         */
        Optional<MaterializationRunnable> initMaterialization() throws Exception;

        /**
         * Implementations should not trigger materialization until the previous one has been
         * confirmed or failed.
         */
        void handleMaterializationResult(
                SnapshotResult<KeyedStateHandle> materializedSnapshot,
                long materializationID,
                SequenceNumber upTo)
                throws Exception;

        void handleMaterializationFailureOrCancellation(
                long materializationID, SequenceNumber upTo, Throwable cause);

        MaterializationTarget NO_OP =
                new MaterializationTarget() {
                    @Override
                    public Optional<MaterializationRunnable> initMaterialization() {
                        return Optional.empty();
                    }

                    @Override
                    public void handleMaterializationResult(
                            SnapshotResult<KeyedStateHandle> materializedSnapshot,
                            long materializationID,
                            SequenceNumber upTo) {}

                    @Override
                    public void handleMaterializationFailureOrCancellation(
                            long materializationID, SequenceNumber upTo, Throwable cause) {}
                };
    }

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

    private final MaterializationTarget target;

    private final ChangelogMaterializationMetricGroup metrics;

    /** Whether PeriodicMaterializationManager is started. */
    private boolean started = false;

    private final long initialDelay;

    public PeriodicMaterializationManager(
            MailboxExecutor mailboxExecutor,
            ExecutorService asyncOperationsThreadPool,
            String subtaskName,
            AsyncExceptionHandler asyncExceptionHandler,
            MaterializationTarget target,
            ChangelogMaterializationMetricGroup metricGroup,
            long periodicMaterializeDelay,
            int allowedNumberOfFailures,
            String operatorSubtaskId) {
        this(
                mailboxExecutor,
                asyncOperationsThreadPool,
                subtaskName,
                asyncExceptionHandler,
                target,
                metricGroup,
                periodicMaterializeDelay,
                allowedNumberOfFailures,
                operatorSubtaskId,
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                "periodic-materialization-scheduler-" + subtaskName)));
    }

    PeriodicMaterializationManager(
            MailboxExecutor mailboxExecutor,
            ExecutorService asyncOperationsThreadPool,
            String subtaskName,
            AsyncExceptionHandler asyncExceptionHandler,
            MaterializationTarget target,
            ChangelogMaterializationMetricGroup metricGroup,
            long periodicMaterializeDelay,
            int allowedNumberOfFailures,
            String operatorSubtaskId,
            ScheduledExecutorService periodicExecutor) {
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.asyncOperationsThreadPool = checkNotNull(asyncOperationsThreadPool);
        this.subtaskName = checkNotNull(subtaskName);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
        this.metrics = metricGroup;
        this.target = checkNotNull(target);

        this.periodicMaterializeDelay = periodicMaterializeDelay;
        this.allowedNumberOfFailures = allowedNumberOfFailures;
        this.numberOfConsecutiveFailures = new AtomicInteger(0);

        this.periodicExecutor = periodicExecutor;

        this.initialDelay =
                // randomize initial delay to avoid thundering herd problem
                MathUtils.murmurHash(operatorSubtaskId.hashCode()) % periodicMaterializeDelay;
    }

    public void start() {
        // disable periodic materialization when periodicMaterializeDelay is negative
        if (!started && periodicMaterializeDelay >= 0) {

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
                    long triggerTime = System.currentTimeMillis();
                    metrics.reportStartedMaterialization();
                    Optional<MaterializationRunnable> materializationRunnableOptional;
                    try {
                        materializationRunnableOptional = target.initMaterialization();
                    } catch (Exception ex) {
                        metrics.reportFailedMaterialization();
                        throw ex;
                    }

                    if (materializationRunnableOptional.isPresent()) {
                        MaterializationRunnable runnable = materializationRunnableOptional.get();
                        asyncOperationsThreadPool.execute(
                                () ->
                                        asyncMaterializationPhase(
                                                triggerTime,
                                                runnable.getMaterializationRunnable(),
                                                runnable.getMaterializationID(),
                                                runnable.getMaterializedTo()));
                    } else {
                        metrics.reportCompletedMaterialization(
                                System.currentTimeMillis() - triggerTime);
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
            long triggerTime,
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
                                        () -> {
                                            try {
                                                target.handleMaterializationResult(
                                                        snapshotResult, materializationID, upTo);
                                                metrics.reportCompletedMaterialization(
                                                        System.currentTimeMillis() - triggerTime);
                                            } catch (Exception ex) {
                                                metrics.reportFailedMaterialization();
                                            }
                                        },
                                        "Task %s update materializedSnapshot up to changelog sequence number: %s.",
                                        subtaskName,
                                        upTo);

                                scheduleNextMaterialization();
                            } else if (throwable instanceof CancellationException) {
                                // can happen e.g. due to task cancellation
                                LOG.info("materialization cancelled", throwable);
                                notifyFailureOrCancellation(materializationID, upTo, throwable);
                                scheduleNextMaterialization();
                            } else {
                                // if failed
                                notifyFailureOrCancellation(materializationID, upTo, throwable);
                                metrics.reportFailedMaterialization();
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

    private void notifyFailureOrCancellation(
            long materializationId, SequenceNumber upTo, Throwable cause) {
        mailboxExecutor.execute(
                () ->
                        target.handleMaterializationFailureOrCancellation(
                                materializationId, upTo, cause),
                "Task %s materialization: %d, upTo: %s, failed or canceled.",
                subtaskName,
                materializationId,
                upTo);
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
        scheduleNextMaterialization(periodicMaterializeDelay);
    }

    // task thread and asyncOperationsThreadPool can access this method
    private synchronized void scheduleNextMaterialization(long delay) {
        if (started && !periodicExecutor.isShutdown()) {

            LOG.info(
                    "Task {} schedules the next materialization in {} seconds",
                    subtaskName,
                    delay / 1000);

            periodicExecutor.schedule(this::triggerMaterialization, delay, TimeUnit.MILLISECONDS);
        }
    }

    /** A {@link Runnable} representing the materialization and the associated metadata. */
    public static class MaterializationRunnable {
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

        public SequenceNumber getMaterializedTo() {
            return materializedTo;
        }

        public long getMaterializationID() {
            return materializationID;
        }
    }
}
