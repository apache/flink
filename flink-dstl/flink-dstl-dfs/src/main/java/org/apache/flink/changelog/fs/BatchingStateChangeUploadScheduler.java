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

package org.apache.flink.changelog.fs;

import org.apache.flink.changelog.fs.StateChangeUploader.UploadTasksResult;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.AvailabilityProvider.AvailabilityHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.lang.Thread.holdsLock;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StateChangeUploadScheduler} that waits for some configured amount of time before passing
 * the accumulated state changes to the actual store.
 */
@ThreadSafe
class BatchingStateChangeUploadScheduler implements StateChangeUploadScheduler {
    private static final Logger LOG =
            LoggerFactory.getLogger(BatchingStateChangeUploadScheduler.class);

    private final RetryingExecutor retryingExecutor;
    private final RetryPolicy retryPolicy;
    private final StateChangeUploader delegate;
    private final ScheduledExecutorService scheduler;
    private final long scheduleDelayMs;
    private final long sizeThresholdBytes;

    /**
     * The lock is used to synchronize concurrent accesses:
     *
     * <ul>
     *   <li>task thread and {@link #scheduler} thread to {@link #scheduled} tasks
     *   <li>task thread and {@link #retryingExecutor uploader} thread to {@link #uploadThrottle}
     * </ul>
     *
     * <p>These code paths are independent, but a single lock is used for simplicity.
     */
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final Queue<UploadTask> scheduled;

    @GuardedBy("lock")
    private long scheduledBytesCounter;

    private final AvailabilityHelper availabilityHelper;

    /**
     * There should be at most one scheduled future, so that changes are batched according to
     * settings.
     */
    @Nullable
    @GuardedBy("lock")
    private ScheduledFuture<?> scheduledFuture;

    @Nullable
    @GuardedBy("this")
    private Throwable errorUnsafe;

    @GuardedBy("lock")
    private final UploadThrottle uploadThrottle;

    private final Histogram uploadBatchSizes;

    BatchingStateChangeUploadScheduler(
            long persistDelayMs,
            long sizeThresholdBytes,
            RetryPolicy retryPolicy,
            StateChangeUploader delegate,
            int numUploadThreads,
            long maxBytesInFlight,
            ChangelogStorageMetricGroup metricGroup) {
        this(
                persistDelayMs,
                sizeThresholdBytes,
                maxBytesInFlight,
                retryPolicy,
                delegate,
                SchedulerFactory.create(1, "ChangelogUploadScheduler", LOG),
                new RetryingExecutor(numUploadThreads, metricGroup.getAttemptsPerUpload()),
                metricGroup);
    }

    BatchingStateChangeUploadScheduler(
            long persistDelayMs,
            long sizeThresholdBytes,
            long maxBytesInFlight,
            RetryPolicy retryPolicy,
            StateChangeUploader delegate,
            ScheduledExecutorService scheduler,
            RetryingExecutor retryingExecutor,
            ChangelogStorageMetricGroup metricGroup) {
        checkArgument(
                sizeThresholdBytes <= maxBytesInFlight,
                "sizeThresholdBytes (%s) must not exceed maxBytesInFlight (%s)",
                sizeThresholdBytes,
                maxBytesInFlight);
        this.scheduleDelayMs = persistDelayMs;
        this.scheduled = new LinkedList<>();
        this.scheduler = scheduler;
        this.retryPolicy = retryPolicy;
        this.retryingExecutor = retryingExecutor;
        this.sizeThresholdBytes = sizeThresholdBytes;
        this.delegate = delegate;
        this.uploadThrottle = new UploadThrottle(maxBytesInFlight);
        this.availabilityHelper = new AvailabilityHelper();
        this.availabilityHelper.resetAvailable();
        this.uploadBatchSizes = metricGroup.getUploadBatchSizes();
        metricGroup.registerUploadQueueSizeGauge(
                () -> {
                    synchronized (scheduled) {
                        return scheduled.size();
                    }
                });
    }

    @Override
    public void upload(UploadTask uploadTask) throws IOException {
        Throwable error = getErrorSafe();
        if (error != null) {
            LOG.debug("don't persist {} changesets, already failed", uploadTask.changeSets.size());
            uploadTask.fail(error);
            return;
        }
        LOG.debug("persist {} changeSets", uploadTask.changeSets.size());
        try {
            long size = uploadTask.getSize();
            synchronized (lock) {
                while (!uploadThrottle.hasCapacity()) {
                    lock.wait();
                }
                uploadThrottle.seizeCapacity(size);
                if (!uploadThrottle.hasCapacity()) {
                    availabilityHelper.resetUnavailable();
                }
                scheduledBytesCounter += size;
                scheduled.add(wrapWithSizeUpdate(uploadTask, size));
                scheduleUploadIfNeeded();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            uploadTask.fail(e);
            throw new IOException(e);
        } catch (Exception e) {
            uploadTask.fail(e);
            throw e;
        }
    }

    private void releaseCapacity(long size) {
        CompletableFuture<?> toNotify = null;
        synchronized (lock) {
            boolean hadCapacityBefore = uploadThrottle.hasCapacity();
            uploadThrottle.releaseCapacity(size);
            lock.notifyAll();
            if (!hadCapacityBefore && uploadThrottle.hasCapacity()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
        }
        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    private void scheduleUploadIfNeeded() {
        checkState(holdsLock(lock));
        if (scheduleDelayMs == 0 || scheduledBytesCounter >= sizeThresholdBytes) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduledFuture = null;
            }
            drainAndSave();
        } else if (scheduledFuture == null) {
            scheduledFuture = scheduler.schedule(this::drainAndSave, scheduleDelayMs, MILLISECONDS);
        }
    }

    private void drainAndSave() {
        Collection<UploadTask> tasks;
        synchronized (lock) {
            tasks = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledBytesCounter = 0;
            scheduledFuture = null;
        }
        try {
            Throwable error = getErrorSafe();
            if (error != null) {
                tasks.forEach(task -> task.fail(error));
                return;
            }
            uploadBatchSizes.update(tasks.size());
            retryingExecutor.execute(retryPolicy, asRetriableAction(tasks));
        } catch (Throwable t) {
            tasks.forEach(task -> task.fail(t));
            if (findThrowable(t, IOException.class).isPresent()) {
                LOG.warn("Caught IO exception while uploading", t);
            } else {
                setErrorSafe(t);
                throw t;
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(5, SECONDS)) {
            LOG.warn("Unable to cleanly shutdown scheduler in 5s");
        }
        ArrayList<UploadTask> drained;
        synchronized (lock) {
            drained = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledBytesCounter = 0;
        }
        CancellationException ce = new CancellationException();
        drained.forEach(task -> task.fail(ce));
        retryingExecutor.close();
        delegate.close();
    }

    private synchronized Throwable getErrorSafe() {
        return errorUnsafe;
    }

    private synchronized void setErrorSafe(Throwable t) {
        errorUnsafe = t;
    }

    private UploadTask wrapWithSizeUpdate(UploadTask uploadTask, long size) {
        return new UploadTask(
                uploadTask.changeSets,
                result -> {
                    try {
                        releaseCapacity(size);
                    } finally {
                        uploadTask.complete(result);
                    }
                },
                (result, error) -> {
                    try {
                        releaseCapacity(size);
                    } finally {
                        uploadTask.fail(error);
                    }
                });
    }

    @Override
    public AvailabilityProvider getAvailabilityProvider() {
        // This method can be called by multiple (task) threads.
        // Though the field itself is final, implementation is generally not thread-safe.
        // However, in case of reading stale AvailabilityHelper.availableFuture
        // the task will either be notified about availability immediately;
        // or back-pressured hard trying to seize capacity in upload()
        return availabilityHelper;
    }

    private RetryingExecutor.RetriableAction<UploadTasksResult> asRetriableAction(
            Collection<UploadTask> tasks) {
        return new RetryingExecutor.RetriableAction<UploadTasksResult>() {
            @Override
            public UploadTasksResult tryExecute() throws Exception {
                return delegate.upload(tasks);
            }

            @Override
            public void completeWithResult(UploadTasksResult uploadTasksResult) {
                uploadTasksResult.complete();
            }

            @Override
            public void discardResult(UploadTasksResult uploadTasksResult) throws Exception {
                uploadTasksResult.discard();
            }

            @Override
            public void handleFailure(Throwable throwable) {
                tasks.forEach(task -> task.fail(throwable));
            }
        };
    }
}
