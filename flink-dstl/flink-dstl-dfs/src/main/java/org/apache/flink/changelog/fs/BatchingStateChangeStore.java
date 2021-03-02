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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.lang.Thread.holdsLock;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StateChangeStore} that waits for some configured amount of time before passing the
 * accumulated state changes to the actual store.
 */
@ThreadSafe
class BatchingStateChangeStore implements StateChangeStore {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingStateChangeStore.class);

    private final RetryingExecutor retryingExecutor;
    private final RetryPolicy retryPolicy;
    private final StateChangeStore delegate;
    private final ScheduledExecutorService scheduler;
    private final long scheduleDelayMs;
    private final long sizeThresholdBytes;

    @GuardedBy("scheduled")
    private final Queue<StoreTask> scheduled;

    @GuardedBy("scheduled")
    private long scheduledSizeInBytes;

    @Nullable
    @GuardedBy("scheduled")
    private ScheduledFuture<?> scheduledFuture;

    private volatile Throwable error;

    BatchingStateChangeStore(
            long persistDelayMs,
            long sizeThresholdBytes,
            RetryPolicy retryPolicy,
            StateChangeStore delegate) {
        this(
                persistDelayMs,
                sizeThresholdBytes,
                retryPolicy,
                delegate,
                SchedulerFactory.create(1, "ChangelogRetryScheduler", LOG),
                new RetryingExecutor());
    }

    BatchingStateChangeStore(
            long persistDelayMs,
            long sizeThresholdBytes,
            RetryPolicy retryPolicy,
            StateChangeStore delegate,
            ScheduledExecutorService scheduler,
            RetryingExecutor retryingExecutor) {
        this.scheduleDelayMs = persistDelayMs;
        this.scheduled = new LinkedList<>();
        this.scheduler = scheduler;
        this.retryPolicy = retryPolicy;
        this.retryingExecutor = retryingExecutor;
        this.sizeThresholdBytes = sizeThresholdBytes;
        this.delegate = delegate;
    }

    @Override
    public void save(StoreTask storeTask) {
        Collection<StateChangeSet> changeSets = storeTask.changeSets;
        if (error != null) {
            LOG.debug("don't persist {} changesets, already failed", changeSets.size());
            storeTask.fail(error);
            return;
        }
        LOG.debug("persist {} changeSets", changeSets.size());
        try {
            synchronized (scheduled) {
                scheduled.add(storeTask);
                scheduledSizeInBytes += storeTask.getSize();
                scheduleUploadIfNeeded();
            }
        } catch (Exception e) {
            storeTask.fail(e);
            throw e;
        }
    }

    private void scheduleUploadIfNeeded() {
        checkState(holdsLock(scheduled));
        if (scheduleDelayMs == 0 || scheduledSizeInBytes >= sizeThresholdBytes) {
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
        Collection<StoreTask> tasks;
        synchronized (scheduled) {
            tasks = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledSizeInBytes = 0;
            scheduledFuture = null;
        }
        try {
            if (error != null) {
                tasks.forEach(task -> task.fail(error));
                return;
            }
            retryingExecutor.execute(retryPolicy, () -> delegate.save(tasks));
        } catch (Throwable t) {
            tasks.forEach(task -> task.fail(t));
            if (findThrowable(t, IOException.class).isPresent()) {
                LOG.warn("Caught IO exception while uploading", t);
            } else {
                error = t;
                throw t;
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(1, SECONDS)) {
            LOG.warn("Unable to cleanly shutdown scheduler in 1s");
        }
        ArrayList<StoreTask> drained;
        synchronized (scheduled) {
            drained = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledSizeInBytes = 0;
        }
        drained.forEach(t -> t.fail(new CancellationException())); // todo: review
        retryingExecutor.close();
        delegate.close();
    }
}
