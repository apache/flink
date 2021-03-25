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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.holdsLock;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StateChangeStore} that waits for some configured amount of time before passing the
 * accumulated state changes to the actual store. The writes are executed asynchronously.
 */
@ThreadSafe
class BatchingStateChangeStore implements StateChangeStore {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingStateChangeStore.class);

    private final StateChangeStore delegate;
    private final ScheduledExecutorService scheduler;
    private final long scheduleDelayMs;
    private final long sizeThresholdBytes;

    @GuardedBy("scheduled")
    private final Queue<StoreTask> scheduled;

    @GuardedBy("scheduled")
    private long scheduledSizeInBytes;

    @GuardedBy("scheduled")
    private Future<?> scheduledFuture = CompletableFuture.completedFuture(null);

    private AtomicReference<Throwable> error = new AtomicReference<>(null);

    BatchingStateChangeStore(
            long persistDelayMs, long sizeThresholdBytes, StateChangeStore delegate) {
        this(
                persistDelayMs,
                sizeThresholdBytes,
                delegate,
                SchedulerFactory.create(1, "ChangelogRetryScheduler", LOG));
    }

    BatchingStateChangeStore(
            long persistDelayMs,
            long sizeThresholdBytes,
            StateChangeStore delegate,
            ScheduledExecutorService scheduler) {
        this.scheduleDelayMs = persistDelayMs;
        this.scheduled = new LinkedList<>();
        this.scheduler = scheduler;
        this.sizeThresholdBytes = sizeThresholdBytes;
        this.delegate = delegate;
    }

    @Override
    public void save(StoreTask storeTask) {
        Collection<StateChangeSet> changeSets = storeTask.changeSets;
        if (error.get() != null) {
            LOG.debug("don't persist {} changesets, already failed", changeSets.size());
            storeTask.fail(error.get());
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
        if (scheduledFuture.isDone() || isOverSizeThresholdAndCancellationSucceded()) {
            scheduleUpload();
        }
    }

    private boolean isOverSizeThresholdAndCancellationSucceded() {
        return scheduledSizeInBytes >= sizeThresholdBytes && scheduledFuture.cancel(false);
    }

    private void scheduleUpload() {
        checkState(scheduledFuture.isDone());
        long delay = scheduleDelayMs;
        if (scheduleDelayMs == 0 || scheduledSizeInBytes >= sizeThresholdBytes) {
            scheduledFuture = scheduler.submit(this::drainAndSave);
        } else {
            scheduledFuture = scheduler.schedule(this::drainAndSave, delay, MILLISECONDS);
        }
    }

    private void drainAndSave() {
        Collection<StoreTask> tasks;
        synchronized (scheduled) {
            tasks = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledSizeInBytes = 0;
        }
        try {
            if (error.get() != null) {
                tasks.forEach(task -> task.fail(error.get()));
                return;
            }
            delegate.save(tasks);

            synchronized (scheduled) {
                scheduleUploadIfNeeded();
            }
        } catch (Throwable t) {
            tasks.forEach(task -> task.fail(t));
            if (findThrowable(t, IOException.class).isPresent()) {
                LOG.warn("Caught IO exception while uploading", t);
            } else {
                error.compareAndSet(null, t);
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
        delegate.close();
    }
}
