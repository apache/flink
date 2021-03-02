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
 * A {@link StateChangeUploader} that waits for some configured amount of time before passing the
 * accumulated state changes to the actual store.
 */
@ThreadSafe
class BatchingStateChangeUploader implements StateChangeUploader {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingStateChangeUploader.class);

    private final RetryingExecutor retryingExecutor;
    private final RetryPolicy retryPolicy;
    private final StateChangeUploader delegate;
    private final ScheduledExecutorService scheduler;
    private final long scheduleDelayMs;
    private final long sizeThresholdBytes;

    @GuardedBy("scheduled")
    private final Queue<StateChangeSetUpload> scheduled;

    @GuardedBy("scheduled")
    private long scheduledSizeInBytes;

    @Nullable
    @GuardedBy("scheduled")
    private ScheduledFuture<?> scheduledFuture;

    private volatile Throwable error;

    BatchingStateChangeUploader(
            long persistDelayMs,
            long sizeThresholdBytes,
            RetryPolicy retryPolicy,
            StateChangeUploader delegate) {
        this(
                persistDelayMs,
                sizeThresholdBytes,
                retryPolicy,
                delegate,
                SchedulerFactory.create(1, "ChangelogRetryScheduler", LOG),
                new RetryingExecutor());
    }

    BatchingStateChangeUploader(
            long persistDelayMs,
            long sizeThresholdBytes,
            RetryPolicy retryPolicy,
            StateChangeUploader delegate,
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
    public void upload(Collection<StateChangeSetUpload> changeSets) {
        if (error != null) {
            LOG.debug("don't persist {} changesets, already failed", changeSets.size());
            changeSets.forEach(cs -> cs.fail(error));
            return;
        }
        LOG.debug("persist {} changeSets", changeSets.size());
        try {
            synchronized (scheduled) {
                for (StateChangeSetUpload upload : changeSets) {
                    scheduled.add(upload);
                    scheduledSizeInBytes += upload.getSize();
                }
                scheduleUploadIfNeeded();
            }
        } catch (Exception e) {
            changeSets.forEach(cs -> cs.fail(e));
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
        Collection<StateChangeSetUpload> changeSets;
        synchronized (scheduled) {
            changeSets = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledSizeInBytes = 0;
            scheduledFuture = null;
        }
        try {
            if (error != null) {
                changeSets.forEach(changeSet -> changeSet.fail(error));
                return;
            }
            retryingExecutor.execute(retryPolicy, () -> delegate.upload(changeSets));
        } catch (Throwable t) {
            changeSets.forEach(changeSet -> changeSet.fail(t));
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
        ArrayList<StateChangeSetUpload> drained;
        synchronized (scheduled) {
            drained = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledSizeInBytes = 0;
        }
        CancellationException ce = new CancellationException();
        drained.forEach(upload -> upload.fail(ce));
        retryingExecutor.close();
        delegate.close();
    }
}
