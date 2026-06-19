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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** An abstract class for threadsafe implementations of the {@link JobResultStore}. */
public abstract class AbstractThreadsafeJobResultStore implements JobResultStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractThreadsafeJobResultStore.class);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Executor ioExecutor;

    protected AbstractThreadsafeJobResultStore(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
    }

    @Override
    public CompletableFuture<Void> createDirtyResultAsync(JobResultEntry jobResultEntry) {
        return hasJobResultEntryAsync(jobResultEntry.getJobId())
                .thenAccept(
                        hasJobResultEntry ->
                                Preconditions.checkState(
                                        !hasJobResultEntry,
                                        "Job result store already contains an entry for job %s",
                                        jobResultEntry.getJobId()))
                .thenCompose(
                        ignoredVoid ->
                                withWriteLockAsync(
                                        () -> createDirtyResultInternal(jobResultEntry)));
    }

    @GuardedBy("readWriteLock")
    protected abstract void createDirtyResultInternal(JobResultEntry jobResultEntry)
            throws IOException;

    @Override
    public CompletableFuture<Void> markResultAsCleanAsync(JobID jobId) {
        return hasCleanJobResultEntryAsync(jobId)
                .thenCompose(
                        hasCleanJobResultEntry -> {
                            if (hasCleanJobResultEntry) {
                                LOG.debug(
                                        "The job {} is already marked as clean. No action required.",
                                        jobId);
                                return FutureUtils.completedVoidFuture();
                            }

                            return withWriteLockAsync(() -> markResultAsCleanInternal(jobId));
                        });
    }

    @GuardedBy("readWriteLock")
    protected abstract void markResultAsCleanInternal(JobID jobId)
            throws IOException, NoSuchElementException;

    @Override
    public CompletableFuture<Boolean> hasJobResultEntryAsync(JobID jobId) {
        return withReadLockAsync(
                () ->
                        hasDirtyJobResultEntryInternal(jobId)
                                || hasCleanJobResultEntryInternal(jobId));
    }

    @Override
    public CompletableFuture<Boolean> hasDirtyJobResultEntryAsync(JobID jobId) {
        return withReadLockAsync(() -> hasDirtyJobResultEntryInternal(jobId));
    }

    @GuardedBy("readWriteLock")
    protected abstract boolean hasDirtyJobResultEntryInternal(JobID jobId) throws IOException;

    @Override
    public CompletableFuture<Boolean> hasCleanJobResultEntryAsync(JobID jobId) {
        return withReadLockAsync(() -> hasCleanJobResultEntryInternal(jobId));
    }

    @GuardedBy("readWriteLock")
    protected abstract boolean hasCleanJobResultEntryInternal(JobID jobId) throws IOException;

    @Override
    public Set<JobResult> getDirtyResults() throws IOException {
        return withReadLock(this::getDirtyResultsInternal);
    }

    @GuardedBy("readWriteLock")
    protected abstract Set<JobResult> getDirtyResultsInternal() throws IOException;

    private CompletableFuture<Void> withWriteLockAsync(ThrowingRunnable<IOException> runnable) {
        return FutureUtils.runAsync(
                () -> {
                    withWriteLock(runnable);
                },
                ioExecutor);
    }

    private void withWriteLock(ThrowingRunnable<IOException> runnable) throws IOException {
        readWriteLock.writeLock().lock();
        try {
            runnable.run();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private <T> CompletableFuture<T> withReadLockAsync(
            SupplierWithException<T, IOException> runnable) {
        return FutureUtils.supplyAsync(() -> withReadLock(runnable), ioExecutor);
    }

    private <T> T withReadLock(SupplierWithException<T, IOException> supplier) throws IOException {
        readWriteLock.readLock().lock();
        try {
            return supplier.get();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
}
