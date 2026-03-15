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

import org.apache.flink.api.common.ApplicationID;
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

/** An abstract class for threadsafe implementations of the {@link ApplicationResultStore}. */
public abstract class AbstractThreadsafeApplicationResultStore implements ApplicationResultStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractThreadsafeApplicationResultStore.class);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Executor ioExecutor;

    protected AbstractThreadsafeApplicationResultStore(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
    }

    @Override
    public CompletableFuture<Void> createDirtyResultAsync(
            ApplicationResultEntry applicationResultEntry) {
        return hasApplicationResultEntryAsync(applicationResultEntry.getApplicationId())
                .thenAccept(
                        hasApplicationResultEntry ->
                                Preconditions.checkState(
                                        !hasApplicationResultEntry,
                                        "Application result store already contains an entry for application %s",
                                        applicationResultEntry.getApplicationId()))
                .thenCompose(
                        ignoredVoid ->
                                withWriteLockAsync(
                                        () -> createDirtyResultInternal(applicationResultEntry)));
    }

    @GuardedBy("readWriteLock")
    protected abstract void createDirtyResultInternal(ApplicationResultEntry applicationResultEntry)
            throws IOException;

    @Override
    public CompletableFuture<Void> markResultAsCleanAsync(ApplicationID applicationId) {
        return hasCleanApplicationResultEntryAsync(applicationId)
                .thenCompose(
                        hasCleanApplicationResultEntry -> {
                            if (hasCleanApplicationResultEntry) {
                                LOG.debug(
                                        "The application {} is already marked as clean. No action required.",
                                        applicationId);
                                return FutureUtils.completedVoidFuture();
                            }

                            return withWriteLockAsync(
                                    () -> markResultAsCleanInternal(applicationId));
                        });
    }

    @GuardedBy("readWriteLock")
    protected abstract void markResultAsCleanInternal(ApplicationID applicationId)
            throws IOException, NoSuchElementException;

    @Override
    public CompletableFuture<Boolean> hasApplicationResultEntryAsync(ApplicationID applicationId) {
        return withReadLockAsync(
                () ->
                        hasDirtyApplicationResultEntryInternal(applicationId)
                                || hasCleanApplicationResultEntryInternal(applicationId));
    }

    @Override
    public CompletableFuture<Boolean> hasDirtyApplicationResultEntryAsync(
            ApplicationID applicationId) {
        return withReadLockAsync(() -> hasDirtyApplicationResultEntryInternal(applicationId));
    }

    @GuardedBy("readWriteLock")
    protected abstract boolean hasDirtyApplicationResultEntryInternal(ApplicationID applicationId)
            throws IOException;

    @Override
    public CompletableFuture<Boolean> hasCleanApplicationResultEntryAsync(
            ApplicationID applicationId) {
        return withReadLockAsync(() -> hasCleanApplicationResultEntryInternal(applicationId));
    }

    @GuardedBy("readWriteLock")
    protected abstract boolean hasCleanApplicationResultEntryInternal(ApplicationID applicationId)
            throws IOException;

    @Override
    public Set<ApplicationResult> getDirtyResults() throws IOException {
        return withReadLock(this::getDirtyResultsInternal);
    }

    @GuardedBy("readWriteLock")
    protected abstract Set<ApplicationResult> getDirtyResultsInternal() throws IOException;

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
