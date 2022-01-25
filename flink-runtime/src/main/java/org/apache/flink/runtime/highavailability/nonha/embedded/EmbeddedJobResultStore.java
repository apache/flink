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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** A thread-safe in-memory implementation of the {@link JobResultStore}. */
public class EmbeddedJobResultStore implements JobResultStore {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedJobResultStore.class);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    @GuardedBy("readWriteLock")
    @VisibleForTesting
    final Map<JobID, JobResultEntry> dirtyJobResults = new HashMap<>();

    @GuardedBy("readWriteLock")
    @VisibleForTesting
    final Map<JobID, JobResultEntry> cleanJobResults = new HashMap<>();

    @Override
    public void createDirtyResult(JobResultEntry jobResultEntry) {
        Preconditions.checkState(
                !hasJobResultEntry(jobResultEntry.getJobId()),
                "There is already a job registered under the passed ID {}.",
                jobResultEntry.getJobId());

        withWriteLock(() -> dirtyJobResults.put(jobResultEntry.getJobId(), jobResultEntry));
    }

    @Override
    public void markResultAsClean(JobID jobId) throws NoSuchElementException {
        if (hasCleanJobResultEntry(jobId)) {
            LOG.debug("The job {} is already marked as clean. No action required.", jobId);

            return;
        }

        withWriteLock(
                () -> {
                    final JobResultEntry jobResultEntry = dirtyJobResults.remove(jobId);
                    if (jobResultEntry != null) {
                        cleanJobResults.put(jobId, jobResultEntry);
                    } else {
                        throw new NoSuchElementException(
                                String.format(
                                        "Could not mark job %s as clean as it is not present in the job result store.",
                                        jobId));
                    }
                });
    }

    @Override
    public boolean hasJobResultEntry(JobID jobId) {
        return withReadLock(
                () -> dirtyJobResults.containsKey(jobId) || cleanJobResults.containsKey(jobId));
    }

    @Override
    public boolean hasDirtyJobResultEntry(JobID jobId) {
        return withReadLock(() -> dirtyJobResults.containsKey(jobId));
    }

    @Override
    public boolean hasCleanJobResultEntry(JobID jobId) {
        return withReadLock(() -> cleanJobResults.containsKey(jobId));
    }

    @Override
    public Set<JobResult> getDirtyResults() {
        return withReadLock(
                () ->
                        dirtyJobResults.values().stream()
                                .map(JobResultEntry::getJobResult)
                                .collect(Collectors.toSet()));
    }

    private void withWriteLock(Runnable callback) {
        readWriteLock.writeLock().lock();
        try {
            callback.run();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private <T> T withReadLock(Supplier<T> callback) {
        readWriteLock.readLock().lock();
        try {
            return callback.get();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
}
