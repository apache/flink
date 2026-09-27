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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.AbstractThreadsafeJobResultStore;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A thread-safe in-memory implementation of the {@link JobResultStore}. */
public class EmbeddedJobResultStore extends AbstractThreadsafeJobResultStore {

    private final Map<JobID, JobResultEntry> dirtyJobResults = new HashMap<>();

    private final Cache<JobID, JobResultEntry> cleanJobResults;

    /**
     * Creates a store that retains the clean job results indefinitely. This corresponds to the
     * behaviour this class always had.
     */
    public EmbeddedJobResultStore() {
        // a null TTL means "no expiration" which keeps the previous behaviour intact
        this((Duration) null);
    }

    /**
     * Creates a store that evicts the clean job results after {@link
     * JobResultStoreOptions#CLEAN_JOB_RESULT_TTL} has passed. If the option is not configured, the
     * clean job results are retained indefinitely.
     */
    public EmbeddedJobResultStore(Configuration configuration) {
        this(
                checkNotNull(configuration, "configuration")
                        .get(JobResultStoreOptions.CLEAN_JOB_RESULT_TTL));
    }

    @VisibleForTesting
    EmbeddedJobResultStore(@Nullable Duration cleanJobResultTtl) {
        super(Executors.directExecutor());
        final CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (cleanJobResultTtl != null) {
            cacheBuilder.expireAfterAccess(cleanJobResultTtl.toMillis(), TimeUnit.MILLISECONDS);
        }
        this.cleanJobResults = cacheBuilder.build();
    }

    @Override
    public void createDirtyResultInternal(JobResultEntry jobResultEntry) {
        dirtyJobResults.put(jobResultEntry.getJobId(), jobResultEntry);
    }

    @Override
    public void markResultAsCleanInternal(JobID jobId) throws NoSuchElementException {
        final JobResultEntry jobResultEntry = dirtyJobResults.remove(jobId);
        if (jobResultEntry != null) {
            cleanJobResults.put(jobId, jobResultEntry);
        } else {
            throw new NoSuchElementException(
                    String.format(
                            "Could not mark job %s as clean as it is not present in the job result store.",
                            jobId));
        }
    }

    @Override
    public boolean hasDirtyJobResultEntryInternal(JobID jobId) {
        return dirtyJobResults.containsKey(jobId);
    }

    @Override
    public boolean hasCleanJobResultEntryInternal(JobID jobId) {
        return cleanJobResults.asMap().containsKey(jobId);
    }

    @Override
    public Set<JobResult> getDirtyResultsInternal() {
        return dirtyJobResults.values().stream()
                .map(JobResultEntry::getJobResult)
                .collect(Collectors.toSet());
    }
}
