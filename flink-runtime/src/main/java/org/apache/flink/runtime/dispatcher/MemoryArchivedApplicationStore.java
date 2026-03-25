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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava33.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@link ArchivedApplicationStore} implementation which stores the {@link ArchivedApplication} in
 * memory. The memory store support to keep maximum applications and remove the timeout ones.
 */
public class MemoryArchivedApplicationStore implements ArchivedApplicationStore {

    private final Cache<ApplicationID, ArchivedApplication> archivedApplicationCache;

    private final Map<JobID, ApplicationID> jobIdToApplicationId = new HashMap<>();

    @Nullable private final ScheduledFuture<?> cleanupFuture;

    public MemoryArchivedApplicationStore() {
        this(Duration.ofMillis(0), 0, null, null);
    }

    public MemoryArchivedApplicationStore(
            Duration expirationTime,
            int maximumCapacity,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable Ticker ticker) {
        final long expirationMills = expirationTime.toMillis();
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expirationMills > 0) {
            cacheBuilder.expireAfterWrite(expirationMills, TimeUnit.MILLISECONDS);
        }
        if (maximumCapacity > 0) {
            cacheBuilder.maximumSize(maximumCapacity);
        }
        if (ticker != null) {
            cacheBuilder.ticker(ticker);
        }

        this.archivedApplicationCache = cacheBuilder.build();
        if (scheduledExecutor != null) {
            this.cleanupFuture =
                    scheduledExecutor.scheduleWithFixedDelay(
                            archivedApplicationCache::cleanUp,
                            expirationTime.toMillis(),
                            expirationTime.toMillis(),
                            TimeUnit.MILLISECONDS);
        } else {
            this.cleanupFuture = null;
        }
    }

    @Override
    public int size() {
        return Math.toIntExact(archivedApplicationCache.size());
    }

    @Override
    public Optional<ArchivedApplication> get(ApplicationID applicationId) {
        return Optional.ofNullable(archivedApplicationCache.getIfPresent(applicationId));
    }

    @Override
    public void put(ArchivedApplication archivedApplication) throws IOException {
        final ApplicationID applicationId = archivedApplication.getApplicationId();
        archivedApplication
                .getJobs()
                .keySet()
                .forEach(jobId -> jobIdToApplicationId.put(jobId, applicationId));

        archivedApplicationCache.put(archivedApplication.getApplicationId(), archivedApplication);
    }

    @Override
    public Collection<ApplicationDetails> getApplicationDetails() {
        return archivedApplicationCache.asMap().values().stream()
                .map(ApplicationDetails::fromArchivedApplication)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ExecutionGraphInfo> getExecutionGraphInfo(JobID jobId) {
        final ApplicationID applicationId = jobIdToApplicationId.get(jobId);
        if (applicationId == null) {
            return Optional.empty();
        }
        final ArchivedApplication archivedApplication =
                archivedApplicationCache.getIfPresent(applicationId);
        return Optional.ofNullable(archivedApplication)
                .map(application -> application.getJobs().get(jobId));
    }

    @Override
    public Collection<JobDetails> getJobDetails() {
        return archivedApplicationCache.asMap().values().stream()
                .flatMap(archivedApplication -> archivedApplication.getJobs().values().stream())
                .map(JobDetails::createDetailsForJob)
                .collect(Collectors.toList());
    }

    @Override
    public JobsOverview getJobsOverview() {
        Collection<JobStatus> allJobStatus =
                archivedApplicationCache.asMap().values().stream()
                        .flatMap(
                                archivedApplication ->
                                        archivedApplication.getJobs().values().stream())
                        .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                        .map(ArchivedExecutionGraph::getState)
                        .collect(Collectors.toList());

        return JobsOverview.create(allJobStatus);
    }

    @Override
    public void close() throws IOException {
        if (cleanupFuture != null) {
            cleanupFuture.cancel(false);
        }

        archivedApplicationCache.invalidateAll();
    }
}
