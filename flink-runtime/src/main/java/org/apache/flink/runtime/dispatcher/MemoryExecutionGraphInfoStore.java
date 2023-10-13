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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava31.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@link ExecutionGraphInfoStore} implementation which stores the {@link ArchivedExecutionGraph} in
 * memory. The memory store support to keep maximum job graphs and remove the timeout ones.
 */
public class MemoryExecutionGraphInfoStore implements ExecutionGraphInfoStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryExecutionGraphInfoStore.class);

    private final Cache<JobID, ExecutionGraphInfo> serializableExecutionGraphInfos;

    @Nullable private final ScheduledFuture<?> cleanupFuture;

    private final Thread shutdownHook;

    public MemoryExecutionGraphInfoStore() {
        this(Time.milliseconds(0), 0, null, null);
    }

    public MemoryExecutionGraphInfoStore(
            Time expirationTime,
            int maximumCapacity,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable Ticker ticker) {
        final long expirationMills = expirationTime.toMilliseconds();
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

        this.serializableExecutionGraphInfos = cacheBuilder.build();
        if (scheduledExecutor != null) {
            this.cleanupFuture =
                    scheduledExecutor.scheduleWithFixedDelay(
                            serializableExecutionGraphInfos::cleanUp,
                            expirationTime.toMilliseconds(),
                            expirationTime.toMilliseconds(),
                            TimeUnit.MILLISECONDS);
        } else {
            this.cleanupFuture = null;
        }
        this.shutdownHook = ShutdownHookUtil.addShutdownHook(this, getClass().getSimpleName(), LOG);
    }

    @Override
    public int size() {
        return Math.toIntExact(serializableExecutionGraphInfos.size());
    }

    @Nullable
    @Override
    public ExecutionGraphInfo get(JobID jobId) {
        return serializableExecutionGraphInfos.getIfPresent(jobId);
    }

    @Override
    public void put(ExecutionGraphInfo serializableExecutionGraphInfo) throws IOException {
        serializableExecutionGraphInfos.put(
                serializableExecutionGraphInfo.getJobId(), serializableExecutionGraphInfo);
    }

    @Override
    public JobsOverview getStoredJobsOverview() {
        Collection<JobStatus> allJobStatus =
                serializableExecutionGraphInfos.asMap().values().stream()
                        .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                        .map(ArchivedExecutionGraph::getState)
                        .collect(Collectors.toList());

        return JobsOverview.create(allJobStatus);
    }

    @Override
    public Collection<JobDetails> getAvailableJobDetails() {
        return serializableExecutionGraphInfos.asMap().values().stream()
                .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                .map(JobDetails::createDetailsForJob)
                .collect(Collectors.toList());
    }

    @Nullable
    @Override
    public JobDetails getAvailableJobDetails(JobID jobId) {
        final ExecutionGraphInfo archivedExecutionGraphInfo =
                serializableExecutionGraphInfos.getIfPresent(jobId);

        if (archivedExecutionGraphInfo != null) {
            return JobDetails.createDetailsForJob(
                    archivedExecutionGraphInfo.getArchivedExecutionGraph());
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (cleanupFuture != null) {
            cleanupFuture.cancel(false);
        }

        serializableExecutionGraphInfos.invalidateAll();

        // Remove shutdown hook to prevent resource leaks
        ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
    }
}
