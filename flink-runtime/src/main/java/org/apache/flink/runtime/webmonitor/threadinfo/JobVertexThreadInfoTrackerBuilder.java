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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.threadinfo.JobVertexThreadInfoTracker.JobVertexKey;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Builder for {@link JobVertexThreadInfoTracker}. */
public class JobVertexThreadInfoTrackerBuilder {

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final ScheduledExecutorService executor;
    private final Time restTimeout;

    private ThreadInfoRequestCoordinator coordinator;
    private Duration cleanUpInterval;
    private int numSamples;
    private Duration statsRefreshInterval;
    private Duration delayBetweenSamples;
    private int maxThreadInfoDepth;
    private Cache<JobVertexKey, VertexThreadInfoStats> jobVertexStatsCache;

    JobVertexThreadInfoTrackerBuilder(
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            ScheduledExecutorService executor,
            Time restTimeout) {
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
        this.executor = executor;
        this.restTimeout = restTimeout;
    }

    /**
     * Sets {@code cleanUpInterval}.
     *
     * @param coordinator Coordinator for thread info stats request.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder setCoordinator(
            ThreadInfoRequestCoordinator coordinator) {
        this.coordinator = coordinator;
        return this;
    }

    /**
     * Sets {@code cleanUpInterval}.
     *
     * @param cleanUpInterval Clean up interval for completed stats.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder setCleanUpInterval(Duration cleanUpInterval) {
        this.cleanUpInterval = cleanUpInterval;
        return this;
    }

    /**
     * Sets {@code numSamples}.
     *
     * @param numSamples Number of thread info samples to collect for each subtask.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    /**
     * Sets {@code statsRefreshInterval}.
     *
     * @param statsRefreshInterval Time interval after which the available thread info stats are
     *     deprecated and need to be refreshed.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder setStatsRefreshInterval(
            Duration statsRefreshInterval) {
        this.statsRefreshInterval = statsRefreshInterval;
        return this;
    }

    /**
     * Sets {@code delayBetweenSamples}.
     *
     * @param delayBetweenSamples Delay between individual samples per task.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder setDelayBetweenSamples(Duration delayBetweenSamples) {
        this.delayBetweenSamples = delayBetweenSamples;
        return this;
    }

    /**
     * Sets {@code delayBetweenSamples}.
     *
     * @param maxThreadInfoDepth Limit for the depth of the stack traces included when sampling
     *     threads.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder setMaxThreadInfoDepth(int maxThreadInfoDepth) {
        this.maxThreadInfoDepth = maxThreadInfoDepth;
        return this;
    }

    /**
     * Sets {@code jobVertexStatsCache}. This is currently only used for testing.
     *
     * @param jobVertexStatsCache The Cache instance to use for caching statistics. Will use the
     *     default defined in {@link JobVertexThreadInfoTrackerBuilder#defaultCache()} if not set.
     * @return Builder.
     */
    @VisibleForTesting
    JobVertexThreadInfoTrackerBuilder setJobVertexStatsCache(
            Cache<JobVertexThreadInfoTracker.JobVertexKey, VertexThreadInfoStats>
                    jobVertexStatsCache) {
        this.jobVertexStatsCache = jobVertexStatsCache;
        return this;
    }

    /**
     * Constructs a new {@link JobVertexThreadInfoTracker}.
     *
     * @return a new {@link JobVertexThreadInfoTracker} instance.
     */
    public JobVertexThreadInfoTracker build() {
        if (jobVertexStatsCache == null) {
            jobVertexStatsCache = defaultCache();
        }
        return new JobVertexThreadInfoTracker(
                coordinator,
                resourceManagerGatewayRetriever,
                executor,
                cleanUpInterval,
                numSamples,
                statsRefreshInterval,
                delayBetweenSamples,
                maxThreadInfoDepth,
                restTimeout,
                jobVertexStatsCache);
    }

    private Cache<JobVertexKey, VertexThreadInfoStats> defaultCache() {
        checkArgument(cleanUpInterval.toMillis() > 0, "Clean up interval must be greater than 0");
        return CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .expireAfterAccess(cleanUpInterval.toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * Create a new {@link JobVertexThreadInfoTrackerBuilder}.
     *
     * @return Builder.
     */
    public static JobVertexThreadInfoTrackerBuilder newBuilder(
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            ScheduledExecutorService executor,
            Time restTimeout) {
        return new JobVertexThreadInfoTrackerBuilder(
                resourceManagerGatewayRetriever, executor, restTimeout);
    }
}
