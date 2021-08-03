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
import org.apache.flink.runtime.webmonitor.stats.Statistics;
import org.apache.flink.runtime.webmonitor.threadinfo.JobVertexThreadInfoTracker.Key;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Builder for {@link JobVertexThreadInfoTracker}.
 *
 * @param <T> Type of the derived statistics to return.
 */
public class JobVertexThreadInfoTrackerBuilder<T extends Statistics> {

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final Function<JobVertexThreadInfoStats, T> createStatsFn;
    private final ScheduledExecutorService executor;
    private final Time restTimeout;

    private ThreadInfoRequestCoordinator coordinator;
    private Duration cleanUpInterval;
    private int numSamples;
    private Duration statsRefreshInterval;
    private Duration delayBetweenSamples;
    private int maxThreadInfoDepth;
    private Cache<Key, T> vertexStatsCache;

    JobVertexThreadInfoTrackerBuilder(
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            Function<JobVertexThreadInfoStats, T> createStatsFn,
            ScheduledExecutorService executor,
            Time restTimeout) {
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
        this.createStatsFn = createStatsFn;
        this.executor = executor;
        this.restTimeout = restTimeout;
    }

    /**
     * Sets {@code cleanUpInterval}.
     *
     * @param coordinator Coordinator for thread info stats request.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder<T> setCoordinator(
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
    public JobVertexThreadInfoTrackerBuilder<T> setCleanUpInterval(Duration cleanUpInterval) {
        this.cleanUpInterval = cleanUpInterval;
        return this;
    }

    /**
     * Sets {@code numSamples}.
     *
     * @param numSamples Number of thread info samples to collect for each subtask.
     * @return Builder.
     */
    public JobVertexThreadInfoTrackerBuilder<T> setNumSamples(int numSamples) {
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
    public JobVertexThreadInfoTrackerBuilder<T> setStatsRefreshInterval(
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
    public JobVertexThreadInfoTrackerBuilder<T> setDelayBetweenSamples(
            Duration delayBetweenSamples) {
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
    public JobVertexThreadInfoTrackerBuilder<T> setMaxThreadInfoDepth(int maxThreadInfoDepth) {
        this.maxThreadInfoDepth = maxThreadInfoDepth;
        return this;
    }

    /**
     * Sets {@code vertexStatsCache}. This is currently only used for testing.
     *
     * @param vertexStatsCache The Cache instance to use for caching statistics. Will use the
     *     default defined in {@link JobVertexThreadInfoTrackerBuilder#defaultCache()} if not set.
     * @return Builder.
     */
    @VisibleForTesting
    JobVertexThreadInfoTrackerBuilder<T> setVertexStatsCache(Cache<Key, T> vertexStatsCache) {
        this.vertexStatsCache = vertexStatsCache;
        return this;
    }

    /**
     * Constructs a new {@link JobVertexThreadInfoTracker}.
     *
     * @return a new {@link JobVertexThreadInfoTracker} instance.
     */
    public JobVertexThreadInfoTracker<T> build() {
        if (vertexStatsCache == null) {
            vertexStatsCache = defaultCache();
        }
        return new JobVertexThreadInfoTracker<>(
                coordinator,
                resourceManagerGatewayRetriever,
                createStatsFn,
                executor,
                cleanUpInterval,
                numSamples,
                statsRefreshInterval,
                delayBetweenSamples,
                maxThreadInfoDepth,
                restTimeout,
                vertexStatsCache);
    }

    private Cache<Key, T> defaultCache() {
        checkArgument(cleanUpInterval.toMillis() > 0, "Clean up interval must be greater than 0");
        return CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .expireAfterAccess(cleanUpInterval.toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * Create a new {@link JobVertexThreadInfoTrackerBuilder}.
     *
     * @param <T> Type of the derived statistics to return.
     * @return Builder.
     */
    public static <T extends Statistics> JobVertexThreadInfoTrackerBuilder<T> newBuilder(
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            Function<JobVertexThreadInfoStats, T> createStatsFn,
            ScheduledExecutorService executor,
            Time restTimeout) {
        return new JobVertexThreadInfoTrackerBuilder<>(
                resourceManagerGatewayRetriever, createStatsFn, executor, restTimeout);
    }
}
