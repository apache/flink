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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.SerializedShuffleDescriptorAndIndicesID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Default implement of {@link ShuffleDescriptorsCache}. Entries will be expired after timeout. */
public class DefaultShuffleDescriptorsCache implements ShuffleDescriptorsCache {
    private final Clock clock;

    private final Map<SerializedShuffleDescriptorAndIndicesID, ShuffleDescriptorCacheEntry> caches;
    private final Map<JobID, Set<SerializedShuffleDescriptorAndIndicesID>>
            serializedShuffleDescriptorsPerJob;
    private final Duration idleTimeout;
    private final int cachedShuffleDescriptorLimit;

    private int totalCachedShuffleDescriptorNumber;
    private boolean started;
    private ComponentMainThreadExecutor mainThreadExecutor;

    public DefaultShuffleDescriptorsCache(Duration idleTimeout, int cachedShuffleDescriptorLimit) {
        this(idleTimeout, cachedShuffleDescriptorLimit, SystemClock.getInstance());
    }

    @VisibleForTesting
    public DefaultShuffleDescriptorsCache(
            Duration idleTimeout, int cachedShuffleDescriptorLimit, Clock clock) {
        this.clock = clock;
        this.idleTimeout = idleTimeout;
        this.cachedShuffleDescriptorLimit = cachedShuffleDescriptorLimit;
        this.caches = new LinkedHashMap<>(16, 0.75F, true);
        this.serializedShuffleDescriptorsPerJob = new HashMap<>();
    }

    @Override
    public void start(ComponentMainThreadExecutor mainThreadExecutor) {
        this.started = true;
        this.mainThreadExecutor = mainThreadExecutor;
        mainThreadExecutor.schedule(
                this::removeIdleEntry, this.idleTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        started = false;
        mainThreadExecutor = null;
        serializedShuffleDescriptorsPerJob.clear();
        caches.clear();
    }

    @Override
    public ShuffleDescriptorCacheEntry get(
            SerializedShuffleDescriptorAndIndicesID serializedShuffleDescriptorsId) {
        ShuffleDescriptorCacheEntry entry = caches.remove(serializedShuffleDescriptorsId);

        if (entry != null) {
            entry.updateIdleSince(clock.absoluteTimeMillis());
            caches.put(serializedShuffleDescriptorsId, entry);
        }
        return entry;
    }

    @Override
    public void put(
            JobID jobId,
            SerializedShuffleDescriptorAndIndicesID serializedShuffleDescriptorsId,
            TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]
                    shuffleDescriptorAndIndices) {
        removeIfOverLimit(shuffleDescriptorAndIndices.length);
        caches.put(
                serializedShuffleDescriptorsId,
                new ShuffleDescriptorCacheEntry(
                        shuffleDescriptorAndIndices, jobId, clock.absoluteTimeMillis()));
        serializedShuffleDescriptorsPerJob
                .computeIfAbsent(jobId, ignore -> new HashSet<>())
                .add(serializedShuffleDescriptorsId);
        totalCachedShuffleDescriptorNumber += shuffleDescriptorAndIndices.length;
    }

    public void clearCacheOfJob(JobID jobId) {
        Set<SerializedShuffleDescriptorAndIndicesID> removed =
                serializedShuffleDescriptorsPerJob.remove(jobId);
        if (removed != null) {
            removed.forEach(
                    id -> {
                        ShuffleDescriptorCacheEntry entry = caches.remove(id);
                        if (entry != null) {
                            totalCachedShuffleDescriptorNumber -=
                                    entry.getShuffleDescriptorAndIndices().length;
                        }
                    });
        }
    }

    private void removeIfOverLimit(int newSize) {
        Iterator<Map.Entry<SerializedShuffleDescriptorAndIndicesID, ShuffleDescriptorCacheEntry>>
                iterator = caches.entrySet().iterator();
        while (iterator.hasNext()
                && totalCachedShuffleDescriptorNumber + newSize > cachedShuffleDescriptorLimit) {
            Map.Entry<SerializedShuffleDescriptorAndIndicesID, ShuffleDescriptorCacheEntry> next =
                    iterator.next();
            iterator.remove();
            JobID jobId = next.getValue().getJobId();
            serializedShuffleDescriptorsPerJob.get(jobId).remove(next.getKey());
            if (serializedShuffleDescriptorsPerJob.get(jobId).isEmpty()) {
                serializedShuffleDescriptorsPerJob.remove(jobId);
            }
            totalCachedShuffleDescriptorNumber -=
                    next.getValue().getShuffleDescriptorAndIndices().length;
        }
    }

    private void removeIdleEntry() {
        if (!started || mainThreadExecutor == null) {
            return;
        }

        long currentTs = clock.absoluteTimeMillis();
        // delete elements and remove job
        Iterator<Map.Entry<SerializedShuffleDescriptorAndIndicesID, ShuffleDescriptorCacheEntry>>
                iterator = caches.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<SerializedShuffleDescriptorAndIndicesID, ShuffleDescriptorCacheEntry> next =
                    iterator.next();
            ShuffleDescriptorCacheEntry entry = next.getValue();
            if (currentTs - entry.getIdleSince() > idleTimeout.toMillis()) {
                iterator.remove();
                totalCachedShuffleDescriptorNumber -= entry.getShuffleDescriptorAndIndices().length;

                JobID jobId = entry.getJobId();
                serializedShuffleDescriptorsPerJob.get(jobId).remove(next.getKey());
                if (serializedShuffleDescriptorsPerJob.get(jobId).isEmpty()) {
                    serializedShuffleDescriptorsPerJob.remove(jobId);
                }
            } else {
                break;
            }
        }

        mainThreadExecutor.schedule(
                this::removeIdleEntry, idleTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
