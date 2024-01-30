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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheStats;

import java.time.Duration;
import java.util.Optional;

/** This manages all the plan caches. */
@Internal
public class PlanCacheManager {

    private final Cache<String, CachedPlan> planCache;

    public PlanCacheManager(long maximumCapacity, Duration ttl) {
        planCache =
                CacheBuilder.newBuilder()
                        .maximumSize(maximumCapacity)
                        .expireAfterWrite(ttl)
                        .recordStats()
                        .build();
    }

    public Optional<CachedPlan> getPlan(String query) {
        CachedPlan cachedPlan = planCache.getIfPresent(query);
        return Optional.ofNullable(cachedPlan);
    }

    public void putPlan(String query, CachedPlan cachedPlan) {
        Preconditions.checkNotNull(query, "query can not be null");
        Preconditions.checkNotNull(cachedPlan, "cachedPlan can not be null");
        planCache.put(query, cachedPlan);
    }

    public void invalidateAll() {
        planCache.invalidateAll();
    }

    @VisibleForTesting
    public CacheStats getCacheStats() {
        return planCache.stats();
    }
}
