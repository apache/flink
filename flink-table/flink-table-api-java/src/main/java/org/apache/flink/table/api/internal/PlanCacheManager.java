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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.Optional;

import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_PLAN_CACHE_ENABLED;
import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_PLAN_CACHE_SIZE;
import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_PLAN_CACHE_TTL;

/** This manages all the plan caches. */
public class PlanCacheManager {

    private final Cache<Tuple2<String, TableConfig>, CachedPlan> planCache;

    public PlanCacheManager(long maximumCapacity, Duration ttl) {
        planCache =
                CacheBuilder.newBuilder()
                        .maximumSize(maximumCapacity)
                        .expireAfterWrite(ttl)
                        .build();
    }

    public static Optional<PlanCacheManager> createPlanCacheManager(ReadableConfig readableConfig) {
        boolean planCacheEnabled = readableConfig.get(TABLE_OPTIMIZER_PLAN_CACHE_ENABLED);
        if (planCacheEnabled) {
            int planCacheSize = readableConfig.get(TABLE_OPTIMIZER_PLAN_CACHE_SIZE);
            Duration ttl = readableConfig.get(TABLE_OPTIMIZER_PLAN_CACHE_TTL);
            return Optional.of(new PlanCacheManager(planCacheSize, ttl));
        }
        return Optional.empty();
    }

    public Optional<CachedPlan> getPlan(String query, TableConfig tableConfig) {
        CachedPlan cachedPlan = planCache.getIfPresent(new Tuple2<>(query, tableConfig));
        return Optional.ofNullable(cachedPlan);
    }

    public void putPlan(String query, TableConfig tableConfig, CachedPlan cachedPlan) {
        Preconditions.checkNotNull(query, "query can not be null");
        Preconditions.checkNotNull(tableConfig, "tableConfig can not be null");
        Preconditions.checkNotNull(cachedPlan, "cachedPlan can not be null");
        planCache.put(new Tuple2<>(query, tableConfig), cachedPlan);
    }
}
