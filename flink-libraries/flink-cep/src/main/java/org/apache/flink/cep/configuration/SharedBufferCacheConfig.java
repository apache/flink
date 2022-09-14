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

package org.apache.flink.cep.configuration;

import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.time.Duration;

/** Configuration immutable class. */
public final class SharedBufferCacheConfig implements Serializable {
    private final int eventsBufferCacheSlots;
    private final int entryCacheSlots;
    private final Duration cacheStatisticsInterval;

    public int getEventsBufferCacheSlots() {
        return eventsBufferCacheSlots;
    }

    public int getEntryCacheSlots() {
        return entryCacheSlots;
    }

    public Duration getCacheStatisticsInterval() {
        return cacheStatisticsInterval;
    }

    public SharedBufferCacheConfig() {
        this.cacheStatisticsInterval = CEPCacheOptions.CEP_CACHE_STATISTICS_INTERVAL.defaultValue();
        this.entryCacheSlots = CEPCacheOptions.CEP_SHARED_BUFFER_ENTRY_CACHE_SLOTS.defaultValue();
        this.eventsBufferCacheSlots =
                CEPCacheOptions.CEP_SHARED_BUFFER_EVENT_CACHE_SLOTS.defaultValue();
    }

    public SharedBufferCacheConfig(
            final int eventsBufferCacheSlots,
            final int entryCacheSlots,
            final Duration cacheStatisticsInterval) {
        this.cacheStatisticsInterval = cacheStatisticsInterval;
        this.entryCacheSlots = entryCacheSlots;
        this.eventsBufferCacheSlots = eventsBufferCacheSlots;
    }

    public static SharedBufferCacheConfig of(ReadableConfig readableConfig) {
        int eventsBufferCacheSlots =
                readableConfig.get(CEPCacheOptions.CEP_SHARED_BUFFER_EVENT_CACHE_SLOTS);
        int entryCacheSlots =
                readableConfig.get(CEPCacheOptions.CEP_SHARED_BUFFER_ENTRY_CACHE_SLOTS);
        Duration cacheStatisticsInterval =
                readableConfig.get(CEPCacheOptions.CEP_CACHE_STATISTICS_INTERVAL);
        return new SharedBufferCacheConfig(
                eventsBufferCacheSlots, entryCacheSlots, cacheStatisticsInterval);
    }
}
