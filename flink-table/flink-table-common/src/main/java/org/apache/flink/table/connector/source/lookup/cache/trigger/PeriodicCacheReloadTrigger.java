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

package org.apache.flink.table.connector.source.lookup.cache.trigger;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_RELOAD_STRATEGY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType.FULL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy.PERIODIC;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A trigger that reloads cache entries periodically with specified interval and {@link
 * ScheduleMode}.
 */
@PublicEvolving
public class PeriodicCacheReloadTrigger implements CacheReloadTrigger {
    private static final long serialVersionUID = 1L;

    private final Duration reloadInterval;
    private final ScheduleMode scheduleMode;

    private transient ScheduledExecutorService scheduledExecutor;

    public PeriodicCacheReloadTrigger(Duration reloadInterval, ScheduleMode scheduleMode) {
        checkArgument(
                !reloadInterval.isNegative() && !reloadInterval.isZero(),
                "Reload interval must be greater than zero.");
        this.reloadInterval = reloadInterval;
        this.scheduleMode = scheduleMode;
    }

    @VisibleForTesting
    PeriodicCacheReloadTrigger(
            Duration reloadInterval,
            ScheduleMode scheduleMode,
            ScheduledExecutorService scheduledExecutor) {
        this(reloadInterval, scheduleMode);
        this.scheduledExecutor = scheduledExecutor;
    }

    @Override
    public void open(CacheReloadTrigger.Context context) {
        if (scheduledExecutor == null) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        }
        switch (scheduleMode) {
            case FIXED_RATE:
                scheduledExecutor.scheduleAtFixedRate(
                        context::triggerReload,
                        0,
                        reloadInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
                break;
            case FIXED_DELAY:
                scheduledExecutor.scheduleWithFixedDelay(
                        () -> {
                            try {
                                context.triggerReload().get();
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Uncaught exception during the reload", e);
                            }
                        },
                        0,
                        reloadInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unrecognized schedule mode \"%s\"", scheduleMode));
        }
    }

    @Override
    public void close() throws Exception {
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }

    public static PeriodicCacheReloadTrigger fromConfig(ReadableConfig config) {
        checkArgument(
                config.get(CACHE_TYPE) == FULL,
                "'%s' should be '%s' in order to build a Periodic cache reload trigger.",
                CACHE_TYPE.key(),
                FULL);
        checkArgument(
                config.get(FULL_CACHE_RELOAD_STRATEGY) == PERIODIC,
                "'%s' should be '%s' in order to build a Periodic cache reload trigger.",
                FULL_CACHE_RELOAD_STRATEGY.key(),
                PERIODIC);
        checkArgument(
                config.getOptional(FULL_CACHE_PERIODIC_RELOAD_INTERVAL).isPresent(),
                "Missing '%s' in the configuration. This option is required to build Periodic cache reload trigger.",
                FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key());
        return new PeriodicCacheReloadTrigger(
                config.get(FULL_CACHE_PERIODIC_RELOAD_INTERVAL),
                config.get(FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE));
    }

    /**
     * Defines the mode how to schedule cache reloads. See {@link
     * ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit) and {@link
     * ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}}.
     */
    @PublicEvolving
    public enum ScheduleMode {
        FIXED_DELAY, // reload interval is between the end of previous and start of the next reload
        FIXED_RATE // reload interval is between the start of previous and start of the next reload
    }
}
