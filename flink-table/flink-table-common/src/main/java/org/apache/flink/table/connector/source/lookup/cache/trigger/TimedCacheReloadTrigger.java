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

import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_RELOAD_STRATEGY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType.FULL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy.TIMED;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A trigger that reloads at a specific time and repeats for the given interval in days. */
@PublicEvolving
public class TimedCacheReloadTrigger implements CacheReloadTrigger {
    private static final long serialVersionUID = 1L;

    private final Temporal reloadTime;
    private final int reloadIntervalInDays;

    private transient ScheduledExecutorService scheduledExecutor;
    private transient Clock clock; // clock for testing purposes

    public TimedCacheReloadTrigger(OffsetTime reloadTime, int reloadIntervalInDays) {
        this((Temporal) reloadTime, reloadIntervalInDays);
    }

    public TimedCacheReloadTrigger(LocalTime reloadTime, int reloadIntervalInDays) {
        this((Temporal) reloadTime, reloadIntervalInDays);
    }

    private TimedCacheReloadTrigger(Temporal reloadTime, int reloadIntervalInDays) {
        checkArgument(
                reloadIntervalInDays > 0,
                "Reload interval for Timed cache reload trigger must be at least 1 day.");
        this.reloadTime = reloadTime;
        this.reloadIntervalInDays = reloadIntervalInDays;
    }

    @VisibleForTesting
    TimedCacheReloadTrigger(
            Temporal reloadTime,
            int reloadIntervalInDays,
            ScheduledExecutorService scheduledExecutor,
            Clock clock) {
        this(reloadTime, reloadIntervalInDays);
        this.scheduledExecutor = scheduledExecutor;
        this.clock = clock;
    }

    @Override
    public void open(Context context) {
        if (scheduledExecutor == null) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        }
        if (clock == null) {
            clock =
                    reloadTime instanceof LocalTime
                            ? Clock.systemDefaultZone()
                            : Clock.system(((OffsetTime) reloadTime).getOffset());
        }
        Temporal now =
                reloadTime instanceof LocalTime ? LocalTime.now(clock) : OffsetTime.now(clock);

        Duration initialDelayDuration = Duration.between(now, reloadTime);
        if (initialDelayDuration.isNegative()) {
            // in case when reloadTime is less than current time, reload will happen next day
            initialDelayDuration = initialDelayDuration.plus(1, ChronoUnit.DAYS);
        }
        scheduledExecutor.execute(context::triggerReload); // trigger first load operation
        scheduledExecutor.scheduleAtFixedRate(
                context::triggerReload,
                initialDelayDuration.toMillis(),
                Duration.ofDays(reloadIntervalInDays).toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }

    @VisibleForTesting
    Temporal getReloadTime() {
        return reloadTime;
    }

    public static TimedCacheReloadTrigger fromConfig(ReadableConfig config) {
        checkArgument(
                config.get(CACHE_TYPE) == FULL,
                "'%s' should be '%s' in order to build a Timed cache reload trigger.",
                CACHE_TYPE.key(),
                FULL);
        checkArgument(
                config.get(FULL_CACHE_RELOAD_STRATEGY) == TIMED,
                "'%s' should be '%s' in order to build a Timed cache reload trigger.",
                FULL_CACHE_RELOAD_STRATEGY.key(),
                TIMED);
        checkArgument(
                config.getOptional(FULL_CACHE_TIMED_RELOAD_ISO_TIME).isPresent(),
                "Missing '%s' in the configuration. This option is required to build a Timed cache reload trigger.",
                FULL_CACHE_TIMED_RELOAD_ISO_TIME.key());
        Temporal reloadTime =
                (Temporal)
                        DateTimeFormatter.ISO_TIME.parseBest(
                                config.get(FULL_CACHE_TIMED_RELOAD_ISO_TIME),
                                OffsetTime::from,
                                LocalTime::from);
        int reloadIntervalInDays = config.get(FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS);
        return new TimedCacheReloadTrigger(reloadTime, reloadIntervalInDays);
    }
}
