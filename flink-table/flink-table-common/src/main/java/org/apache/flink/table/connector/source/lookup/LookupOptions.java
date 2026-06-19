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

package org.apache.flink.table.connector.source.lookup;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger.ScheduleMode.FIXED_DELAY;
import static org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger.ScheduleMode.FIXED_RATE;

/** Predefined options for lookup table. */
@PublicEvolving
public class LookupOptions {
    public static final ConfigOption<LookupCacheType> CACHE_TYPE =
            ConfigOptions.key("lookup.cache")
                    .enumType(LookupCacheType.class)
                    .defaultValue(LookupCacheType.NONE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The caching strategy for this lookup table, including %s, %s and %s",
                                            code(LookupCacheType.NONE.toString()),
                                            code(LookupCacheType.PARTIAL.toString()),
                                            code(LookupCacheType.FULL.toString()))
                                    .build());

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The maximum allowed retries if a lookup operation fails");

    public static final ConfigOption<Duration> PARTIAL_CACHE_EXPIRE_AFTER_ACCESS =
            ConfigOptions.key("lookup.partial-cache.expire-after-access")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Duration to expire an entry in the cache after accessing");

    public static final ConfigOption<Duration> PARTIAL_CACHE_EXPIRE_AFTER_WRITE =
            ConfigOptions.key("lookup.partial-cache.expire-after-write")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Duration to expire an entry in the cache after writing");

    public static final ConfigOption<Boolean> PARTIAL_CACHE_CACHE_MISSING_KEY =
            ConfigOptions.key("lookup.partial-cache.cache-missing-key")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to store an empty value into the cache if the lookup key doesn't match any rows in the table");

    public static final ConfigOption<Long> PARTIAL_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.partial-cache.max-rows")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The maximum number of rows to store in the cache");

    public static final ConfigOption<ReloadStrategy> FULL_CACHE_RELOAD_STRATEGY =
            ConfigOptions.key("lookup.full-cache.reload-strategy")
                    .enumType(ReloadStrategy.class)
                    .defaultValue(ReloadStrategy.PERIODIC)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines which strategy to use to reload full cache: "
                                                    + "%s - cache is reloaded with fixed intervals without initial delay; "
                                                    + "%s - cache is reloaded at specified time with fixed intervals multiples of one day",
                                            code(ReloadStrategy.PERIODIC.toString()),
                                            code(ReloadStrategy.TIMED.toString()))
                                    .build());

    public static final ConfigOption<Duration> FULL_CACHE_PERIODIC_RELOAD_INTERVAL =
            ConfigOptions.key("lookup.full-cache.periodic-reload.interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Reload interval of Full cache with PERIODIC reload strategy");

    public static final ConfigOption<PeriodicCacheReloadTrigger.ScheduleMode>
            FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE =
                    ConfigOptions.key("lookup.full-cache.periodic-reload.schedule-mode")
                            .enumType(PeriodicCacheReloadTrigger.ScheduleMode.class)
                            .defaultValue(FIXED_DELAY)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "Defines which schedule mode to use in PERIODIC reload strategy of Full cache:"
                                                            + "%s - reload interval is a time between the end of previous and start of the next reload; "
                                                            + "%s - reload interval is a time between the start of previous and start of the next reload",
                                                    code(FIXED_DELAY.toString()),
                                                    code(FIXED_RATE.toString()))
                                            .build());

    public static final ConfigOption<String> FULL_CACHE_TIMED_RELOAD_ISO_TIME =
            ConfigOptions.key("lookup.full-cache.timed-reload.iso-time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Time in ISO-8601 format when Full cache with TIMED strategy needs to be reloaded. "
                                    + "It can be useful when dimension is updated once a few days at specified time. "
                                    + "Time can be specified either with timezone or without timezone "
                                    + "(target JVM local timezone will be used). "
                                    + "For example, '10:15' = local TZ, '10:15Z' = UTC TZ, '10:15+07:00' = UTC +7 TZ");

    public static final ConfigOption<Integer> FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS =
            ConfigOptions.key("lookup.full-cache.timed-reload.interval-in-days")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Number of days between reloads of Full cache with TIMED strategy");

    /** Types of the lookup cache. */
    @PublicEvolving
    public enum LookupCacheType {
        NONE,
        PARTIAL,
        FULL
    }

    /** Defines which {@link CacheReloadTrigger} to use. */
    @PublicEvolving
    public enum ReloadStrategy {
        PERIODIC,
        TIMED
    }
}
