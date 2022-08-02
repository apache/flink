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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.code;

/** Predefined options for lookup table. */
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

    /** Types of the lookup cache. */
    public enum LookupCacheType {
        NONE,
        PARTIAL,
        FULL
    }
}
