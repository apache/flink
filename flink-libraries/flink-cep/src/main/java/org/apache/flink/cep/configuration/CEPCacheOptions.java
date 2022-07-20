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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;

/** CEP Cache Options. */
public class CEPCacheOptions {

    private CEPCacheOptions() {}

    private static final String COMMON_HINT =
            "And it could accelerate the CEP operate process "
                    + "speed and limit the capacity of cache in pure memory. Note: It's only effective to "
                    + "limit usage of memory when 'state.backend' was set as 'rocksdb', which would "
                    + "transport the elements exceeded the number of the cache into the rocksdb state "
                    + "storage instead of memory state storage.";

    public static final ConfigOption<Integer> CEP_SHARED_BUFFER_EVENT_CACHE_SLOTS =
            ConfigOptions.key("pipeline.cep.sharedbuffer.cache.event-slots")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The Config option to set the maximum element number the "
                                    + "eventsBufferCache of SharedBuffer could hold. "
                                    + COMMON_HINT);

    public static final ConfigOption<Integer> CEP_SHARED_BUFFER_ENTRY_CACHE_SLOTS =
            ConfigOptions.key("pipeline.cep.sharedbuffer.cache.entry-slots")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The Config option to set the maximum element number the entryCache"
                                    + " of SharedBuffer could hold. And it could accelerate the"
                                    + " CEP operate process speed with state."
                                    + COMMON_HINT);

    public static final ConfigOption<Duration> CEP_CACHE_STATISTICS_INTERVAL =
            ConfigOptions.key("pipeline.cep.sharedbuffer.cache.statistics-interval")
                    .durationType()
                    .defaultValue(TimeUtils.parseDuration("30 min"))
                    .withDescription(
                            "The interval to log the information of cache state statistics in "
                                    + "CEP operator.");
}
