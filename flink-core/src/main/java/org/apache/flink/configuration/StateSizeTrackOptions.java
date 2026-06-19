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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;

/** A collection of all configuration options that relate to the size tracking for state access. */
@PublicEvolving
public class StateSizeTrackOptions {

    @Documentation.Section(Documentation.Sections.STATE_SIZE_TRACKING)
    public static final ConfigOption<Boolean> SIZE_TRACK_ENABLED =
            ConfigOptions.key("state.size-track.keyed-state-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to track size of keyed state operations, e.g value state put/get/clear. Please note that if state.ttl is enabled, the size of the value will include the size of the TTL-related timestamp.");

    @Documentation.Section(Documentation.Sections.STATE_SIZE_TRACKING)
    public static final ConfigOption<Integer> SIZE_TRACK_SAMPLE_INTERVAL =
            ConfigOptions.key("state.size-track.sample-interval")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            String.format(
                                    "The sample interval of size track once '%s' is enabled. "
                                            + "The default value is 100, which means we would track the size every 100 access requests.",
                                    SIZE_TRACK_ENABLED.key()));

    @Documentation.Section(Documentation.Sections.STATE_SIZE_TRACKING)
    public static final ConfigOption<Integer> SIZE_TRACK_HISTORY_SIZE =
            ConfigOptions.key("state.size-track.history-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "Defines the number of measured size to maintain at each state access operation.");

    @Documentation.Section(Documentation.Sections.STATE_SIZE_TRACKING)
    public static final ConfigOption<Boolean> SIZE_TRACK_STATE_NAME_AS_VARIABLE =
            ConfigOptions.key("state.size-track.state-name-as-variable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to expose state name as a variable if tracking size.");
}
