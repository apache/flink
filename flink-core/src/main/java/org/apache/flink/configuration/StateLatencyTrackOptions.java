/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * A collection of all configuration options that relate to the latency tracking for state access.
 */
@PublicEvolving
public class StateLatencyTrackOptions {

    @Documentation.Section(Documentation.Sections.STATE_LATENCY_TRACKING)
    public static final ConfigOption<Boolean> LATENCY_TRACK_ENABLED =
            ConfigOptions.key("state.latency-track.keyed-state-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys(StateBackendOptions.LATENCY_TRACK_ENABLED.key())
                    .withDescription(
                            "Whether to track latency of keyed state operations, e.g value state put/get/clear.");

    @Documentation.Section(Documentation.Sections.STATE_LATENCY_TRACKING)
    public static final ConfigOption<Integer> LATENCY_TRACK_SAMPLE_INTERVAL =
            ConfigOptions.key("state.latency-track.sample-interval")
                    .intType()
                    .defaultValue(100)
                    .withDeprecatedKeys(StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL.key())
                    .withDescription(
                            String.format(
                                    "The sample interval of latency track once '%s' is enabled. "
                                            + "The default value is 100, which means we would track the latency every 100 access requests.",
                                    LATENCY_TRACK_ENABLED.key()));

    @Documentation.Section(Documentation.Sections.STATE_LATENCY_TRACKING)
    public static final ConfigOption<Integer> LATENCY_TRACK_HISTORY_SIZE =
            ConfigOptions.key("state.latency-track.history-size")
                    .intType()
                    .defaultValue(128)
                    .withDeprecatedKeys(StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE.key())
                    .withDescription(
                            "Defines the number of measured latencies to maintain at each state access operation.");

    @Documentation.Section(Documentation.Sections.STATE_LATENCY_TRACKING)
    public static final ConfigOption<Boolean> LATENCY_TRACK_STATE_NAME_AS_VARIABLE =
            ConfigOptions.key("state.latency-track.state-name-as-variable")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys(
                            StateBackendOptions.LATENCY_TRACK_STATE_NAME_AS_VARIABLE.key())
                    .withDescription(
                            "Whether to expose state name as a variable if tracking latency.");
}
