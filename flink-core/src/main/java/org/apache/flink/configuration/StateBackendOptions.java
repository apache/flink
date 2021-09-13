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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

/** A collection of all configuration options that relate to state backend. */
public class StateBackendOptions {

    // ------------------------------------------------------------------------
    //  general state backend options
    // ------------------------------------------------------------------------

    /**
     * The checkpoint storage used to store operator state locally within the cluster during
     * execution.
     *
     * <p>The implementation can be specified either via their shortcut name, or via the class name
     * of a {@code StateBackendFactory}. If a StateBackendFactory class name is specified, the
     * factory is instantiated (via its zero-argument constructor) and its {@code
     * StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
     *
     * <p>Recognized shortcut names are 'hashmap' and 'rocksdb'.
     */
    @Documentation.Section(value = Documentation.Sections.COMMON_STATE_BACKENDS, position = 1)
    public static final ConfigOption<String> STATE_BACKEND =
            ConfigOptions.key("state.backend")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The state backend to be used to store state.")
                                    .linebreak()
                                    .text(
                                            "The implementation can be specified either via their shortcut "
                                                    + " name, or via the class name of a %s. "
                                                    + "If a factory is specified it is instantiated via its "
                                                    + "zero argument constructor and its %s "
                                                    + "method is called.",
                                            TextElement.code("StateBackendFactory"),
                                            TextElement.code(
                                                    "StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)"))
                                    .linebreak()
                                    .text("Recognized shortcut names are 'hashmap' and 'rocksdb'.")
                                    .build());

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_LATENCY_TRACKING)
    public static final ConfigOption<Boolean> LATENCY_TRACK_ENABLED =
            ConfigOptions.key("state.backend.latency-track.keyed-state-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to track latency of keyed state operations, e.g value state put/get/clear.");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_LATENCY_TRACKING)
    public static final ConfigOption<Integer> LATENCY_TRACK_SAMPLE_INTERVAL =
            ConfigOptions.key("state.backend.latency-track.sample-interval")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            String.format(
                                    "The sample interval of latency track once '%s' is enabled. "
                                            + "The default value is 100, which means we would track the latency every 100 access requests.",
                                    LATENCY_TRACK_ENABLED.key()));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_LATENCY_TRACKING)
    public static final ConfigOption<Integer> LATENCY_TRACK_HISTORY_SIZE =
            ConfigOptions.key("state.backend.latency-track.history-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "Defines the number of measured latencies to maintain at each state access operation.");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_LATENCY_TRACKING)
    public static final ConfigOption<Boolean> LATENCY_TRACK_STATE_NAME_AS_VARIABLE =
            ConfigOptions.key("state.backend.latency-track.state-name-as-variable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to expose state name as a variable if tracking latency.");
}
