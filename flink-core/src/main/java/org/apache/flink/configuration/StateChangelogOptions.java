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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

/** A collection of all configuration options that relate to changelog. */
@PublicEvolving
public class StateChangelogOptions {

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_CHANGELOG)
    public static final ConfigOption<Duration> PERIODIC_MATERIALIZATION_INTERVAL =
            ConfigOptions.key("state.backend.changelog.periodic-materialize.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "Defines the interval in milliseconds to perform "
                                    + "periodic materialization for state backend. "
                                    + "The periodic materialization will be disabled when the value is negative");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_CHANGELOG)
    public static final ConfigOption<Integer> MATERIALIZATION_MAX_FAILURES_ALLOWED =
            ConfigOptions.key("state.backend.changelog.max-failures-allowed")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Max number of consecutive materialization failures allowed.");

    /** Whether to enable state change log. */
    @Documentation.Section(value = Documentation.Sections.STATE_BACKEND_CHANGELOG)
    public static final ConfigOption<Boolean> ENABLE_STATE_CHANGE_LOG =
            ConfigOptions.key("state.backend.changelog.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable state backend to write state changes to StateChangelog. "
                                    + "If this config is not set explicitly, it means no preference "
                                    + "for enabling the change log, and the value in lower config "
                                    + "level will take effect. The default value 'false' here means "
                                    + "if no value set (job or cluster), the change log will not be "
                                    + "enabled.");

    /**
     * Which storage to use to store state changelog.
     *
     * <p>Recognized shortcut name is 'memory' from {@code
     * InMemoryStateChangelogStorageFactory.getIdentifier()}, which is also the default value.
     */
    @Documentation.Section(value = Documentation.Sections.STATE_BACKEND_CHANGELOG)
    public static final ConfigOption<String> STATE_CHANGE_LOG_STORAGE =
            ConfigOptions.key("state.backend.changelog.storage")
                    .stringType()
                    .defaultValue("memory")
                    .withDescription(
                            Description.builder()
                                    .text("The storage to be used to store state changelog.")
                                    .linebreak()
                                    .text(
                                            "The implementation can be specified via their"
                                                    + " shortcut name.")
                                    .linebreak()
                                    .text(
                                            "The list of recognized shortcut names currently includes"
                                                    + " 'memory' and 'filesystem'.")
                                    .build());
}
