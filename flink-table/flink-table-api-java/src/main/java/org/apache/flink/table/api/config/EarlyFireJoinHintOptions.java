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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableSet;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds hint option name definitions for EARLY_FIRE join hints based on {@link
 * org.apache.flink.configuration.ConfigOption}.
 */
@PublicEvolving
public class EarlyFireJoinHintOptions {

    public static final ConfigOption<Duration> DELAY =
            key("delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The delay between the time an unmatched outer row becomes eligible to"
                                    + " be emitted with null padding and the time it is actually"
                                    + " emitted. Must be at least 1 millisecond.");

    public static final ConfigOption<TimeMode> TIME_MODE =
            key("time-mode")
                    .enumType(TimeMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "The time domain that drives the early-fire delay, can be 'rowtime' or"
                                    + " 'proctime'. If not set, it defaults to the time domain of"
                                    + " the interval join.");

    private static final Set<ConfigOption<?>> requiredKeys = new HashSet<>();
    private static final Set<ConfigOption<?>> supportedKeys = new HashSet<>();

    static {
        requiredKeys.add(DELAY);

        supportedKeys.add(DELAY);
        supportedKeys.add(TIME_MODE);
    }

    public static ImmutableSet<ConfigOption> getRequiredOptions() {
        return ImmutableSet.copyOf(requiredKeys);
    }

    public static ImmutableSet<ConfigOption> getSupportedOptions() {
        return ImmutableSet.copyOf(supportedKeys);
    }

    /** The time domain that drives the early-fire delay. */
    @PublicEvolving
    public enum TimeMode {
        ROWTIME,
        PROCTIME
    }
}
