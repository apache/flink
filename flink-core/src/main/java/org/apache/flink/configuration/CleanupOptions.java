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
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * {@link ConfigOption} collection for the configuration of repeatable cleanup of resource cleanup
 * after a job reached a globally-terminated state.
 *
 * <p>This implementation copies {@link RestartStrategyOptions} to provide similar user experience.
 * FLINK-26359 is created to clean this up.
 */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(
                    name = "ExponentialDelayCleanupStrategy",
                    keyPrefix = "cleanup-strategy.exponential-delay"),
            @ConfigGroup(
                    name = "FixedDelayCleanupStrategy",
                    keyPrefix = "cleanup-strategy.fixed-delay"),
        })
public class CleanupOptions {

    private static final String CLEANUP_STRATEGY_PARAM = "cleanup-strategy";

    public static final String FIXED_DELAY_LABEL = "fixed-delay";
    public static final String EXPONENTIAL_DELAY_LABEL = "exponential-delay";

    public static final Set<String> NONE_PARAM_VALUES = ImmutableSet.of("none", "disable", "off");

    private static String createParameterPrefix(String paramGroupKey) {
        return CLEANUP_STRATEGY_PARAM + "." + paramGroupKey + ".";
    }

    private static String createFixedDelayParameterPrefix(String parameter) {
        return createParameterPrefix(FIXED_DELAY_LABEL) + parameter;
    }

    private static String createExponentialBackoffParameterPrefix(String parameter) {
        return createParameterPrefix(EXPONENTIAL_DELAY_LABEL) + parameter;
    }

    public static String extractAlphaNumericCharacters(String paramName) {
        return paramName.replaceAll("[^a-zA-Z0-9]", "");
    }

    public static final ConfigOption<String> CLEANUP_STRATEGY =
            ConfigOptions.key(CLEANUP_STRATEGY_PARAM + ".type")
                    .stringType()
                    .defaultValue(EXPONENTIAL_DELAY_LABEL)
                    .withDeprecatedKeys(CLEANUP_STRATEGY_PARAM)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines the cleanup strategy to use in case of cleanup failures.")
                                    .linebreak()
                                    .text("Accepted values are:")
                                    .list(
                                            text(
                                                    NONE_PARAM_VALUES.stream()
                                                                    .map(ignored -> "%s")
                                                                    .collect(
                                                                            Collectors.joining(
                                                                                    ", "))
                                                            + ": Cleanup is only performed once. No retry "
                                                            + "will be initiated in case of failure. The job "
                                                            + "artifacts (and the job's JobResultStore entry) have "
                                                            + "to be cleaned up manually in case of a failure.",
                                                    NONE_PARAM_VALUES.stream()
                                                            .map(TextElement::code)
                                                            .collect(Collectors.toList())
                                                            .toArray(
                                                                    new TextElement
                                                                            [NONE_PARAM_VALUES
                                                                                    .size()])),
                                            text(
                                                    "%s, %s: Cleanup attempts will be separated by a fixed "
                                                            + "interval up to the point where the cleanup is "
                                                            + "considered successful or a set amount of retries "
                                                            + "is reached. Reaching the configured limit means that "
                                                            + "the job artifacts (and the job's JobResultStore entry) "
                                                            + "might need to be cleaned up manually.",
                                                    code(FIXED_DELAY_LABEL),
                                                    code(
                                                            extractAlphaNumericCharacters(
                                                                    FIXED_DELAY_LABEL))),
                                            text(
                                                    "%s, %s: Exponential delay restart strategy "
                                                            + "triggers the cleanup with an exponentially "
                                                            + "increasing delay up to the point where the "
                                                            + "cleanup succeeded or a set amount of retries "
                                                            + "is reached. Reaching the configured limit means that "
                                                            + "the job artifacts (and the job's JobResultStore entry) "
                                                            + "might need to be cleaned up manually.",
                                                    code(EXPONENTIAL_DELAY_LABEL),
                                                    code(
                                                            extractAlphaNumericCharacters(
                                                                    EXPONENTIAL_DELAY_LABEL))))
                                    .text(
                                            "The default configuration relies on an exponentially delayed "
                                                    + "retry strategy with the given default values.")
                                    .build());

    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> CLEANUP_STRATEGY_FIXED_DELAY_ATTEMPTS =
            ConfigOptions.key(createFixedDelayParameterPrefix("attempts"))
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of times that Flink retries the cleanup "
                                                    + "before giving up if %s has been set to %s. "
                                                    + "Reaching the configured limit means that "
                                                    + "the job artifacts (and the job's JobResultStore entry) "
                                                    + "might need to be cleaned up manually.",
                                            code(CLEANUP_STRATEGY.key()), code(FIXED_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> CLEANUP_STRATEGY_FIXED_DELAY_DELAY =
            ConfigOptions.key(createFixedDelayParameterPrefix("delay"))
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Amount of time that Flink waits before re-triggering "
                                                    + "the cleanup after a failed attempt if the %s is "
                                                    + "set to %s. It can be specified using the following "
                                                    + "notation: \"1 min\", \"20 s\"",
                                            code(CLEANUP_STRATEGY.key()), code(FIXED_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF =
            ConfigOptions.key(createExponentialBackoffParameterPrefix("initial-backoff"))
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Starting duration between cleanup retries if %s has "
                                                    + "been set to %s. It can be specified using the "
                                                    + "following notation: \"1 min\", \"20 s\"",
                                            code(CLEANUP_STRATEGY.key()),
                                            code(EXPONENTIAL_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF =
            ConfigOptions.key(createExponentialBackoffParameterPrefix("max-backoff"))
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The highest possible duration between cleanup "
                                                    + "retries if %s has been set to %s. It can be "
                                                    + "specified using the following notation: "
                                                    + "\"1 min\", \"20 s\"",
                                            code(CLEANUP_STRATEGY.key()),
                                            code(EXPONENTIAL_DELAY_LABEL))
                                    .build());

    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS =
            ConfigOptions.key(createExponentialBackoffParameterPrefix("attempts"))
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of times a failed cleanup is retried "
                                                    + "if %s has been set to %s. Reaching the "
                                                    + "configured limit means that the job artifacts "
                                                    + "(and the job's JobResultStore entry) "
                                                    + "might need to be cleaned up manually.",
                                            code(CLEANUP_STRATEGY.key()),
                                            code(EXPONENTIAL_DELAY_LABEL))
                                    .build());
}
