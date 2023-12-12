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

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Config options for {@link org.apache.flink.util.concurrent.RetryStrategy} in
 * CollectResultFetcher.
 */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(
                    name = "ExponentialDelayCollectStrategy",
                    keyPrefix = "collect-strategy.exponential-delay"),
            @ConfigGroup(
                    name = "FixedDelayCollectStrategy",
                    keyPrefix = "collect-strategy.fixed-delay"),
            @ConfigGroup(
                    name = "IncrementalDelayCollectStrategy",
                    keyPrefix = "collect-strategy.incremental-delay"),
        })
public class CollectOptions {

    private static final String COLLECT_STRATEGY_PARAM = "collect-strategy";
    public static final String FIXED_DELAY_LABEL = "fixed-delay";
    public static final String EXPONENTIAL_DELAY_LABEL = "exponential-delay";

    public static final String INCREMENTAL_DELAY_LABEL = "incremental-delay";

    private static String createParameterPrefix(String paramGroupKey) {
        return COLLECT_STRATEGY_PARAM + "." + paramGroupKey + ".";
    }

    private static String createFixedDelayParameterPrefix(String parameter) {
        return createParameterPrefix(FIXED_DELAY_LABEL) + parameter;
    }

    private static String createExponentialBackoffParameterPrefix(String parameter) {
        return createParameterPrefix(EXPONENTIAL_DELAY_LABEL) + parameter;
    }

    private static String createIncrementalDelayParameterPrefix(String parameter) {
        return createParameterPrefix(INCREMENTAL_DELAY_LABEL) + parameter;
    }

    public static String extractAlphaNumericCharacters(String paramName) {
        return paramName.replaceAll("[^a-zA-Z0-9]", "");
    }

    public static final ConfigOption<String> COLLECT_STRATEGY =
            ConfigOptions.key(COLLECT_STRATEGY_PARAM + ".type")
                    .stringType()
                    .defaultValue(FIXED_DELAY_LABEL)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines the retry strategy to use in CollectResultFetcher.")
                                    .linebreak()
                                    .text("Accepted values are:")
                                    .list(
                                            text(
                                                    "%s, %s: CollectResultFetcher will send collect "
                                                            + "request to cluster in a fixed interval up "
                                                            + "to the point where the job is considered "
                                                            + "successful or a set amount of retries is "
                                                            + "reached.",
                                                    code(FIXED_DELAY_LABEL),
                                                    code(
                                                            extractAlphaNumericCharacters(
                                                                    FIXED_DELAY_LABEL))),
                                            text(
                                                    "%s, %s: Exponential delay retry strategy "
                                                            + "triggers the collect request with an "
                                                            + "exponentially increasing delay up to "
                                                            + " a configured max delay or the point "
                                                            + "where the job is succeeded or a set "
                                                            + "amount of retries is reached.",
                                                    code(EXPONENTIAL_DELAY_LABEL),
                                                    code(
                                                            extractAlphaNumericCharacters(
                                                                    EXPONENTIAL_DELAY_LABEL))),
                                            text(
                                                    "%s, %s: Incremental delay retry strategy "
                                                            + "triggers the collect request with an "
                                                            + "incremental delay up to a configured "
                                                            + "max delay or the point where the job "
                                                            + "is succeeded or a set amount of retries "
                                                            + "is reached.",
                                                    code(INCREMENTAL_DELAY_LABEL),
                                                    code(
                                                            extractAlphaNumericCharacters(
                                                                    INCREMENTAL_DELAY_LABEL))))
                                    .text(
                                            "The default configuration relies on an fixed delayed "
                                                    + "retry strategy with the given default values.")
                                    .build());

    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> COLLECT_STRATEGY_FIXED_DELAY_ATTEMPTS =
            ConfigOptions.key(createFixedDelayParameterPrefix("attempts"))
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of times that CollectResultFetcher retries the "
                                                    + "collect request before giving up if %s has been set to %s. "
                                                    + "Typically, it should keep retry until the job is finished.",
                                            code(COLLECT_STRATEGY.key()), code(FIXED_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> COLLECT_STRATEGY_FIXED_DELAY_DELAY =
            ConfigOptions.key(createFixedDelayParameterPrefix("delay"))
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Amount of time that CollectResultFetcher waits before next retry. "
                                                    + "It can be specified using the following notation: \"1 min\", "
                                                    + "\"20 s\"",
                                            code(COLLECT_STRATEGY.key()),
                                            code(FIXED_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> COLLECT_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF =
            ConfigOptions.key(createExponentialBackoffParameterPrefix("initial-backoff"))
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Starting duration between CollectResultFetcher "
                                                    + "retries the collect request if %s has "
                                                    + "been set to %s. It can be specified using the "
                                                    + "following notation: \"1 min\", \"20 s\"",
                                            code(COLLECT_STRATEGY.key()),
                                            code(EXPONENTIAL_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> COLLECT_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF =
            ConfigOptions.key(createExponentialBackoffParameterPrefix("max-backoff"))
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The highest possible duration between CollectResultFetcher "
                                                    + "retries the collect request if %s has "
                                                    + "been set to %s. It can be specified using the "
                                                    + "following notation: \"1 min\", \"20 s\"",
                                            code(COLLECT_STRATEGY.key()),
                                            code(EXPONENTIAL_DELAY_LABEL))
                                    .build());

    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> COLLECT_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS =
            ConfigOptions.key(createExponentialBackoffParameterPrefix("attempts"))
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of times that CollectResultFetcher retries the "
                                                    + "collect request before giving up if %s has been set to %s. "
                                                    + "Typically, it should keep retry until the job is finished.",
                                            code(COLLECT_STRATEGY.key()),
                                            code(EXPONENTIAL_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> COLLECT_STRATEGY_INCREMENTAL_DELAY_INITIAL_DELAY =
            ConfigOptions.key(createIncrementalDelayParameterPrefix("initial-delay"))
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Starting duration between CollectResultFetcher "
                                                    + "retries the collect request if %s has "
                                                    + "been set to %s. It can be specified using the "
                                                    + "following notation: \"1 min\", \"20 s\"",
                                            code(COLLECT_STRATEGY.key()),
                                            code(INCREMENTAL_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> COLLECT_STRATEGY_INCREMENTAL_DELAY_INCREMENT =
            ConfigOptions.key(createIncrementalDelayParameterPrefix("increment"))
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The delay increment between CollectResultFetcher "
                                                    + "retries the collect request if %s has "
                                                    + "been set to %s. It can be specified using the "
                                                    + "following notation: \"1 min\", \"20 s\"",
                                            code(COLLECT_STRATEGY.key()),
                                            code(INCREMENTAL_DELAY_LABEL))
                                    .build());

    public static final ConfigOption<Duration> COLLECT_STRATEGY_INCREMENTAL_DELAY_MAX_DELAY =
            ConfigOptions.key(createIncrementalDelayParameterPrefix("max-delay"))
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The highest possible duration between fetch "
                                                    + "retries if %s has been set to %s. It can be "
                                                    + "specified using the following notation: "
                                                    + "\"1 min\", \"20 s\"",
                                            code(COLLECT_STRATEGY.key()),
                                            code(INCREMENTAL_DELAY_LABEL))
                                    .build());

    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> COLLECT_STRATEGY_INCREMENTAL_DELAY_MAX_ATTEMPTS =
            ConfigOptions.key(createIncrementalDelayParameterPrefix("attempts"))
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of times that CollectResultFetcher retries the "
                                                    + "collect request before giving up if %s has been set to %s. "
                                                    + "Typically, it should keep retry until the job is finished.",
                                            code(COLLECT_STRATEGY.key()),
                                            code(INCREMENTAL_DELAY_LABEL))
                                    .build());
}
