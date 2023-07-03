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

package org.apache.flink.table.planner.hint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/** This {@link LookupJoinHintOptions} defines valid hint options of lookup join hint. */
@Internal
public class LookupJoinHintOptions {
    private LookupJoinHintOptions() {}

    public static final ConfigOption<String> LOOKUP_TABLE =
            key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table name of the lookup source.");

    public static final ConfigOption<Boolean> ASYNC_LOOKUP =
            key("async")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Value can be 'true' or 'false' to suggest the planner choose the corresponding"
                                    + " lookup function. If the backend lookup source does not support the"
                                    + " suggested lookup mode, it will take no effect.");

    public static final ConfigOption<ExecutionConfigOptions.AsyncOutputMode> ASYNC_OUTPUT_MODE =
            key("output-mode")
                    .enumType(ExecutionConfigOptions.AsyncOutputMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Output mode for asynchronous operations which will convert to {@see AsyncDataStream.OutputMode}, ORDERED by default. "
                                    + "If set to ALLOW_UNORDERED, will attempt to use {@see AsyncDataStream.OutputMode.UNORDERED} when it does not "
                                    + "affect the correctness of the result, otherwise ORDERED will be still used.");

    public static final ConfigOption<Integer> ASYNC_CAPACITY =
            key("capacity")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The max number of async i/o operation that the async lookup join can trigger.");

    public static final ConfigOption<Duration> ASYNC_TIMEOUT =
            key("timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Timeout from first invoke to final completion of asynchronous operation, may include multiple"
                                    + " retries, and will be reset in case of failover.");
    public static final ConfigOption<String> RETRY_PREDICATE =
            key("retry-predicate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A predicate expresses the retry condition, can be 'lookup-miss' which will"
                                    + " enable retry if lookup result is empty.");

    public static final ConfigOption<RetryStrategy> RETRY_STRATEGY =
            key("retry-strategy")
                    .enumType(RetryStrategy.class)
                    .noDefaultValue()
                    .withDescription("The retry strategy name, can be 'fixed-delay' for now.");

    public static final ConfigOption<Duration> FIXED_DELAY =
            key("fixed-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Delay time for the 'fixed-delay' retry strategy.");

    public static final ConfigOption<Integer> MAX_ATTEMPTS =
            key("max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Max attempt number of the 'fixed-delay' retry strategy.");

    public static final String LOOKUP_MISS_PREDICATE = "lookup_miss";

    private static Set<ConfigOption> requiredKeys = new HashSet<>();
    private static Set<ConfigOption> supportedKeys = new HashSet<>();

    static {
        requiredKeys.add(LOOKUP_TABLE);

        supportedKeys.add(LOOKUP_TABLE);
        supportedKeys.add(ASYNC_LOOKUP);
        supportedKeys.add(ASYNC_CAPACITY);
        supportedKeys.add(ASYNC_TIMEOUT);
        supportedKeys.add(ASYNC_OUTPUT_MODE);
        supportedKeys.add(RETRY_PREDICATE);
        supportedKeys.add(RETRY_STRATEGY);
        supportedKeys.add(FIXED_DELAY);
        supportedKeys.add(MAX_ATTEMPTS);
    }

    public static ImmutableSet<ConfigOption> getRequiredOptions() {
        return ImmutableSet.copyOf(requiredKeys);
    }

    public static ImmutableSet<ConfigOption> getSupportedOptions() {
        return ImmutableSet.copyOf(supportedKeys);
    }

    /** Supported retry strategies. */
    public enum RetryStrategy {
        /** Fixed-delay retry strategy. */
        FIXED_DELAY
    }
}
