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
 * This class holds option name definitions for ML_PREDICT runtime config based on {@link
 * ConfigOption}.
 */
@PublicEvolving
public class MLPredictRuntimeConfigOptions {

    public static final ConfigOption<Boolean> ASYNC =
            key("async")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Value can be 'true' or 'false' to suggest the planner choose the corresponding"
                                    + " predict function. If the backend predict function provider does not support the"
                                    + " suggested mode, it will throw exception to notify users.");

    public static final ConfigOption<ExecutionConfigOptions.AsyncOutputMode> ASYNC_OUTPUT_MODE =
            key("output-mode")
                    .enumType(ExecutionConfigOptions.AsyncOutputMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Output mode for asynchronous operations which will convert to {@see AsyncDataStream.OutputMode}, ORDERED by default. "
                                    + "If set to ALLOW_UNORDERED, will attempt to use {@see AsyncDataStream.OutputMode.UNORDERED} when it does not "
                                    + "affect the correctness of the result, otherwise ORDERED will be still used.");

    public static final ConfigOption<Integer> ASYNC_MAX_CONCURRENT_OPERATIONS =
            key("max-concurrent-operations")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The max number of async i/o operation that the async ml predict can trigger.");

    public static final ConfigOption<Duration> ASYNC_TIMEOUT =
            key("timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Timeout from first invoke to final completion of asynchronous operation, may include multiple"
                                    + " retries, and will be reset in case of failover.");

    private static final Set<ConfigOption<?>> supportedKeys = new HashSet<>();

    static {
        supportedKeys.add(ASYNC);
        supportedKeys.add(ASYNC_OUTPUT_MODE);
        supportedKeys.add(ASYNC_MAX_CONCURRENT_OPERATIONS);
        supportedKeys.add(ASYNC_TIMEOUT);
    }

    public static ImmutableSet<ConfigOption> getSupportedOptions() {
        return ImmutableSet.copyOf(supportedKeys);
    }
}
