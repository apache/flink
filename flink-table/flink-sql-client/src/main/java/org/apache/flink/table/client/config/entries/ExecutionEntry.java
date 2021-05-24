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

package org.apache.flink.table.client.config.entries;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.client.config.ConfigUtil;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.descriptors.DescriptorProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.client.config.Environment.EXECUTION_ENTRY;

/**
 * Configuration of a table program execution. This class parses the `execution` part in an
 * environment file. The execution describes properties that would usually be defined in the
 * ExecutionEnvironment/StreamExecutionEnvironment/TableEnvironment or as code in a Flink job.
 *
 * <p>All properties of this entry are optional and evaluated lazily.
 *
 * @deprecated This will be removed in Flink 1.14 with dropping support of {@code sql-client.yaml}
 *     configuration file.
 */
@Deprecated
public class ExecutionEntry extends ConfigEntry {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionEntry.class);

    public static final ExecutionEntry DEFAULT_INSTANCE =
            new ExecutionEntry(new DescriptorProperties(true));

    public static final String EXECUTION_PLANNER = "planner";

    public static final String EXECUTION_PLANNER_VALUE_OLD = "old";

    public static final String EXECUTION_PLANNER_VALUE_BLINK = "blink";

    public static final String EXECUTION_TYPE = "type";

    public static final String EXECUTION_TYPE_VALUE_STREAMING = "streaming";

    private static final String EXECUTION_TYPE_VALUE_BATCH = "batch";

    public static final String EXECUTION_TIME_CHARACTERISTIC = "time-characteristic";

    private static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME = "event-time";

    private static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME =
            "processing-time";

    public static final String EXECUTION_PERIODIC_WATERMARKS_INTERVAL =
            "periodic-watermarks-interval";

    public static final String EXECUTION_MIN_STATE_RETENTION = "min-idle-state-retention";

    public static final String EXECUTION_MAX_STATE_RETENTION = "max-idle-state-retention";

    public static final String EXECUTION_PARALLELISM = "parallelism";

    public static final String EXECUTION_MAX_PARALLELISM = "max-parallelism";

    public static final String EXECUTION_RESULT_MODE = "result-mode";

    public static final String EXECUTION_MAX_TABLE_RESULT_ROWS = "max-table-result-rows";

    public static final String EXECUTION_RESTART_STRATEGY_TYPE = "restart-strategy.type";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK = "fallback";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE = "none";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY = "fixed-delay";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE = "failure-rate";

    public static final String EXECUTION_RESTART_STRATEGY_ATTEMPTS = "restart-strategy.attempts";

    public static final String EXECUTION_RESTART_STRATEGY_DELAY = "restart-strategy.delay";

    public static final String EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL =
            "restart-strategy.failure-rate-interval";

    public static final String EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL =
            "restart-strategy.max-failures-per-interval";

    public static final String EXECUTION_CURRENT_CATALOG = "current-catalog";

    public static final String EXECUTION_CURRENT_DATABASE = "current-database";

    private ExecutionEntry(DescriptorProperties properties) {
        super(properties);
    }

    @Override
    protected void validate(DescriptorProperties properties) {
        properties.validateEnumValues(
                EXECUTION_PLANNER,
                true,
                Arrays.asList(EXECUTION_PLANNER_VALUE_OLD, EXECUTION_PLANNER_VALUE_BLINK));
        properties.validateEnumValues(
                EXECUTION_TYPE,
                true,
                Arrays.asList(EXECUTION_TYPE_VALUE_BATCH, EXECUTION_TYPE_VALUE_STREAMING));
        properties.validateEnumValues(
                EXECUTION_TIME_CHARACTERISTIC,
                true,
                Arrays.asList(
                        EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME,
                        EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME));
        properties.validateLong(EXECUTION_PERIODIC_WATERMARKS_INTERVAL, true, 1);
        properties.validateLong(EXECUTION_MIN_STATE_RETENTION, true, 0);
        properties.validateLong(EXECUTION_MAX_STATE_RETENTION, true, 0);
        properties.validateInt(EXECUTION_PARALLELISM, true, 1);
        properties.validateInt(EXECUTION_MAX_PARALLELISM, true, 1);
        properties.validateInt(EXECUTION_MAX_TABLE_RESULT_ROWS, true, 1);
        properties.validateEnumValues(
                EXECUTION_RESTART_STRATEGY_TYPE,
                true,
                Arrays.asList(
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK,
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE,
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY,
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE));
        properties.validateInt(EXECUTION_RESTART_STRATEGY_ATTEMPTS, true, 1);
        properties.validateLong(EXECUTION_RESTART_STRATEGY_DELAY, true, 0);
        properties.validateLong(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL, true, 1);
        properties.validateInt(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL, true, 1);
        properties.validateString(EXECUTION_CURRENT_CATALOG, true, 1);
        properties.validateString(EXECUTION_CURRENT_DATABASE, true, 1);
    }

    public boolean inStreamingMode() {
        return properties
                .getOptionalString(EXECUTION_TYPE)
                .map((v) -> v.equals(EXECUTION_TYPE_VALUE_STREAMING))
                .orElse(true);
    }

    public boolean inBatchMode() {
        return properties
                .getOptionalString(EXECUTION_TYPE)
                .map((v) -> v.equals(EXECUTION_TYPE_VALUE_BATCH))
                .orElse(false);
    }

    public boolean isStreamingPlanner() {
        final String planner =
                properties
                        .getOptionalString(EXECUTION_PLANNER)
                        .orElse(EXECUTION_PLANNER_VALUE_BLINK);

        // Blink planner is a streaming planner
        if (planner.equals(EXECUTION_PLANNER_VALUE_BLINK)) {
            return true;
        }
        // Old planner can be a streaming or batch planner
        else if (planner.equals(EXECUTION_PLANNER_VALUE_OLD)) {
            return inStreamingMode();
        }

        return false;
    }

    public boolean isBlinkPlanner() {
        final String planner =
                properties
                        .getOptionalString(EXECUTION_PLANNER)
                        .orElse(EXECUTION_PLANNER_VALUE_BLINK);
        if (planner.equals(EXECUTION_PLANNER_VALUE_OLD)) {
            return false;
        }
        return true;
    }

    public Optional<Integer> getParallelism() {
        return properties.getOptionalInt(EXECUTION_PARALLELISM);
    }

    public int getMaxParallelism() {
        return properties
                .getOptionalInt(EXECUTION_MAX_PARALLELISM)
                .orElseGet(() -> useDefaultValue(EXECUTION_MAX_PARALLELISM, 128));
    }

    public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
        return properties
                .getOptionalString(EXECUTION_RESTART_STRATEGY_TYPE)
                .flatMap(
                        (v) -> {
                            switch (v) {
                                case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE:
                                    return Optional.of(RestartStrategies.noRestart());
                                case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY:
                                    final int attempts =
                                            properties
                                                    .getOptionalInt(
                                                            EXECUTION_RESTART_STRATEGY_ATTEMPTS)
                                                    .orElseGet(
                                                            () ->
                                                                    useDefaultValue(
                                                                            EXECUTION_RESTART_STRATEGY_ATTEMPTS,
                                                                            Integer.MAX_VALUE));
                                    final long fixedDelay =
                                            properties
                                                    .getOptionalLong(
                                                            EXECUTION_RESTART_STRATEGY_DELAY)
                                                    .orElseGet(
                                                            () ->
                                                                    useDefaultValue(
                                                                            EXECUTION_RESTART_STRATEGY_DELAY,
                                                                            10_000L));
                                    return Optional.of(
                                            RestartStrategies.fixedDelayRestart(
                                                    attempts, fixedDelay));
                                case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE:
                                    final int failureRate =
                                            properties
                                                    .getOptionalInt(
                                                            EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL)
                                                    .orElseGet(
                                                            () ->
                                                                    useDefaultValue(
                                                                            EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL,
                                                                            1));
                                    final long failureInterval =
                                            properties
                                                    .getOptionalLong(
                                                            EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL)
                                                    .orElseGet(
                                                            () ->
                                                                    useDefaultValue(
                                                                            EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL,
                                                                            60_000L));
                                    final long attemptDelay =
                                            properties
                                                    .getOptionalLong(
                                                            EXECUTION_RESTART_STRATEGY_DELAY)
                                                    .orElseGet(
                                                            () ->
                                                                    useDefaultValue(
                                                                            EXECUTION_RESTART_STRATEGY_DELAY,
                                                                            10_000L));
                                    return Optional.of(
                                            RestartStrategies.failureRateRestart(
                                                    failureRate,
                                                    Time.milliseconds(failureInterval),
                                                    Time.milliseconds(attemptDelay)));
                                default:
                                    return Optional.empty();
                            }
                        })
                .orElseGet(
                        () ->
                                useDefaultValue(
                                        EXECUTION_RESTART_STRATEGY_TYPE,
                                        RestartStrategies.fallBackRestart(),
                                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK));
    }

    public Optional<String> getCurrentCatalog() {
        return properties.getOptionalString(EXECUTION_CURRENT_CATALOG);
    }

    public Optional<String> getCurrentDatabase() {
        return properties.getOptionalString(EXECUTION_CURRENT_DATABASE);
    }

    public Map<String, String> asTopLevelMap() {
        return properties.asPrefixedMap(EXECUTION_ENTRY + '.');
    }

    private <V> V useDefaultValue(String key, V defaultValue) {
        return useDefaultValue(key, defaultValue, defaultValue.toString());
    }

    private <V> V useDefaultValue(String key, V defaultValue, String defaultString) {
        LOG.info(
                "Property '{}.{}' not specified. Using default value: {}",
                EXECUTION_ENTRY,
                key,
                defaultString);
        return defaultValue;
    }

    // --------------------------------------------------------------------------------------------

    public static ExecutionEntry create(Map<String, Object> config) {
        return new ExecutionEntry(ConfigUtil.normalizeYaml(config));
    }

    /**
     * Merges two execution entries. The properties of the first execution entry might be
     * overwritten by the second one.
     */
    public static ExecutionEntry merge(ExecutionEntry execution1, ExecutionEntry execution2) {
        final Map<String, String> mergedProperties = new HashMap<>(execution1.asMap());
        mergedProperties.putAll(execution2.asMap());

        final DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(mergedProperties);

        return new ExecutionEntry(properties);
    }

    /**
     * Creates a new execution entry enriched with additional properties that are prefixed with
     * {@link Environment#EXECUTION_ENTRY}.
     */
    public static ExecutionEntry enrich(
            ExecutionEntry execution, Map<String, String> prefixedProperties) {
        final Map<String, String> enrichedProperties = new HashMap<>(execution.asMap());

        prefixedProperties.forEach(
                (k, v) -> {
                    final String normalizedKey = k.toLowerCase();
                    if (k.startsWith(EXECUTION_ENTRY + '.')) {
                        enrichedProperties.put(
                                normalizedKey.substring(EXECUTION_ENTRY.length() + 1), v);
                    }
                });

        final DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(enrichedProperties);

        return new ExecutionEntry(properties);
    }
}
