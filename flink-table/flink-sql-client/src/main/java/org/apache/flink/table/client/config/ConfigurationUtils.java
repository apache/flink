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

package org.apache.flink.table.client.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.client.config.entries.ExecutionEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptions.AUTO_WATERMARK_INTERVAL;
import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_PLANNER;
import static org.apache.flink.table.client.config.Environment.EXECUTION_ENTRY;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_MAX_TABLE_RESULT_ROWS;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;

/**
 * Convert {@link ExecutionEntry} to {@link Configuration}.
 *
 * <ul>
 *   The rule is simple.
 *   <li>If the key is from {@link ExecutionEntry}, set the key and corresponding {@link
 *       ConfigOption} at the same time.
 *   <li>If the key is {@link ConfigOption} and corresponding {@link ExecutionEntry} is not set, set
 *       the value for {@link ConfigOption} only.
 *   <li>If the key is {@link ConfigOption} and corresponding {@link ExecutionEntry} is set, set the
 *       value for both.
 * </ul>
 *
 * <p>When read all entries from the {@link Configuration}, read the key from {@link ExecutionEntry}
 * first and then read the {@link ConfigOption}.
 *
 * <p>When YAML is removed from the project, it should also remove this helper class.
 */
@Deprecated
public class ConfigurationUtils {

    static Map<String, String> entryToConfigOptions = new HashMap<>();
    static Map<String, String> configOptionToEntries = new HashMap<>();
    static Set<String> deprecatedEntries = new HashSet<>();

    static {
        // EnvironmentSettings
        entryToConfigOptions.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_TYPE),
                RUNTIME_MODE.key());
        entryToConfigOptions.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PLANNER),
                TABLE_PLANNER.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PERIODIC_WATERMARKS_INTERVAL),
                AUTO_WATERMARK_INTERVAL.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MIN_STATE_RETENTION),
                IDLE_STATE_RETENTION.key());
        entryToConfigOptions.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PARALLELISM),
                DEFAULT_PARALLELISM.key());
        entryToConfigOptions.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_PARALLELISM),
                MAX_PARALLELISM.key());
        entryToConfigOptions.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESULT_MODE),
                EXECUTION_RESULT_MODE.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_TABLE_RESULT_ROWS),
                EXECUTION_MAX_TABLE_RESULT_ROWS.key());
        // restart strategy
        entryToConfigOptions.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_TYPE),
                RESTART_STRATEGY.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_DELAY),
                RESTART_STRATEGY_FIXED_DELAY_DELAY.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_ATTEMPTS),
                RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL),
                RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL),
                RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL.key());
        entryToConfigOptions.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL),
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY.key());
    }

    static {
        configOptionToEntries.put(
                RUNTIME_MODE.key(),
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_TYPE));
        configOptionToEntries.put(
                TABLE_PLANNER.key(),
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PLANNER));
        configOptionToEntries.put(
                AUTO_WATERMARK_INTERVAL.key(),
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PERIODIC_WATERMARKS_INTERVAL));
        configOptionToEntries.put(
                IDLE_STATE_RETENTION.key(),
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MIN_STATE_RETENTION));
        configOptionToEntries.put(
                DEFAULT_PARALLELISM.key(),
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PARALLELISM));
        configOptionToEntries.put(
                MAX_PARALLELISM.key(),
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_PARALLELISM));
        configOptionToEntries.put(
                EXECUTION_RESULT_MODE.key(),
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESULT_MODE));
        configOptionToEntries.put(
                EXECUTION_MAX_TABLE_RESULT_ROWS.key(),
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_TABLE_RESULT_ROWS));
        // restart strategy
        configOptionToEntries.put(
                RESTART_STRATEGY.key(),
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_TYPE));
        configOptionToEntries.put(
                RESTART_STRATEGY_FIXED_DELAY_DELAY.key(),
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_DELAY));
        configOptionToEntries.put(
                RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS.key(),
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_ATTEMPTS));
        configOptionToEntries.put(
                RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL.key(),
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL));
        configOptionToEntries.put(
                RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL.key(),
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL));
        configOptionToEntries.put(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY.key(),
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL));
    }

    static {
        deprecatedEntries.add(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_TIME_CHARACTERISTIC));
        deprecatedEntries.add(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_STATE_RETENTION));
        deprecatedEntries.add(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_CURRENT_CATALOG));
        deprecatedEntries.add(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_CURRENT_DATABASE));
    }

    // --------------------------------------------------------------------------------------------

    public static boolean isDeprecatedKey(String key) {
        return deprecatedEntries.contains(key);
    }

    public static boolean isYamlKey(String key) {
        return deprecatedEntries.contains(key) || entryToConfigOptions.containsKey(key);
    }

    // --------------------------------------------------------------------------------------------

    public static void setKeyToConfiguration(
            Configuration configuration, String key, String value) {
        // ignore deprecated key
        if (isDeprecatedKey(key)) {
            return;
        }
        if (entryToConfigOptions.containsKey(key)) {
            configuration.setString(key, value);
            configuration.setString(entryToConfigOptions.get(key), value);
        } else {
            if (configOptionToEntries.containsKey(key)
                    && configuration.containsKey(configOptionToEntries.get(key))) {
                configuration.setString(configOptionToEntries.get(key), value);
            }
            configuration.setString(key, value);
        }
    }

    public static Configuration convertExecutionEntryToConfiguration(ExecutionEntry execution) {
        Configuration configuration = new Configuration();
        Map<String, String> executionEntry = execution.asMap();
        for (Map.Entry<String, String> entry : executionEntry.entrySet()) {
            String key = String.format("%s.%s", EXECUTION_ENTRY, entry.getKey());
            if (isDeprecatedKey(key)) {
                continue;
            }
            configuration.setString(key, entry.getValue());
            configuration.setString(entryToConfigOptions.get(key), entry.getValue());
        }
        setRestartStrategy(execution, configuration);
        return configuration;
    }

    public static List<String> getPropertiesInPretty(Map<String, String> properties) {
        // first extract YAML key
        List<String> prettyEntries = new ArrayList<>();
        for (String key : properties.keySet()) {
            if (isYamlKey(key)) {
                prettyEntries.add(String.format("[DEPRECATED]%s=%s", key, properties.get(key)));
            }
        }
        prettyEntries.sort(String::compareTo);

        // add the configuration key
        List<String> prettyConfigOptions = new ArrayList<>();
        for (String key : properties.keySet()) {
            if (!isYamlKey(key)) {
                prettyConfigOptions.add(String.format("%s=%s", key, properties.get(key)));
            }
        }
        prettyConfigOptions.sort(String::compareTo);

        prettyEntries.addAll(prettyConfigOptions);
        return prettyEntries;
    }

    // --------------------------------------------------------------------------------------------

    private static void setRestartStrategy(ExecutionEntry execution, Configuration configuration) {
        RestartStrategies.RestartStrategyConfiguration restartStrategy =
                execution.getRestartStrategy();
        if (restartStrategy instanceof RestartStrategies.NoRestartStrategyConfiguration) {
            configuration.setString(
                    configOptionToEntries.get(RestartStrategyOptions.RESTART_STRATEGY.key()),
                    "none");
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        } else if (restartStrategy
                instanceof RestartStrategies.FixedDelayRestartStrategyConfiguration) {
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            RestartStrategies.FixedDelayRestartStrategyConfiguration fixedDelay =
                    ((RestartStrategies.FixedDelayRestartStrategyConfiguration) restartStrategy);
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                    fixedDelay.getRestartAttempts());
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                    Duration.ofMillis(
                            fixedDelay.getDelayBetweenAttemptsInterval().toMilliseconds()));

            configuration.setString(
                    configOptionToEntries.get(RestartStrategyOptions.RESTART_STRATEGY.key()),
                    "fixed-delay");

        } else if (restartStrategy
                instanceof RestartStrategies.FailureRateRestartStrategyConfiguration) {
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
            RestartStrategies.FailureRateRestartStrategyConfiguration failureRate =
                    (RestartStrategies.FailureRateRestartStrategyConfiguration) restartStrategy;
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL,
                    failureRate.getMaxFailureRate());
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
                    Duration.ofMillis(failureRate.getFailureInterval().toMilliseconds()));
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY,
                    Duration.ofMillis(
                            failureRate.getDelayBetweenAttemptsInterval().toMilliseconds()));

            configuration.setString(
                    configOptionToEntries.get(RestartStrategyOptions.RESTART_STRATEGY.key()),
                    "failure-rate");

        } else if (restartStrategy
                instanceof RestartStrategies.FallbackRestartStrategyConfiguration) {
            // default is FallbackRestartStrategyConfiguration
            // see ExecutionConfig.restartStrategyConfiguration
            configuration.removeConfig(RestartStrategyOptions.RESTART_STRATEGY);
        }
    }
}
