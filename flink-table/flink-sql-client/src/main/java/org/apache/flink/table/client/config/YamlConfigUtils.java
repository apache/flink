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
 * @deprecated This will be removed in Flink 1.14 with dropping support of {@code sql-client.yaml}
 *     configuration file.
 */
@Deprecated
public class YamlConfigUtils {

    static final Map<String, String> ENTRY_TO_OPTION = new HashMap<>();
    static final Map<String, String> OPTION_TO_ENTRY = new HashMap<>();
    static final Set<String> REMOVED_ENTRY = new HashSet<>();

    static {
        // EnvironmentSettings
        ENTRY_TO_OPTION.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_TYPE),
                RUNTIME_MODE.key());
        ENTRY_TO_OPTION.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PLANNER),
                TABLE_PLANNER.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PERIODIC_WATERMARKS_INTERVAL),
                AUTO_WATERMARK_INTERVAL.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MIN_STATE_RETENTION),
                IDLE_STATE_RETENTION.key());
        ENTRY_TO_OPTION.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_PARALLELISM),
                DEFAULT_PARALLELISM.key());
        ENTRY_TO_OPTION.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_PARALLELISM),
                MAX_PARALLELISM.key());
        ENTRY_TO_OPTION.put(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESULT_MODE),
                EXECUTION_RESULT_MODE.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_TABLE_RESULT_ROWS),
                EXECUTION_MAX_TABLE_RESULT_ROWS.key());
        // restart strategy
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_TYPE),
                RESTART_STRATEGY.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_DELAY),
                RESTART_STRATEGY_FIXED_DELAY_DELAY.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY, ExecutionEntry.EXECUTION_RESTART_STRATEGY_ATTEMPTS),
                RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL),
                RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL),
                RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL.key());
        ENTRY_TO_OPTION.put(
                String.format(
                        "%s.%s",
                        EXECUTION_ENTRY,
                        ExecutionEntry.EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL),
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY.key());
    }

    static {
        for (String key : ENTRY_TO_OPTION.keySet()) {
            OPTION_TO_ENTRY.put(ENTRY_TO_OPTION.get(key), key);
        }
    }

    static {
        REMOVED_ENTRY.add(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_TIME_CHARACTERISTIC));
        REMOVED_ENTRY.add(
                String.format(
                        "%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_MAX_STATE_RETENTION));
        REMOVED_ENTRY.add(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_CURRENT_CATALOG));
        REMOVED_ENTRY.add(
                String.format("%s.%s", EXECUTION_ENTRY, ExecutionEntry.EXECUTION_CURRENT_DATABASE));
    }

    // --------------------------------------------------------------------------------------------

    public static boolean isRemovedKey(String key) {
        return REMOVED_ENTRY.contains(key);
    }

    public static boolean isDeprecatedKey(String key) {
        return ENTRY_TO_OPTION.containsKey(key);
    }

    public static String getOptionNameWithDeprecatedKey(String key) {
        return ENTRY_TO_OPTION.get(key);
    }

    // --------------------------------------------------------------------------------------------

    public static void setKeyToConfiguration(
            Configuration configuration, String key, String value) {
        if (isRemovedKey(key)) {
            return;
        }
        configuration.setString(key, value);
        if (ENTRY_TO_OPTION.containsKey(key)) {
            // old key => set new key
            configuration.setString(ENTRY_TO_OPTION.get(key), value);
        } else if (OPTION_TO_ENTRY.containsKey(key)
                && configuration.containsKey(OPTION_TO_ENTRY.get(key))) {
            // new key && old key exist => set old key
            configuration.setString(OPTION_TO_ENTRY.get(key), value);
        }
    }

    public static Configuration convertExecutionEntryToConfiguration(ExecutionEntry execution) {
        Configuration configuration = new Configuration();
        Map<String, String> executionEntry = execution.asMap();
        for (Map.Entry<String, String> entry : executionEntry.entrySet()) {
            String key = String.format("%s.%s", EXECUTION_ENTRY, entry.getKey());
            if (isRemovedKey(key)) {
                continue;
            }
            configuration.setString(key, entry.getValue());
            configuration.setString(ENTRY_TO_OPTION.get(key), entry.getValue());
        }
        setRestartStrategy(execution, configuration);
        return configuration;
    }

    public static List<String> getPropertiesInPretty(Map<String, String> properties) {
        List<String> prettyConfigOptions = new ArrayList<>();
        for (String key : properties.keySet()) {
            if (!isRemovedKey(key) && !isDeprecatedKey(key)) {
                prettyConfigOptions.add(String.format("%s=%s", key, properties.get(key)));
            }
        }
        prettyConfigOptions.sort(String::compareTo);

        List<String> prettyEntries = new ArrayList<>();
        for (String key : properties.keySet()) {
            if (isDeprecatedKey(key)) {
                prettyEntries.add(String.format("[DEPRECATED] %s=%s", key, properties.get(key)));
            }
        }
        prettyEntries.sort(String::compareTo);

        prettyConfigOptions.addAll(prettyEntries);
        return prettyConfigOptions;
    }

    // --------------------------------------------------------------------------------------------

    private static void setRestartStrategy(ExecutionEntry execution, Configuration configuration) {
        RestartStrategies.RestartStrategyConfiguration restartStrategy =
                execution.getRestartStrategy();
        if (restartStrategy instanceof RestartStrategies.NoRestartStrategyConfiguration) {
            configuration.setString(
                    OPTION_TO_ENTRY.get(RestartStrategyOptions.RESTART_STRATEGY.key()), "none");
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
                    OPTION_TO_ENTRY.get(RestartStrategyOptions.RESTART_STRATEGY.key()),
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
                    OPTION_TO_ENTRY.get(RestartStrategyOptions.RESTART_STRATEGY.key()),
                    "failure-rate");

        } else if (restartStrategy
                instanceof RestartStrategies.FallbackRestartStrategyConfiguration) {
            // default is FallbackRestartStrategyConfiguration
            // see ExecutionConfig.restartStrategyConfiguration
            configuration.removeConfig(RestartStrategyOptions.RESTART_STRATEGY);
        }
    }
}
