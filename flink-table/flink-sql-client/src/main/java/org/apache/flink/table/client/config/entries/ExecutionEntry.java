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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamPipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.ConfigUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.client.config.Environment.EXECUTION_ENTRY;

/**
 * Configuration of a table program execution. This class parses the `execution` part
 * in an environment file. The execution describes properties that would usually be defined in the
 * ExecutionEnvironment/StreamExecutionEnvironment/TableEnvironment or as code in a Flink job.
 *
 * <p>All properties of this entry are optional and evaluated lazily.
 */
public class ExecutionEntry extends ConfigEntry {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionEntry.class);

	public static final ExecutionEntry DEFAULT_INSTANCE =
		new ExecutionEntry(new DescriptorProperties(true));

	private static final String EXECUTION_PLANNER = "planner";

	public static final String EXECUTION_PLANNER_VALUE_OLD = "old";

	public static final String EXECUTION_PLANNER_VALUE_BLINK = "blink";

	private static final String EXECUTION_TYPE = "type";

	private static final String EXECUTION_TYPE_VALUE_STREAMING = "streaming";

	private static final String EXECUTION_TYPE_VALUE_BATCH = "batch";

	@Deprecated
	// use StreamPipelineOptions.TIME_CHARACTERISTIC instead
	private static final String EXECUTION_TIME_CHARACTERISTIC = "time-characteristic";

	private static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME = "event-time";

	private static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME = "processing-time";

	@Deprecated
	// use PipelineOptions.AUTO_WATERMARK_INTERVAL instead
	private static final String EXECUTION_PERIODIC_WATERMARKS_INTERVAL = "periodic-watermarks-interval";

	private static final String EXECUTION_MIN_STATE_RETENTION = "min-idle-state-retention";

	private static final String EXECUTION_MAX_STATE_RETENTION = "max-idle-state-retention";

	@Deprecated
	// use CoreOptions.DEFAULT_PARALLELISM instead
	private static final String EXECUTION_PARALLELISM = "parallelism";

	@Deprecated
	// use PipelineOptions.MAX_PARALLELISM instead
	private static final String EXECUTION_MAX_PARALLELISM = "max-parallelism";

	private static final String EXECUTION_RESULT_MODE = "result-mode";

	private static final String EXECUTION_RESULT_MODE_VALUE_CHANGELOG = "changelog";

	private static final String EXECUTION_RESULT_MODE_VALUE_TABLE = "table";

	private static final String EXECUTION_RESULT_MODE_VALUE_TABLEAU = "tableau";

	private static final String EXECUTION_MAX_TABLE_RESULT_ROWS = "max-table-result-rows";

	@Deprecated
	// use RestartStrategyOptions.RESTART_STRATEGY instead
	private static final String EXECUTION_RESTART_STRATEGY_TYPE = "restart-strategy.type";

	private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK = "fallback";

	private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE = "none";

	private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY = "fixed-delay";

	private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE = "failure-rate";

	@Deprecated
	// use RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS instead
	private static final String EXECUTION_RESTART_STRATEGY_ATTEMPTS = "restart-strategy.attempts";

	@Deprecated
	// use RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY for "failure-rate" strategy
	// or RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY for "fixed-delay" strategy instead
	private static final String EXECUTION_RESTART_STRATEGY_DELAY = "restart-strategy.delay";

	@Deprecated
	// use RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL instead
	private static final String EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL = "restart-strategy.failure-rate-interval";

	@Deprecated
	// use RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL instead
	private static final String EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL = "restart-strategy.max-failures-per-interval";

	public static final String EXECUTION_CURRENT_CATALOG = "current-catalog";

	public static final String EXECUTION_CURRENT_DATABASE = "current-database";

	private ExecutionEntry(DescriptorProperties properties) {
		super(properties);
	}

	private static final Set<String> ACCEPTED_KEY_LISTS = Sets.newHashSet(
			getFullKey(EXECUTION_PLANNER),
			getFullKey(EXECUTION_TYPE),
			getFullKey(EXECUTION_TIME_CHARACTERISTIC),
			getFullKey(EXECUTION_PERIODIC_WATERMARKS_INTERVAL),
			getFullKey(EXECUTION_MIN_STATE_RETENTION),
			getFullKey(EXECUTION_MAX_STATE_RETENTION),
			getFullKey(EXECUTION_PARALLELISM),
			getFullKey(EXECUTION_MAX_PARALLELISM),
			getFullKey(EXECUTION_RESULT_MODE),
			getFullKey(EXECUTION_MAX_TABLE_RESULT_ROWS),
			getFullKey(EXECUTION_RESTART_STRATEGY_TYPE),
			getFullKey(EXECUTION_RESTART_STRATEGY_ATTEMPTS),
			getFullKey(EXECUTION_RESTART_STRATEGY_DELAY),
			getFullKey(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL),
			getFullKey(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL),
			getFullKey(EXECUTION_CURRENT_CATALOG),
			getFullKey(EXECUTION_CURRENT_DATABASE)
	);

	public static Map<String, String> getDeprecatedKeyToNewKeyMap() {
		Map<String, String> map = new HashMap<>();
		map.put(getFullKey(EXECUTION_TIME_CHARACTERISTIC), StreamPipelineOptions.TIME_CHARACTERISTIC.key());
		map.put(getFullKey(EXECUTION_PERIODIC_WATERMARKS_INTERVAL), PipelineOptions.AUTO_WATERMARK_INTERVAL.key());
		map.put(getFullKey(EXECUTION_PARALLELISM), CoreOptions.DEFAULT_PARALLELISM.key());
		map.put(getFullKey(EXECUTION_MAX_PARALLELISM), PipelineOptions.MAX_PARALLELISM.key());
		map.put(getFullKey(EXECUTION_RESTART_STRATEGY_TYPE), RestartStrategyOptions.RESTART_STRATEGY.key());
		map.put(getFullKey(EXECUTION_RESTART_STRATEGY_ATTEMPTS),
				RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS.key());
		map.put(getFullKey(EXECUTION_RESTART_STRATEGY_DELAY),
				RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY.key() + ", " +
						RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY.key());
		map.put(getFullKey(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL),
				RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL.key());
		map.put(getFullKey(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL),
				RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL.key());
		return map;
	}

	public static Map<String, String> getNewKeyToDeprecatedKeyMap() {
		Map<String, String> map = new HashMap<>();
		for (Map.Entry<String, String> entry : getDeprecatedKeyToNewKeyMap().entrySet()) {
			for (String value : entry.getValue().split(",")) {
				if (map.put(value.trim(), entry.getKey()) != null) {
					throw new SqlClientException("This should not happen.");
				}
			}
		}
		return map;
	}

	private static String getFullKey(String key) {
		return EXECUTION_ENTRY + "." + key;
	}

	@Override
	protected void validate(DescriptorProperties properties) {
		properties.validateEnumValues(
			EXECUTION_PLANNER,
			true,
			Arrays.asList(
				EXECUTION_PLANNER_VALUE_OLD,
				EXECUTION_PLANNER_VALUE_BLINK));
		properties.validateEnumValues(
			EXECUTION_TYPE,
			true,
			Arrays.asList(
				EXECUTION_TYPE_VALUE_BATCH,
				EXECUTION_TYPE_VALUE_STREAMING));
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

	public EnvironmentSettings getEnvironmentSettings() {
		final EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();

		if (inStreamingMode()) {
			builder.inStreamingMode();
		} else if (inBatchMode()) {
			builder.inBatchMode();
		}

		final String planner = properties.getOptionalString(EXECUTION_PLANNER)
			.orElse(EXECUTION_PLANNER_VALUE_BLINK);

		if (planner.equals(EXECUTION_PLANNER_VALUE_OLD)) {
			builder.useOldPlanner();
		} else if (planner.equals(EXECUTION_PLANNER_VALUE_BLINK)) {
			builder.useBlinkPlanner();
		}

		return builder.build();
	}

	public boolean inStreamingMode() {
		return properties.getOptionalString(EXECUTION_TYPE)
				.map((v) -> v.equals(EXECUTION_TYPE_VALUE_STREAMING))
				.orElse(false);
	}

	public boolean inBatchMode() {
		return properties.getOptionalString(EXECUTION_TYPE)
				.map((v) -> v.equals(EXECUTION_TYPE_VALUE_BATCH))
				.orElse(false);
	}

	public boolean isStreamingPlanner() {
		final String planner = properties.getOptionalString(EXECUTION_PLANNER)
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

	public boolean isBatchPlanner() {
		final String planner = properties.getOptionalString(EXECUTION_PLANNER)
			.orElse(EXECUTION_PLANNER_VALUE_BLINK);

		// Blink planner is not a batch planner
		if (planner.equals(EXECUTION_PLANNER_VALUE_BLINK)) {
			return false;
		}
		// Old planner can be a streaming or batch planner
		else if (planner.equals(EXECUTION_PLANNER_VALUE_OLD)) {
			return inBatchMode();
		}

		return false;
	}

	public boolean isBlinkPlanner() {
		final String planner = properties.getOptionalString(EXECUTION_PLANNER)
			.orElse(EXECUTION_PLANNER_VALUE_BLINK);
		if (planner.equals(EXECUTION_PLANNER_VALUE_OLD)) {
			return false;
		}
		return true;
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return properties.getOptionalString(EXECUTION_TIME_CHARACTERISTIC)
			.flatMap((v) -> {
				switch (v) {
					case EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME:
						return Optional.of(TimeCharacteristic.EventTime);
					case EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME:
						return Optional.of(TimeCharacteristic.ProcessingTime);
					default:
						return Optional.empty();
				}
			})
			.orElseGet(() ->
				useDefaultValue(
					EXECUTION_TIME_CHARACTERISTIC,
					TimeCharacteristic.EventTime,
					EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME));
	}

	public long getPeriodicWatermarksInterval() {
		return properties.getOptionalLong(EXECUTION_PERIODIC_WATERMARKS_INTERVAL)
			.orElseGet(() -> useDefaultValue(EXECUTION_PERIODIC_WATERMARKS_INTERVAL, 200L));
	}

	public long getMinStateRetention() {
		return properties.getOptionalLong(EXECUTION_MIN_STATE_RETENTION)
			.orElseGet(() -> useDefaultValue(EXECUTION_MIN_STATE_RETENTION, 0L));
	}

	public long getMaxStateRetention() {
		return properties.getOptionalLong(EXECUTION_MAX_STATE_RETENTION)
			.orElseGet(() -> useDefaultValue(EXECUTION_MAX_STATE_RETENTION, 0L));
	}

	public Optional<Integer> getParallelism() {
		return properties.getOptionalInt(EXECUTION_PARALLELISM);
	}

	public int getMaxParallelism() {
		return properties.getOptionalInt(EXECUTION_MAX_PARALLELISM)
			.orElseGet(() -> useDefaultValue(EXECUTION_MAX_PARALLELISM, 128));
	}

	public int getMaxTableResultRows() {
		return properties.getOptionalInt(EXECUTION_MAX_TABLE_RESULT_ROWS)
			.orElseGet(() -> useDefaultValue(EXECUTION_MAX_TABLE_RESULT_ROWS, 1_000_000));
	}

	public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
		return properties.getOptionalString(EXECUTION_RESTART_STRATEGY_TYPE)
			.flatMap((v) -> {
				switch (v) {
					case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE:
						return Optional.of(RestartStrategies.noRestart());
					case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY:
						final int attempts = properties.getOptionalInt(EXECUTION_RESTART_STRATEGY_ATTEMPTS)
							.orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_ATTEMPTS, Integer.MAX_VALUE));
						final long fixedDelay = properties.getOptionalLong(EXECUTION_RESTART_STRATEGY_DELAY)
							.orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_DELAY, 10_000L));
						return Optional.of(RestartStrategies.fixedDelayRestart(attempts, fixedDelay));
					case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE:
						final int failureRate = properties.getOptionalInt(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL)
							.orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL, 1));
						final long failureInterval = properties.getOptionalLong(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL)
							.orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL, 60_000L));
						final long attemptDelay = properties.getOptionalLong(EXECUTION_RESTART_STRATEGY_DELAY)
							.orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_DELAY, 10_000L));
						return Optional.of(RestartStrategies.failureRateRestart(
							failureRate,
							Time.milliseconds(failureInterval),
							Time.milliseconds(attemptDelay)));
					default:
						return Optional.empty();
					}
			})
			.orElseGet(() ->
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

	public boolean isChangelogMode() {
		return properties.getOptionalString(EXECUTION_RESULT_MODE)
			.map((v) -> v.equals(EXECUTION_RESULT_MODE_VALUE_CHANGELOG))
			.orElse(false);
	}

	public boolean isTableMode() {
		return properties.getOptionalString(EXECUTION_RESULT_MODE)
			.map((v) -> v.equals(EXECUTION_RESULT_MODE_VALUE_TABLE))
			.orElse(false);
	}

	public boolean isTableauMode() {
		return properties.getOptionalString(EXECUTION_RESULT_MODE)
				.map((v) -> v.equals(EXECUTION_RESULT_MODE_VALUE_TABLEAU))
				.orElse(false);
	}

	public Map<String, String> asTopLevelMap() {
		return properties.asPrefixedMap(EXECUTION_ENTRY + '.');
	}

	private <V> V useDefaultValue(String key, V defaultValue) {
		return useDefaultValue(key, defaultValue, defaultValue.toString());
	}

	private <V> V useDefaultValue(String key, V defaultValue, String defaultString) {
		LOG.info("Property '{}.{}' not specified. Using default value: {}", EXECUTION_ENTRY, key, defaultString);
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
	 * Creates a new execution entry enriched with additional properties that are defined in
	 * {@link ExecutionEntry#ACCEPTED_KEY_LISTS}.
	 */
	public static ExecutionEntry enrich(ExecutionEntry execution, Map<String, String> remainingPrefixedProperties) {
		final Map<String, String> enrichedProperties = new HashMap<>(execution.asMap());

		Iterator<Map.Entry<String, String>> it = remainingPrefixedProperties.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			final String normalizedKey = entry.getKey().toLowerCase();
			if (ACCEPTED_KEY_LISTS.contains(normalizedKey)) {
				enrichedProperties.put(normalizedKey.substring(EXECUTION_ENTRY.length() + 1), entry.getValue());
				it.remove();
			}
		}

		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(enrichedProperties);

		return new ExecutionEntry(properties);
	}
}
