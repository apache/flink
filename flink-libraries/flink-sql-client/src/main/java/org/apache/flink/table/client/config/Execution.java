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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration of a table program execution. This class parses the `execution` part
 * in an environment file. The execution describes properties that would usually be defined in the
 * ExecutionEnvironment/StreamExecutionEnvironment/TableEnvironment or as code in a Flink job.
 */
public class Execution {

	private final Map<String, String> properties;

	public Execution() {
		this.properties = Collections.emptyMap();
	}

	private Execution(Map<String, String> properties) {
		this.properties = properties;
	}

	// TODO add logger warnings if default value is used

	public boolean isStreamingExecution() {
		return Objects.equals(
			properties.get(PropertyStrings.EXECUTION_TYPE),
			PropertyStrings.EXECUTION_TYPE_VALUE_STREAMING);
	}

	public boolean isBatchExecution() {
		return Objects.equals(
			properties.get(PropertyStrings.EXECUTION_TYPE),
			PropertyStrings.EXECUTION_TYPE_VALUE_BATCH);
	}

	public TimeCharacteristic getTimeCharacteristic() {
		final String characteristic = properties.getOrDefault(
			PropertyStrings.EXECUTION_TIME_CHARACTERISTIC,
			PropertyStrings.EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME);
		switch (characteristic) {
			case PropertyStrings.EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME:
				return TimeCharacteristic.EventTime;
			case PropertyStrings.EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME:
				return TimeCharacteristic.ProcessingTime;
			default:
				return TimeCharacteristic.EventTime;
		}
	}

	public long getPeriodicWatermarksInterval() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.EXECUTION_PERIODIC_WATERMARKS_INTERVAL, Long.toString(200L)));
	}

	public long getMinStateRetention() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.EXECUTION_MIN_STATE_RETENTION, Long.toString(0)));
	}

	public long getMaxStateRetention() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.EXECUTION_MAX_STATE_RETENTION, Long.toString(0)));
	}

	public int getParallelism() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.EXECUTION_PARALLELISM, Integer.toString(1)));
	}

	public int getMaxParallelism() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.EXECUTION_MAX_PARALLELISM, Integer.toString(128)));
	}

	public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
		final String restartStrategy = properties.getOrDefault(
			PropertyStrings.EXECUTION_RESTART_STRATEGY_TYPE,
			PropertyStrings.EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK);
		switch (restartStrategy) {
			case PropertyStrings.EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE:
				return RestartStrategies.noRestart();
			case PropertyStrings.EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY:
				final int attempts = Integer.parseInt(
					properties.getOrDefault(
						PropertyStrings.EXECUTION_RESTART_STRATEGY_ATTEMPTS,
						Integer.toString(Integer.MAX_VALUE)));
				final long fixedDelay = Long.parseLong(
					properties.getOrDefault(
						PropertyStrings.EXECUTION_RESTART_STRATEGY_DELAY,
						Long.toString(10_000)));
				return RestartStrategies.fixedDelayRestart(attempts, fixedDelay);
			case PropertyStrings.EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE:
				final int failureRate = Integer.parseInt(
					properties.getOrDefault(
						PropertyStrings.EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL,
						Integer.toString(1)));
				final long failureInterval = Long.parseLong(
					properties.getOrDefault(
						PropertyStrings.EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL,
						Long.toString(60_000)));
				final long attemptDelay = Long.parseLong(
					properties.getOrDefault(
						PropertyStrings.EXECUTION_RESTART_STRATEGY_DELAY,
						Long.toString(10_000)));
				return RestartStrategies.failureRateRestart(
					failureRate,
					Time.milliseconds(failureInterval),
					Time.milliseconds(attemptDelay));
			default:
				return RestartStrategies.fallBackRestart();
		}
	}

	public boolean isChangelogMode() {
		return Objects.equals(
			properties.get(PropertyStrings.EXECUTION_RESULT_MODE),
			PropertyStrings.EXECUTION_RESULT_MODE_VALUE_CHANGELOG);
	}

	public boolean isTableMode() {
		return Objects.equals(
				properties.get(PropertyStrings.EXECUTION_RESULT_MODE),
				PropertyStrings.EXECUTION_RESULT_MODE_VALUE_TABLE);
	}

	public Map<String, String> toProperties() {
		final Map<String, String> copy = new HashMap<>();
		properties.forEach((k, v) -> copy.put(PropertyStrings.EXECUTION + "." + k, v));
		return copy;
	}

	// --------------------------------------------------------------------------------------------

	public static Execution create(Map<String, Object> config) {
		return new Execution(ConfigUtil.normalizeYaml(config));
	}

	/**
	 * Merges two executions. The properties of the first execution might be overwritten by the second one.
	 */
	public static Execution merge(Execution exec1, Execution exec2) {
		final Map<String, String> newProperties = new HashMap<>(exec1.properties);
		newProperties.putAll(exec2.properties);

		return new Execution(newProperties);
	}

	/**
	 * Creates a new execution enriched with additional properties.
	 */
	public static Execution enrich(Execution exec, Map<String, String> properties) {
		final Map<String, String> newProperties = new HashMap<>(exec.properties);
		properties.forEach((k, v) -> {
			final String normalizedKey = k.toLowerCase();
			if (k.startsWith(PropertyStrings.EXECUTION + ".")) {
				newProperties.put(normalizedKey.substring(PropertyStrings.EXECUTION.length() + 1), v);
			}
		});

		return new Execution(newProperties);
	}
}
