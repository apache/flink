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
			properties.getOrDefault(PropertyStrings.EXECUTION_TYPE, PropertyStrings.EXECUTION_TYPE_VALUE_STREAMING),
			PropertyStrings.EXECUTION_TYPE_VALUE_STREAMING);
	}

	public boolean isBatchExecution() {
		return Objects.equals(
			properties.getOrDefault(PropertyStrings.EXECUTION_TYPE, PropertyStrings.EXECUTION_TYPE_VALUE_STREAMING),
			PropertyStrings.EXECUTION_TYPE_VALUE_BATCH);
	}

	public TimeCharacteristic getTimeCharacteristic() {
		final String s = properties.getOrDefault(
			PropertyStrings.EXECUTION_TIME_CHARACTERISTIC,
			PropertyStrings.EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME);
		switch (s) {
			case PropertyStrings.EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME:
				return TimeCharacteristic.EventTime;
			case PropertyStrings.EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME:
				return TimeCharacteristic.ProcessingTime;
			default:
				return TimeCharacteristic.EventTime;
		}
	}

	public long getMinStateRetention() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.EXECUTION_MIN_STATE_RETENTION, Long.toString(Long.MIN_VALUE)));
	}

	public long getMaxStateRetention() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.EXECUTION_MAX_STATE_RETENTION, Long.toString(Long.MIN_VALUE)));
	}

	public int getParallelism() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.EXECUTION_PARALLELISM, Integer.toString(1)));
	}

	public int getMaxParallelism() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.EXECUTION_MAX_PARALLELISM, Integer.toString(128)));
	}

	public boolean isChangelogMode() {
		return Objects.equals(
			properties.getOrDefault(PropertyStrings.EXECUTION_RESULT_MODE, PropertyStrings.EXECUTION_RESULT_MODE_VALUE_CHANGELOG),
			PropertyStrings.EXECUTION_RESULT_MODE_VALUE_CHANGELOG);
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
