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

package org.apache.flink.metrics;

import java.util.Map;

/**
 * Represents the scope of group, i.e., information used to identify metrics.
 */
public interface MetricScope {

	/**
	 * Returns a map of all variables and their associated value, for example
	 * {@code {"<host>"="host-7", "<tm_id>"="taskmanager-2"}}.
	 *
	 * @return map of all variables and their associated value
	 */
	Map<String, String> getAllVariables();

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
	 *
	 * @param metricName metric name
	 * @return fully qualified metric name
	 */
	String getMetricIdentifier(String metricName);

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
	 *
	 * @param metricName metric name
	 * @param filter character filter which is applied to the scope components if not null.
	 * @return fully qualified metric name
	 */
	String getMetricIdentifier(String metricName, CharacterFilter filter);
}
