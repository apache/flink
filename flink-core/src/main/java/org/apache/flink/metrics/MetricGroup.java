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

import org.apache.flink.annotation.PublicEvolving;

/**
 * A MetricGroup is a named container for {@link org.apache.flink.metrics.Metric}s and
 * {@link org.apache.flink.metrics.MetricGroup}s.
 * <p>
 * Instances of this class can be used to register new metrics with Flink and to create a nested hierarchy based on the
 * group names.
 * <p>
 * A MetricGroup is uniquely identified by it's place in the hierarchy and name.
 */
@PublicEvolving
public interface MetricGroup {

	/**
	 * Recursively unregisters all {@link org.apache.flink.metrics.Metric}s contained in this
	 * {@link org.apache.flink.metrics.MetricGroup}
	 */
	void close();

	// -----------------------------------------------------------------------------------------------------------------
	// Metrics
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Creates and registers a new {@link org.apache.flink.metrics.Counter} with Flink.
	 *
	 * @param name name of the counter
	 * @return the registered counter
	 */
	Counter counter(int name);

	/**
	 * Creates and registers a new {@link org.apache.flink.metrics.Counter} with Flink.
	 *
	 * @param name name of the counter
	 * @return the registered counter
	 */
	Counter counter(String name);

	/**
	 * Registers a new {@link org.apache.flink.metrics.Gauge} with Flink.
	 *
	 * @param name  name of the gauge
	 * @param gauge gauge to register
	 * @param <T>   return type of the gauge
	 * @return the registered gauge
	 */
	<T> Gauge<T> gauge(int name, Gauge<T> gauge);

	/**
	 * Registers a new {@link org.apache.flink.metrics.Gauge} with Flink.
	 *
	 * @param name  name of the gauge
	 * @param gauge gauge to register
	 * @param <T>   return type of the gauge
	 * @return the registered gauge
	 */
	<T> Gauge<T> gauge(String name, Gauge<T> gauge);

	// -----------------------------------------------------------------------------------------------------------------
	// Groups
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Creates a new {@link org.apache.flink.metrics.MetricGroup} and adds it to this groups sub-groups.
	 *
	 * @param name name of the group
	 * @return the created group
	 */
	MetricGroup addGroup(int name);

	/**
	 * Creates a new {@link org.apache.flink.metrics.MetricGroup} and adds it to this groups sub-groups.
	 *
	 * @param name name of the group
	 * @return the created group
	 */
	MetricGroup addGroup(String name);
}
