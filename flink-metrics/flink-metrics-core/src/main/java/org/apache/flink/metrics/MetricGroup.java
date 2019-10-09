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
 * A MetricGroup is a named container for {@link Metric Metrics} and further metric subgroups.
 *
 * <p>Instances of this class can be used to register new metrics with Flink and to create a nested
 * hierarchy based on the group names.
 *
 * <p>A MetricGroup is uniquely identified by it's place in the hierarchy and name.
 */
public interface MetricGroup {

	// ------------------------------------------------------------------------
	//  Metrics
	// ------------------------------------------------------------------------

	/**
	 * Creates and registers a new {@link org.apache.flink.metrics.Counter} with Flink.
	 *
	 * @param name name of the counter
	 * @return the created counter
	 */
	Counter counter(int name);

	/**
	 * Creates and registers a new {@link org.apache.flink.metrics.Counter} with Flink.
	 *
	 * @param name name of the counter
	 * @return the created counter
	 */
	Counter counter(String name);

	/**
	 * Registers a {@link org.apache.flink.metrics.Counter} with Flink.
	 *
	 * @param name    name of the counter
	 * @param counter counter to register
	 * @param <C>     counter type
	 * @return the given counter
	 */
	<C extends Counter> C counter(int name, C counter);

	/**
	 * Registers a {@link org.apache.flink.metrics.Counter} with Flink.
	 *
	 * @param name    name of the counter
	 * @param counter counter to register
	 * @param <C>     counter type
	 * @return the given counter
	 */
	<C extends Counter> C counter(String name, C counter);

	/**
	 * Registers a new {@link org.apache.flink.metrics.Gauge} with Flink.
	 *
	 * @param name  name of the gauge
	 * @param gauge gauge to register
	 * @param <T>   return type of the gauge
	 * @return the given gauge
	 */
	<T, G extends Gauge<T>> G gauge(int name, G gauge);

	/**
	 * Registers a new {@link org.apache.flink.metrics.Gauge} with Flink.
	 *
	 * @param name  name of the gauge
	 * @param gauge gauge to register
	 * @param <T>   return type of the gauge
	 * @return the given gauge
	 */
	<T, G extends Gauge<T>> G gauge(String name, G gauge);

	/**
	 * Registers a new {@link Histogram} with Flink.
	 *
	 * @param name name of the histogram
	 * @param histogram histogram to register
	 * @param <H> histogram type
	 * @return the registered histogram
	 */
	<H extends Histogram> H histogram(String name, H histogram);

	/**
	 * Registers a new {@link Histogram} with Flink.
	 *
	 * @param name name of the histogram
	 * @param histogram histogram to register
	 * @param <H> histogram type
	 * @return the registered histogram
	 */
	<H extends Histogram> H histogram(int name, H histogram);

	/**
	 * Registers a new {@link Meter} with Flink.
	 *
	 * @param name name of the meter
	 * @param meter meter to register
	 * @param <M> meter type
	 * @return the registered meter
	 */
	<M extends Meter> M meter(String name, M meter);

	/**
	 * Registers a new {@link Meter} with Flink.
	 *
	 * @param name name of the meter
	 * @param meter meter to register
	 * @param <M> meter type
	 * @return the registered meter
	 */
	<M extends Meter> M meter(int name, M meter);

	// ------------------------------------------------------------------------
	// Groups
	// ------------------------------------------------------------------------

	/**
	 * Creates a new MetricGroup and adds it to this groups sub-groups.
	 *
	 * @param name name of the group
	 * @return the created group
	 */
	MetricGroup addGroup(int name);

	/**
	 * Creates a new MetricGroup and adds it to this groups sub-groups.
	 *
	 * @param name name of the group
	 * @return the created group
	 */
	MetricGroup addGroup(String name);

	/**
	 * Creates a new key-value MetricGroup pair. The key group is added to this groups sub-groups, while the value group
	 * is added to the key group's sub-groups. This method returns the value group.
	 *
	 * <p>The only difference between calling this method and {@code group.addGroup(key).addGroup(value)} is that
	 * {@link #getAllVariables()} of the value group return an additional {@code "<key>"="value"} pair.
	 *
	 * @param key name of the first group
	 * @param value name of the second group
	 * @return the second created group
	 */
	MetricGroup addGroup(String key, String value);

	// ------------------------------------------------------------------------
	// Scope
	// ------------------------------------------------------------------------

	/** @deprecated Will be removed in a future version */
	@Deprecated
	String[] getScopeComponents();

	/** @deprecated use {@link MetricScope#getAllVariables()} instead */
	@Deprecated
	Map<String, String> getAllVariables();

	/** @deprecated use {@link MetricScope#getMetricIdentifier(String)} instead */
	@Deprecated
	String getMetricIdentifier(String metricName);

	/** @deprecated use {@link MetricScope#getMetricIdentifier(String, CharacterFilter)} instead */
	@Deprecated
	String getMetricIdentifier(String metricName, CharacterFilter filter);

	/**
	 * Returns the scope of this metric group.
	 *
	 * @return scope of this metric group
	 */
	MetricScope getScope();
}
