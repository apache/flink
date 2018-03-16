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

package org.apache.flink.metrics.groups;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

import java.util.Collections;
import java.util.Map;

/**
 * A special {@link MetricGroup} that does not register any metrics at the metrics registry
 * and any reporters.
 */
public class UnregisteredMetricsGroup implements MetricGroup {

	@Override
	public Counter counter(int name) {
		return new SimpleCounter();
	}

	@Override
	public Counter counter(String name) {
		return new SimpleCounter();
	}

	@Override
	public <C extends Counter> C counter(int name, C counter) {
		return counter;
	}

	@Override
	public <C extends Counter> C counter(String name, C counter) {
		return counter;
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
		return gauge;
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
		return gauge;
	}

	@Override
	public <H extends Histogram> H histogram(int name, H histogram) {
		return histogram;
	}

	@Override
	public <M extends Meter> M meter(String name, M meter) {
		return meter;
	}

	@Override
	public <M extends Meter> M meter(int name, M meter) {
		return meter;
	}

	@Override
	public <H extends Histogram> H histogram(String name, H histogram) {
		return histogram;
	}

	@Override
	public MetricGroup addGroup(int name) {
		return addGroup(String.valueOf(name));
	}

	@Override
	public MetricGroup addGroup(String name) {
		return new UnregisteredMetricsGroup();
	}

	@Override
	public MetricGroup addGroup(String key, String value) {
		return new UnregisteredMetricsGroup();
	}

	@Override
	public String[] getScopeComponents() {
		return new String[0];
	}

	@Override
	public Map<String, String> getAllVariables() {
		return Collections.emptyMap();
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return metricName;
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return metricName;
	}
}
