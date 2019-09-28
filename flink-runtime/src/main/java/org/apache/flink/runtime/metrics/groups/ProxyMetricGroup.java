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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Metric group which forwards all registration calls to its parent metric group.
 *
 * @param <P> Type of the parent metric group
 */
public class ProxyMetricGroup<P extends MetricGroup> implements MetricGroup {

	protected final P parentMetricGroup;

	public ProxyMetricGroup(P parentMetricGroup) {
		this.parentMetricGroup = checkNotNull(parentMetricGroup);
	}

	@Override
	public final Counter counter(int name) {
		return parentMetricGroup.counter(name);
	}

	@Override
	public final Counter counter(String name) {
		return parentMetricGroup.counter(name);
	}

	@Override
	public final <C extends Counter> C counter(int name, C counter) {
		return parentMetricGroup.counter(name, counter);
	}

	@Override
	public final <C extends Counter> C counter(String name, C counter) {
		return parentMetricGroup.counter(name, counter);
	}

	@Override
	public final <T, G extends Gauge<T>> G gauge(int name, G gauge) {
		return parentMetricGroup.gauge(name, gauge);
	}

	@Override
	public final <T, G extends Gauge<T>> G gauge(String name, G gauge) {
		return parentMetricGroup.gauge(name, gauge);
	}

	@Override
	public final <H extends Histogram> H histogram(String name, H histogram) {
		return parentMetricGroup.histogram(name, histogram);
	}

	@Override
	public final <H extends Histogram> H histogram(int name, H histogram) {
		return parentMetricGroup.histogram(name, histogram);
	}

	@Override
	public <M extends Meter> M meter(String name, M meter) {
		return parentMetricGroup.meter(name, meter);
	}

	@Override
	public <M extends Meter> M meter(int name, M meter) {
		return parentMetricGroup.meter(name, meter);
	}

	@Override
	public final MetricGroup addGroup(int name) {
		return parentMetricGroup.addGroup(name);
	}

	@Override
	public final MetricGroup addGroup(String name) {
		return parentMetricGroup.addGroup(name);
	}

	@Override
	public final MetricGroup addGroup(String key, String value) {
		return parentMetricGroup.addGroup(key, value);
	}

	@Override
	public String[] getScopeComponents() {
		return parentMetricGroup.getScopeComponents();
	}

	@Override
	public Map<String, String> getAllVariables() {
		return parentMetricGroup.getAllVariables();
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return parentMetricGroup.getMetricIdentifier(metricName);
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return parentMetricGroup.getMetricIdentifier(metricName, filter);
	}
}
