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
 * When need know position number for reporter in  metric group
 *
 * @param <A> reference to {@link AbstractMetricGroup AbstractMetricGroup}
 */
public class FrontMetricGroup<A extends AbstractMetricGroup<?>> implements MetricGroup {

	protected A reference;
	protected int index;

	public FrontMetricGroup(int index) {
		this.index = index;
	}

	public void setReference(A group) {
		this.reference = checkNotNull(group);
	}

	@Override
	public Counter counter(int name) {
		return reference.counter(name);
	}

	@Override
	public Counter counter(String name) {
		return reference.counter(name);
	}

	@Override
	public <C extends Counter> C counter(int name, C counter) {
		return reference.counter(name, counter);
	}

	@Override
	public <C extends Counter> C counter(String name, C counter) {
		return reference.counter(name, counter);
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
		return reference.gauge(name, gauge);
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
		return reference.gauge(name, gauge);
	}

	@Override
	public <H extends Histogram> H histogram(String name, H histogram) {
		return reference.histogram(name, histogram);
	}

	@Override
	public <H extends Histogram> H histogram(int name, H histogram) {
		return reference.histogram(name, histogram);
	}

	@Override
	public <M extends Meter> M meter(String name, M meter) {
		return reference.meter(name, meter);
	}

	@Override
	public <M extends Meter> M meter(int name, M meter) {
		return reference.meter(name, meter);
	}

	@Override
	public MetricGroup addGroup(int name) {
		return reference.addGroup(name);
	}

	@Override
	public MetricGroup addGroup(String name) {
		return reference.addGroup(name);
	}

	@Override
	public String[] getScopeComponents() {
		return reference.getScopeComponents();
	}

	@Override
	public Map<String, String> getAllVariables() {
		return reference.getAllVariables();
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return reference.getMetricIdentifier(metricName, null, this.index);
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return reference.getMetricIdentifier(metricName, filter, this.index);
	}
}
