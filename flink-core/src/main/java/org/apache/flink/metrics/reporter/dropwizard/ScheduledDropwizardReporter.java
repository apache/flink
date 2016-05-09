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
package org.apache.flink.metrics.reporter.dropwizard;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.Timer;
import org.apache.flink.metrics.impl.CounterMetric;
import org.apache.flink.metrics.impl.GaugeMetric;
import org.apache.flink.metrics.impl.HistogramMetric;
import org.apache.flink.metrics.impl.MeterMetric;
import org.apache.flink.metrics.impl.TimerMetric;
import org.apache.flink.metrics.reporter.Listener;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Base class for {@link org.apache.flink.metrics.reporter.MetricReporter} that wraps a
 * Dropwizard {@link com.codahale.metrics.Reporter}.
 */
@PublicEvolving
public abstract class ScheduledDropwizardReporter implements MetricReporter, Listener, Scheduled {
	protected MetricRegistry registry;
	protected ScheduledReporter reporter;

	public static final String ARG_HOST = "host";
	public static final String ARG_PORT = "port";
	public static final String ARG_PREFIX = "prefix";
	public static final String ARG_CONVERSION_RATE = "rateConversion";
	public static final String ARG_CONVERSION_DURATION = "durationConversion";

	protected ScheduledDropwizardReporter() {
		this.registry = new MetricRegistry();
	}

	@Override
	public synchronized void notifyOfAddedMetric(Metric metric, String name) {
		registry.register(name, (com.codahale.metrics.Metric) metric);
	}

	@Override
	public synchronized void notifyOfRemovedMetric(Metric metric, String name) {
		registry.remove(name);
	}

	public abstract ScheduledReporter getReporter(Configuration config);

	@Override
	public void open(Configuration config) {
		this.reporter = getReporter(config);
	}

	@Override
	public void close() {
		this.reporter.stop();
	}

	@Override
	public String generateName(String name, List<String> scope) {
		StringBuilder sb = new StringBuilder();
		for (String s : scope) {
			sb.append(s);
			sb.append('.');
		}
		sb.append(name);
		return sb.toString();
	}

	@Override
	public synchronized void report(
		Map<String, Gauge> gauges,
		Map<String, Counter> counters,
		Map<String, Histogram> histograms,
		Map<String, Meter> meters,
		Map<String, Timer> timers) {

		TreeMap<String, com.codahale.metrics.Gauge> g = new TreeMap<>();
		for (Map.Entry<String, Gauge> metric : gauges.entrySet()) {
			g.put(metric.getKey(), (GaugeMetric) metric.getValue());
		}
		TreeMap<String, com.codahale.metrics.Counter> c = new TreeMap<>();
		for (Map.Entry<String, Counter> metric : counters.entrySet()) {
			c.put(metric.getKey(), (CounterMetric) metric.getValue());
		}
		TreeMap<String, com.codahale.metrics.Histogram> h = new TreeMap<>();
		for (Map.Entry<String, Histogram> metric : histograms.entrySet()) {
			h.put(metric.getKey(), (HistogramMetric) metric.getValue());
		}
		TreeMap<String, com.codahale.metrics.Meter> m = new TreeMap<>();
		for (Map.Entry<String, Meter> metric : meters.entrySet()) {
			m.put(metric.getKey(), (MeterMetric) metric.getValue());
		}
		TreeMap<String, com.codahale.metrics.Timer> t = new TreeMap<>();
		for (Map.Entry<String, Timer> metric : timers.entrySet()) {
			t.put(metric.getKey(), (TimerMetric) metric.getValue());
		}

		this.reporter.report(g, c, h, m, t);
	}
}
