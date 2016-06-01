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

package org.apache.flink.dropwizard;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.ScheduledReporter;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.CounterWrapper;
import org.apache.flink.dropwizard.metrics.GaugeWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.AbstractMetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for {@link org.apache.flink.metrics.reporter.MetricReporter} that wraps a
 * Dropwizard {@link com.codahale.metrics.Reporter}.
 */
@PublicEvolving
public abstract class ScheduledDropwizardReporter implements MetricReporter, Scheduled, Reporter {

	public static final String ARG_HOST = "host";
	public static final String ARG_PORT = "port";
	public static final String ARG_PREFIX = "prefix";
	public static final String ARG_CONVERSION_RATE = "rateConversion";
	public static final String ARG_CONVERSION_DURATION = "durationConversion";

	// ------------------------------------------------------------------------

	protected final MetricRegistry registry;

	protected ScheduledReporter reporter;

	private final Map<Gauge<?>, String> gauges = new HashMap<>();
	private final Map<Counter, String> counters = new HashMap<>();

	// ------------------------------------------------------------------------

	protected ScheduledDropwizardReporter() {
		this.registry = new MetricRegistry();
	}

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration config) {
		this.reporter = getReporter(config);
	}

	@Override
	public void close() {
		this.reporter.stop();
	}

	// ------------------------------------------------------------------------
	//  adding / removing metrics
	// ------------------------------------------------------------------------

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, AbstractMetricGroup group) {
		final String fullName = group.getScopeString() + '.' + metricName;

		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, fullName);
				registry.register(fullName, new CounterWrapper((Counter) metric));
			}
			else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, fullName);
				registry.register(fullName, GaugeWrapper.fromGauge((Gauge<?>) metric));
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, AbstractMetricGroup group) {
		synchronized (this) {
			String fullName;
			
			if (metric instanceof Counter) {
				fullName = counters.remove(metric);
			} else if (metric instanceof Gauge) {
				fullName = gauges.remove(metric);
			} else {
				fullName = null;
			}
			
			if (fullName != null) {
				registry.remove(fullName);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  scheduled reporting
	// ------------------------------------------------------------------------

	@Override
	public void report() {
		synchronized (this) {
			this.reporter.report(
				this.registry.getGauges(),
				this.registry.getCounters(),
				this.registry.getHistograms(),
				this.registry.getMeters(),
				this.registry.getTimers());
		}
	}

	public abstract ScheduledReporter getReporter(Configuration config);
}
