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
import com.codahale.metrics.ScheduledReporter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.CounterWrapper;
import org.apache.flink.dropwizard.metrics.GaugeWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.List;

/**
 * Base class for {@link org.apache.flink.metrics.reporter.MetricReporter} that wraps a
 * Dropwizard {@link com.codahale.metrics.Reporter}.
 */
@PublicEvolving
public abstract class ScheduledDropwizardReporter implements MetricReporter, Scheduled {
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
		if (metric instanceof Counter) {
			registry.register(name, new CounterWrapper((Counter) metric));
		} else if (metric instanceof Gauge) {
			registry.register(name, new GaugeWrapper((Gauge<?>) metric));
		}
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
	public synchronized void report() {
		this.reporter.report(
			this.registry.getGauges(),
			this.registry.getCounters(),
			this.registry.getHistograms(),
			this.registry.getMeters(),
			this.registry.getTimers());
	}
}
