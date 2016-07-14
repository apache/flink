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
import org.apache.flink.dropwizard.metrics.FlinkCounterWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.FlinkGaugeWrapper;
import org.apache.flink.dropwizard.metrics.FlinkHistogramWrapper;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.AbstractMetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Base class for {@link org.apache.flink.metrics.reporter.MetricReporter} that wraps a
 * Dropwizard {@link com.codahale.metrics.Reporter}.
 */
@PublicEvolving
public abstract class ScheduledDropwizardReporter implements MetricReporter, Scheduled, Reporter, CharacterFilter {

	protected final Logger log = LoggerFactory.getLogger(getClass());

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
	private final Map<Histogram, String> histograms = new HashMap<>();

	// ------------------------------------------------------------------------

	protected ScheduledDropwizardReporter() {
		this.registry = new MetricRegistry();
	}

	// ------------------------------------------------------------------------
	//  Getters
	// ------------------------------------------------------------------------

	// used for testing purposes
	Map<Counter, String> getCounters() {
		return counters;
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
		final String fullName = group.getMetricIdentifier(metricName, this);

		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, fullName);
				registry.register(fullName, new FlinkCounterWrapper((Counter) metric));
			}
			else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, fullName);
				registry.register(fullName, FlinkGaugeWrapper.fromGauge((Gauge<?>) metric));
			} else if (metric instanceof Histogram) {
				Histogram histogram = (Histogram) metric;
				histograms.put(histogram, fullName);

				if (histogram instanceof DropwizardHistogramWrapper) {
					registry.register(fullName, ((DropwizardHistogramWrapper) histogram).getDropwizarHistogram());
				} else {
					registry.register(fullName, new FlinkHistogramWrapper(histogram));
				}
			} else {
				log.warn("Cannot add metric of type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
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
			} else if (metric instanceof Histogram) {
				fullName = histograms.remove(metric);
			} else {
				fullName = null;
			}
			
			if (fullName != null) {
				registry.remove(fullName);
			}
		}
	}

	@Override
	public String filterCharacters(String metricName) {
		char[] chars = null;
		final int strLen = metricName.length();
		int pos = 0;

		for (int i = 0; i < strLen; i++) {
			final char c = metricName.charAt(i);
			switch (c) {
				case '.':
					if (chars == null) {
						chars = metricName.toCharArray();
					}
					chars[pos++] = '-';
					break;
				case '"':
					if (chars == null) {
						chars = metricName.toCharArray();
					}
					break;

				default:
					if (chars != null) {
						chars[pos] = c;
					}
					pos++;
			}
		}

		return chars == null ? metricName : new String(chars, 0, pos);
	}

	// ------------------------------------------------------------------------
	//  scheduled reporting
	// ------------------------------------------------------------------------

	@Override
	public void report() {
		// we do not need to lock here, because the dropwizard registry is
		// internally a concurrent map
		@SuppressWarnings("rawtypes")
		final SortedMap<String, com.codahale.metrics.Gauge> gauges = registry.getGauges();
		final SortedMap<String, com.codahale.metrics.Counter> counters = registry.getCounters();
		final SortedMap<String, com.codahale.metrics.Histogram> histograms = registry.getHistograms();
		final SortedMap<String, com.codahale.metrics.Meter> meters = registry.getMeters();
		final SortedMap<String, com.codahale.metrics.Timer> timers = registry.getTimers();

		this.reporter.report(gauges, counters, histograms, meters, timers);
	}

	public abstract ScheduledReporter getReporter(Configuration config);
}
