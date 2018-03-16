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

package org.apache.flink.metrics.slf4j;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via SLF4J {@link Logger}.
 */
public class Slf4jReporter extends AbstractReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(Slf4jReporter.class);
	private static final String lineSeparator = System.lineSeparator();

	@VisibleForTesting
	Map<Gauge<?>, String> getGauges() {
		return gauges;
	}

	@VisibleForTesting
	Map<Counter, String> getCounters() {
		return counters;
	}

	@VisibleForTesting
	Map<Histogram, String> getHistograms() {
		return histograms;
	}

	@VisibleForTesting
	Map<Meter, String> getMeters() {
		return meters;
	}

	@Override
	public void open(MetricConfig metricConfig) {
	}

	@Override
	public void close() {
	}

	@Override
	public void report() {
		StringBuilder builder = new StringBuilder();
		builder
			.append(lineSeparator)
			.append("=========================== Starting metrics report ===========================")
			.append(lineSeparator);

		builder
			.append(lineSeparator)
			.append("-- Counters -------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Counter, String> metric : counters.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getCount())
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("-- Gauges ---------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getValue())
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("-- Meters ---------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Meter, String> metric : meters.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getRate())
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("-- Histograms -----------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
			HistogramStatistics stats = metric.getKey().getStatistics();
			builder
				.append(metric.getValue()).append(": count=").append(stats.size())
				.append(", min=").append(stats.getMin())
				.append(", max=").append(stats.getMax())
				.append(", mean=").append(stats.getMean())
				.append(", stddev=").append(stats.getStdDev())
				.append(", p50=").append(stats.getQuantile(0.50))
				.append(", p75=").append(stats.getQuantile(0.75))
				.append(", p95=").append(stats.getQuantile(0.95))
				.append(", p98=").append(stats.getQuantile(0.98))
				.append(", p99=").append(stats.getQuantile(0.99))
				.append(", p999=").append(stats.getQuantile(0.999))
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("=========================== Finished metrics report ===========================")
			.append(lineSeparator);
		LOG.info(builder.toString());
	}

	@Override
	public String filterCharacters(String input) {
		return input;
	}
}
