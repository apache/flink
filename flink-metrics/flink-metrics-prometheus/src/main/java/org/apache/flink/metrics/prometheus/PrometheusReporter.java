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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;

import fi.iki.elonen.NanoHTTPD;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus.
 */
@PublicEvolving
public class PrometheusReporter implements MetricReporter {
	private static final Logger LOG = LoggerFactory.getLogger(PrometheusReporter.class);

	static final String ARG_PORT = "port";
	private static final int DEFAULT_PORT = 9249;

	private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
	private static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return replaceInvalidChars(input);
		}
	};

	private static final char SCOPE_SEPARATOR = '_';
	private static final String SCOPE_PREFIX = "flink" + SCOPE_SEPARATOR;

	private PrometheusEndpoint prometheusEndpoint;
	private final Map<String, Collector> collectorsByMetricName = new HashMap<>();

	@VisibleForTesting
	static String replaceInvalidChars(final String input) {
		// https://prometheus.io/docs/instrumenting/writing_exporters/
		// Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to an underscore.
		return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
	}

	@Override
	public void open(MetricConfig config) {
		int port = config.getInteger(ARG_PORT, DEFAULT_PORT);
		LOG.info("Using port {}.", port);
		prometheusEndpoint = new PrometheusEndpoint(port);
		try {
			prometheusEndpoint.start(NanoHTTPD.SOCKET_READ_TIMEOUT, true);
		} catch (IOException e) {
			final String msg = "Could not start PrometheusEndpoint on port " + port;
			LOG.warn(msg, e);
			throw new RuntimeException(msg, e);
		}
	}

	@Override
	public void close() {
		prometheusEndpoint.stop();
		CollectorRegistry.defaultRegistry.clear();
	}

	@Override
	public void notifyOfAddedMetric(final Metric metric, final String metricName, final MetricGroup group) {
		final String scope = SCOPE_PREFIX + getLogicalScope(group);

		List<String> dimensionKeys = new LinkedList<>();
		List<String> dimensionValues = new LinkedList<>();
		for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
			final String key = dimension.getKey();
			dimensionKeys.add(CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1)));
			dimensionValues.add(CHARACTER_FILTER.filterCharacters(dimension.getValue()));
		}

		final String validMetricName = scope + SCOPE_SEPARATOR + CHARACTER_FILTER.filterCharacters(metricName);
		final String metricIdentifier = group.getMetricIdentifier(metricName);
		final Collector collector;
		if (metric instanceof Gauge) {
			collector = createGauge((Gauge) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else if (metric instanceof Counter) {
			collector = createGauge((Counter) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else if (metric instanceof Meter) {
			collector = createGauge((Meter) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else if (metric instanceof Histogram) {
			collector = createSummary((Histogram) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else {
			LOG.warn("Cannot add unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
				metric.getClass().getName());
			return;
		}
		collector.register();
		collectorsByMetricName.put(metricName, collector);
	}

	@Override
	public void notifyOfRemovedMetric(final Metric metric, final String metricName, final MetricGroup group) {
		CollectorRegistry.defaultRegistry.unregister(collectorsByMetricName.get(metricName));
		collectorsByMetricName.remove(metricName);
	}

	@SuppressWarnings("unchecked")
	private static String getLogicalScope(MetricGroup group) {
		return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
	}

	private Collector createGauge(final Gauge gauge, final String name, final String identifier, final List<String> labelNames, final List<String> labelValues) {
		return newGauge(name, identifier, labelNames, labelValues, new io.prometheus.client.Gauge.Child() {
			@Override
			public double get() {
				final Object value = gauge.getValue();
				if (value instanceof Double) {
					return (double) value;
				}
				if (value instanceof Number) {
					return ((Number) value).doubleValue();
				} else if (value instanceof Boolean) {
					return ((Boolean) value) ? 1 : 0;
				} else {
					LOG.debug("Invalid type for Gauge {}: {}, only number types and booleans are supported by this reporter.",
						gauge, value.getClass().getName());
					return 0;
				}
			}
		});
	}

	private static Collector createGauge(final Counter counter, final String name, final String identifier, final List<String> labelNames, final List<String> labelValues) {
		return newGauge(name, identifier, labelNames, labelValues, new io.prometheus.client.Gauge.Child() {
			@Override
			public double get() {
				return (double) counter.getCount();
			}
		});
	}

	private Collector createGauge(final Meter meter, final String name, final String identifier, final List<String> labelNames, final List<String> labelValues) {
		return newGauge(name, identifier, labelNames, labelValues, new io.prometheus.client.Gauge.Child() {
			@Override
			public double get() {
				return meter.getRate();
			}
		});
	}

	private static Collector newGauge(String name, String identifier, List<String> labelNames, List<String> labelValues, io.prometheus.client.Gauge.Child child) {
		return io.prometheus.client.Gauge
			.build()
			.name(name)
			.help(identifier)
			.labelNames(toArray(labelNames))
			.create()
			.setChild(child, toArray(labelValues));
	}

	private static HistogramSummaryProxy createSummary(final Histogram histogram, final String name, final String identifier, final List<String> dimensionKeys, final List<String> dimensionValues) {
		return new HistogramSummaryProxy(histogram, name, identifier, dimensionKeys, dimensionValues);
	}

	static class PrometheusEndpoint extends NanoHTTPD {
		static final String MIME_TYPE = "plain/text";

		PrometheusEndpoint(int port) {
			super(port);
		}

		@Override
		public Response serve(IHTTPSession session) {
			if (session.getUri().equals("/metrics")) {
				StringWriter writer = new StringWriter();
				try {
					TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
				} catch (IOException e) {
					return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_TYPE, "Unable to output metrics");
				}
				return newFixedLengthResponse(Response.Status.OK, TextFormat.CONTENT_TYPE_004, writer.toString());
			} else {
				return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_TYPE, "Not found");
			}
		}
	}

	private static class HistogramSummaryProxy extends Collector {
		private static final List<Double> QUANTILES = Arrays.asList(.5, .75, .95, .98, .99, .999);

		private final Histogram histogram;
		private final String metricName;
		private final String metricIdentifier;
		private final List<String> labelNamesWithQuantile;
		private final List<String> labelValues;

		HistogramSummaryProxy(final Histogram histogram, final String metricName, final String metricIdentifier, final List<String> labelNames, final List<String> labelValues) {
			this.histogram = histogram;
			this.metricName = metricName;
			this.metricIdentifier = metricIdentifier;
			this.labelNamesWithQuantile = addToList(labelNames, "quantile");
			this.labelValues = labelValues;
		}

		@Override
		public List<MetricFamilySamples> collect() {
			// We cannot use SummaryMetricFamily because it is impossible to get a sum of all values (at least for Dropwizard histograms,
			// whose snapshot's values array only holds a sample of recent values).

			final HistogramStatistics statistics = histogram.getStatistics();

			List<MetricFamilySamples.Sample> samples = new LinkedList<>();
			samples.add(new MetricFamilySamples.Sample(metricName + "_count",
				labelNamesWithQuantile.subList(0, labelNamesWithQuantile.size() - 1), labelValues, histogram.getCount()));
			for (final Double quantile : QUANTILES) {
				samples.add(new MetricFamilySamples.Sample(metricName, labelNamesWithQuantile,
					addToList(labelValues, quantile.toString()),
					statistics.getQuantile(quantile)));
			}
			return Collections.singletonList(new MetricFamilySamples(metricName, Type.SUMMARY, metricIdentifier, samples));
		}
	}

	private static List<String> addToList(List<String> list, String element) {
		final List<String> result = new ArrayList<>(list);
		result.add(element);
		return result;
	}

	private static String[] toArray(List<String> labelNames) {
		return labelNames.toArray(new String[labelNames.size()]);
	}
}
