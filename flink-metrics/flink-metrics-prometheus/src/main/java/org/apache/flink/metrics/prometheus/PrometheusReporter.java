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

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import fi.iki.elonen.NanoHTTPD;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterables.toArray;

@PublicEvolving
public class PrometheusReporter implements MetricReporter {
	private static final Logger log = LoggerFactory.getLogger(PrometheusReporter.class);

	static final         String ARG_PORT     = "port";
	private static final int    DEFAULT_PORT = 9249;

	private static final Pattern         UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
	private static final CharacterFilter CHARACTER_FILTER       = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return replaceInvalidChars(input);
		}
	};

	private PrometheusEndpoint prometheusEndpoint;
	private Map<String, Collector> collectorsByMetricName = new HashMap<>();

	@VisibleForTesting
	static String replaceInvalidChars(final String input) {
		// https://prometheus.io/docs/instrumenting/writing_exporters/
		// Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to an underscore.
		return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
	}

	@Override
	public void open(MetricConfig config) {
		int port = config.getInteger(ARG_PORT, DEFAULT_PORT);
		log.info("Using port {}.", port);
		prometheusEndpoint = new PrometheusEndpoint(port);
		try {
			prometheusEndpoint.start(NanoHTTPD.SOCKET_READ_TIMEOUT, true);
		} catch (IOException e) {
			log.error("Could not start PrometheusEndpoint on port " + port, e);
		}
	}

	@Override
	public void close() {
		prometheusEndpoint.stop();
		CollectorRegistry.defaultRegistry.clear();
	}

	@Override
	public void notifyOfAddedMetric(final Metric metric, final String metricName, final MetricGroup group) {
		List<String> dimensionKeys = new LinkedList<>();
		List<String> dimensionValues = new LinkedList<>();
		for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
			dimensionKeys.add(CHARACTER_FILTER.filterCharacters(CharMatcher.anyOf("<>").trimFrom(dimension.getKey())));
			dimensionValues.add(CHARACTER_FILTER.filterCharacters(dimension.getValue()));
		}

		final String validMetricName = CHARACTER_FILTER.filterCharacters(metricName);
		final String metricIdentifier = group.getMetricIdentifier(metricName);
		final Collector collector;
		if (metric instanceof Gauge) {
			collector = createGauge((Gauge) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else if (metric instanceof Counter) {
			collector = createGauge((Counter) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else if (metric instanceof Histogram) {
			collector = createSummary((Histogram) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else if (metric instanceof Meter) {
			collector = createCounter((Meter) metric, validMetricName, metricIdentifier, dimensionKeys, dimensionValues);
		} else {
			log.error("Cannot add unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
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

	private Collector createGauge(final Gauge gauge, final String name, final String identifier, final List<String> labelNames, final List<String> labelValues) {
		return io.prometheus.client.Gauge
			.build()
			.name(name)
			.help(identifier)
			.labelNames(toArray(labelNames, String.class))
			.create()
			.setChild(new io.prometheus.client.Gauge.Child() {
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
						log.debug("Invalid type for Gauge {}: {}, only number types and booleans are supported by this reporter.",
							gauge, value.getClass().getName());
						return 0;
					}
				}
			}, toArray(labelValues, String.class));
	}

	private static Collector createGauge(final Counter counter, final String name, final String identifier, final List<String> labelNames, final List<String> labelValues) {
		return io.prometheus.client.Gauge
			.build()
			.name(name)
			.help(identifier)
			.labelNames(toArray(labelNames, String.class))
			.create()
			.setChild(new io.prometheus.client.Gauge.Child() {
				@Override
				public double get() {
					return (double) counter.getCount();
				}
			}, toArray(labelValues, String.class));
	}

	private Collector createCounter(final Meter meter, final String name, final String identifier, final List<String> labelNames, final List<String> labelValues) {
		return io.prometheus.client.Counter
			.build()
			.name(name + "_total")
			.help(identifier)
			.labelNames(toArray(labelNames, String.class))
			.create()
			.setChild(new io.prometheus.client.Counter.Child() {
				@Override
				public double get() {
					return (double) meter.getCount();
				}
			}, toArray(labelValues, String.class));
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
		private static final ImmutableList<Double> QUANTILES = ImmutableList.of(.5, .75, .95, .98, .99, .999);

		private final Histogram    histogram;
		private final String       metricName;
		private final String       metricIdentifier;
		private final List<String> labelNamesWithQuantile;
		private final List<String> labelValues;

		HistogramSummaryProxy(final Histogram histogram, final String metricName, final String metricIdentifier, final List<String> labelNames, final List<String> labelValues) {
			this.histogram = histogram;
			this.metricName = metricName;
			this.metricIdentifier = metricIdentifier;
			labelNamesWithQuantile = ImmutableList.<String>builder().addAll(labelNames).add("quantile").build();
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
					ImmutableList.<String>builder().addAll(labelValues).add(quantile.toString()).build(),
					statistics.getQuantile(quantile)));
			}
			return Collections.singletonList(new MetricFamilySamples(metricName, Type.SUMMARY, metricIdentifier, samples));
		}

	}
}
