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
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * base prometheus reporter for prometheus metrics.
 */
@PublicEvolving
public abstract class AbstractPrometheusReporter implements MetricReporter {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
	private static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return replaceInvalidChars(input);
		}
	};

	private static final char SCOPE_SEPARATOR = '_';
	private static final String SCOPE_PREFIX = "flink" + SCOPE_SEPARATOR;

	private final Map<String, AbstractMap.SimpleImmutableEntry<Collector, Integer>> collectorsWithCountByMetricName = new HashMap<>();

	@VisibleForTesting
	static String replaceInvalidChars(final String input) {
		// https://prometheus.io/docs/instrumenting/writing_exporters/
		// Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to an underscore.
		return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
	}

	@Override
	public void close() {
		CollectorRegistry.defaultRegistry.clear();
	}

	@Override
	public void notifyOfAddedMetric(final Metric metric, final String metricName, final MetricGroup group) {

		List<String> dimensionKeys = new LinkedList<>();
		List<String> dimensionValues = new LinkedList<>();
		for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
			final String key = dimension.getKey();
			dimensionKeys.add(CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1)));
			dimensionValues.add(CHARACTER_FILTER.filterCharacters(dimension.getValue()));
		}

		final String scopedMetricName = getScopedName(metricName, group);
		final String helpString = metricName + " (scope: " + getLogicalScope(group) + ")";

		final Collector collector;
		Integer count = 0;

		synchronized (this) {
			if (collectorsWithCountByMetricName.containsKey(scopedMetricName)) {
				final AbstractMap.SimpleImmutableEntry<Collector, Integer> collectorWithCount = collectorsWithCountByMetricName.get(scopedMetricName);
				collector = collectorWithCount.getKey();
				count = collectorWithCount.getValue();
			} else {
				collector = createCollector(metric, dimensionKeys, dimensionValues, scopedMetricName, helpString);
				try {
					collector.register();
				} catch (Exception e) {
					log.warn("There was a problem registering metric {}.", metricName, e);
				}
			}
			addMetric(metric, dimensionValues, collector);
			collectorsWithCountByMetricName.put(scopedMetricName, new AbstractMap.SimpleImmutableEntry<>(collector, count + 1));
		}
	}

	private static String getScopedName(String metricName, MetricGroup group) {
		return SCOPE_PREFIX + getLogicalScope(group) + SCOPE_SEPARATOR + CHARACTER_FILTER.filterCharacters(metricName);
	}

	private Collector createCollector(Metric metric, List<String> dimensionKeys, List<String> dimensionValues, String scopedMetricName, String helpString) {
		Collector collector;
		if (metric instanceof Gauge || metric instanceof Counter || metric instanceof Meter) {
			collector = io.prometheus.client.Gauge
				.build()
				.name(scopedMetricName)
				.help(helpString)
				.labelNames(toArray(dimensionKeys))
				.create();
		} else if (metric instanceof Histogram) {
			collector = new HistogramSummaryProxy((Histogram) metric, scopedMetricName, helpString, dimensionKeys, dimensionValues);
		} else {
			log.warn("Cannot create collector for unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
				metric.getClass().getName());
			collector = null;
		}
		return collector;
	}

	private void addMetric(Metric metric, List<String> dimensionValues, Collector collector) {
		if (metric instanceof Gauge) {
			((io.prometheus.client.Gauge) collector).setChild(gaugeFrom((Gauge) metric), toArray(dimensionValues));
		} else if (metric instanceof Counter) {
			((io.prometheus.client.Gauge) collector).setChild(gaugeFrom((Counter) metric), toArray(dimensionValues));
		} else if (metric instanceof Meter) {
			((io.prometheus.client.Gauge) collector).setChild(gaugeFrom((Meter) metric), toArray(dimensionValues));
		} else if (metric instanceof Histogram) {
			((HistogramSummaryProxy) collector).addChild((Histogram) metric, dimensionValues);
		} else {
			log.warn("Cannot add unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
				metric.getClass().getName());
		}
	}

	private void removeMetric(Metric metric, List<String> dimensionValues, Collector collector) {
		if (metric instanceof Gauge) {
			((io.prometheus.client.Gauge) collector).remove(toArray(dimensionValues));
		} else if (metric instanceof Counter) {
			((io.prometheus.client.Gauge) collector).remove(toArray(dimensionValues));
		} else if (metric instanceof Meter) {
			((io.prometheus.client.Gauge) collector).remove(toArray(dimensionValues));
		} else if (metric instanceof Histogram) {
			((HistogramSummaryProxy) collector).remove(dimensionValues);
		} else {
			log.warn("Cannot remove unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
				metric.getClass().getName());
		}
	}

	@Override
	public void notifyOfRemovedMetric(final Metric metric, final String metricName, final MetricGroup group) {

		List<String> dimensionValues = new LinkedList<>();
		for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
			dimensionValues.add(CHARACTER_FILTER.filterCharacters(dimension.getValue()));
		}

		final String scopedMetricName = getScopedName(metricName, group);
		synchronized (this) {
			final AbstractMap.SimpleImmutableEntry<Collector, Integer> collectorWithCount = collectorsWithCountByMetricName.get(scopedMetricName);
			final Integer count = collectorWithCount.getValue();
			final Collector collector = collectorWithCount.getKey();

			removeMetric(metric, dimensionValues, collector);

			if (count == 1) {
				try {
					CollectorRegistry.defaultRegistry.unregister(collector);
				} catch (Exception e) {
					log.warn("There was a problem unregistering metric {}.", scopedMetricName, e);
				}
				collectorsWithCountByMetricName.remove(scopedMetricName);
			} else {
				collectorsWithCountByMetricName.put(scopedMetricName, new AbstractMap.SimpleImmutableEntry<>(collector, count - 1));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static String getLogicalScope(MetricGroup group) {
		return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
	}

	@VisibleForTesting
	io.prometheus.client.Gauge.Child gaugeFrom(Gauge gauge) {
		return new io.prometheus.client.Gauge.Child() {
			@Override
			public double get() {
				final Object value = gauge.getValue();
				if (value == null) {
					log.debug("Gauge {} is null-valued, defaulting to 0.", gauge);
					return 0;
				}
				if (value instanceof Double) {
					return (double) value;
				}
				if (value instanceof Number) {
					return ((Number) value).doubleValue();
				}
				if (value instanceof Boolean) {
					return ((Boolean) value) ? 1 : 0;
				}
				log.debug("Invalid type for Gauge {}: {}, only number types and booleans are supported by this reporter.",
					gauge, value.getClass().getName());
				return 0;
			}
		};
	}

	private static io.prometheus.client.Gauge.Child gaugeFrom(Counter counter) {
		return new io.prometheus.client.Gauge.Child() {
			@Override
			public double get() {
				return (double) counter.getCount();
			}
		};
	}

	private static io.prometheus.client.Gauge.Child gaugeFrom(Meter meter) {
		return new io.prometheus.client.Gauge.Child() {
			@Override
			public double get() {
				return meter.getRate();
			}
		};
	}

	@VisibleForTesting
	static class HistogramSummaryProxy extends Collector {
		static final List<Double> QUANTILES = Arrays.asList(.5, .75, .95, .98, .99, .999);

		private final String metricName;
		private final String helpString;
		private final List<String> labelNamesWithQuantile;

		private final Map<List<String>, Histogram> histogramsByLabelValues = new HashMap<>();

		HistogramSummaryProxy(final Histogram histogram, final String metricName, final String helpString, final List<String> labelNames, final List<String> labelValues) {
			this.metricName = metricName;
			this.helpString = helpString;
			this.labelNamesWithQuantile = addToList(labelNames, "quantile");
			histogramsByLabelValues.put(labelValues, histogram);
		}

		@Override
		public List<MetricFamilySamples> collect() {
			// We cannot use SummaryMetricFamily because it is impossible to get a sum of all values (at least for Dropwizard histograms,
			// whose snapshot's values array only holds a sample of recent values).

			List<MetricFamilySamples.Sample> samples = new LinkedList<>();
			for (Map.Entry<List<String>, Histogram> labelValuesToHistogram : histogramsByLabelValues.entrySet()) {
				addSamples(labelValuesToHistogram.getKey(), labelValuesToHistogram.getValue(), samples);
			}
			return Collections.singletonList(new MetricFamilySamples(metricName, Type.SUMMARY, helpString, samples));
		}

		void addChild(final Histogram histogram, final List<String> labelValues) {
			histogramsByLabelValues.put(labelValues, histogram);
		}

		void remove(final List<String> labelValues) {
			histogramsByLabelValues.remove(labelValues);
		}

		private void addSamples(final List<String> labelValues, final Histogram histogram, final List<MetricFamilySamples.Sample> samples) {
			samples.add(new MetricFamilySamples.Sample(metricName + "_count",
				labelNamesWithQuantile.subList(0, labelNamesWithQuantile.size() - 1), labelValues, histogram.getCount()));
			for (final Double quantile : QUANTILES) {
				samples.add(new MetricFamilySamples.Sample(metricName, labelNamesWithQuantile,
					addToList(labelValues, quantile.toString()),
					histogram.getStatistics().getQuantile(quantile)));
			}
		}
	}

	private static List<String> addToList(List<String> list, String element) {
		final List<String> result = new ArrayList<>(list);
		result.add(element);
		return result;
	}

	private static String[] toArray(List<String> list) {
		return list.toArray(new String[list.size()]);
	}
}
