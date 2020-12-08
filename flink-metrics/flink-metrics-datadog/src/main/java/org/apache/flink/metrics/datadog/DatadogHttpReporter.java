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

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric Reporter for Datadog.
 *
 * <p>Variables in metrics scope will be sent to Datadog as tags.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.datadog.DatadogHttpReporterFactory")
public class DatadogHttpReporter implements MetricReporter, Scheduled {
	private static final Logger LOGGER = LoggerFactory.getLogger(DatadogHttpReporter.class);
	private static final String HOST_VARIABLE = "<host>";

	// Both Flink's Gauge and Meter values are taken as gauge in Datadog
	private final Map<Gauge, DGauge> gauges = new ConcurrentHashMap<>();
	private final Map<Counter, DCounter> counters = new ConcurrentHashMap<>();
	private final Map<Meter, DMeter> meters = new ConcurrentHashMap<>();
	private final Map<Histogram, DHistogram> histograms = new ConcurrentHashMap<>();

	private DatadogHttpClient client;
	private List<String> configTags;
	private int maxMetricsPerRequestValue;

	private final Clock clock = () -> System.currentTimeMillis() / 1000L;

	public static final String API_KEY = "apikey";
	public static final String PROXY_HOST = "proxyHost";
	public static final String PROXY_PORT = "proxyPort";
	public static final String DATA_CENTER = "dataCenter";
	public static final String TAGS = "tags";
	public static final String MAX_METRICS_PER_REQUEST = "maxMetricsPerRequest";

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final String name = group.getMetricIdentifier(metricName);

		List<String> tags = new ArrayList<>(configTags);
		tags.addAll(getTagsFromMetricGroup(group));
		String host = getHostFromMetricGroup(group);

		if (metric instanceof Counter) {
			Counter c = (Counter) metric;
			counters.put(c, new DCounter(c, name, host, tags, clock));
		} else if (metric instanceof Gauge) {
			Gauge g = (Gauge) metric;
			gauges.put(g, new DGauge(g, name, host, tags, clock));
		} else if (metric instanceof Meter) {
			Meter m = (Meter) metric;
			// Only consider rate
			meters.put(m, new DMeter(m, name, host, tags, clock));
		} else if (metric instanceof Histogram) {
			Histogram h = (Histogram) metric;
			histograms.put(h, new DHistogram(h, name, host, tags, clock));
		} else {
			LOGGER.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
				"does not support this metric type.", metric.getClass().getName());
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		if (metric instanceof Counter) {
			counters.remove(metric);
		} else if (metric instanceof Gauge) {
			gauges.remove(metric);
		} else if (metric instanceof Meter) {
			meters.remove(metric);
		} else if (metric instanceof Histogram) {
			histograms.remove(metric);
		} else {
			LOGGER.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
				"does not support this metric type.", metric.getClass().getName());
		}
	}

	@Override
	public void open(MetricConfig config) {
		String apiKey = config.getString(API_KEY, null);
		String proxyHost = config.getString(PROXY_HOST, null);
		Integer proxyPort = config.getInteger(PROXY_PORT, 8080);
		String rawDataCenter = config.getString(DATA_CENTER, "US");
		maxMetricsPerRequestValue = config.getInteger(MAX_METRICS_PER_REQUEST, 2000);
		DataCenter dataCenter = DataCenter.valueOf(rawDataCenter);
		String tags = config.getString(TAGS, "");

		client = new DatadogHttpClient(apiKey, proxyHost, proxyPort, dataCenter, true);

		configTags = getTagsFromConfig(tags);

		LOGGER.info("Configured DatadogHttpReporter with {tags={}, proxyHost={}, proxyPort={}, dataCenter={}, maxMetricsPerRequest={}", tags, proxyHost, proxyPort, dataCenter, maxMetricsPerRequestValue);
	}

	@Override
	public void close() {
		client.close();
		LOGGER.info("Shut down DatadogHttpReporter");
	}

	@Override
	public void report() {
		DSeries request = new DSeries();

		addGaugesAndUnregisterOnException(request);
		counters.values().forEach(request::add);
		meters.values().forEach(request::add);
		histograms.values().forEach(histogram -> histogram.addTo(request));

		int totalMetrics = request.getSeries().size();
		int fromIndex = 0;
		while (fromIndex < totalMetrics) {
			int toIndex = Math.min(fromIndex + maxMetricsPerRequestValue, totalMetrics);
			try {
				DSeries chunk = new DSeries(request.getSeries().subList(fromIndex, toIndex));
				client.send(chunk);
				chunk.getSeries().forEach(DMetric::ackReport);
				LOGGER.debug("Reported series with size {}.", chunk.getSeries().size());
			} catch (SocketTimeoutException e) {
				LOGGER.warn("Failed reporting metrics to Datadog because of socket timeout: {}", e.getMessage());
			} catch (Exception e) {
				LOGGER.warn("Failed reporting metrics to Datadog.", e);
			}
			fromIndex = toIndex;
		}
	}

	private void addGaugesAndUnregisterOnException(DSeries request) {
		List<Gauge> gaugesToRemove = new ArrayList<>();
		for (Map.Entry<Gauge, DGauge> entry : gauges.entrySet()) {
			DGauge g = entry.getValue();
			try {
				// Will throw exception if the Gauge is not of Number type
				// Flink uses Gauge to store many types other than Number
				g.getMetricValue();
				request.add(g);
			} catch (ClassCastException e) {
				LOGGER.info("The metric {} will not be reported because only number types are supported by this reporter.", g.getMetricName());
				gaugesToRemove.add(entry.getKey());
			} catch (Exception e) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("The metric {} will not be reported because it threw an exception.", g.getMetricName(), e);
				} else {
					LOGGER.info("The metric {} will not be reported because it threw an exception.", g.getMetricName());
				}
				gaugesToRemove.add(entry.getKey());
			}
		}
		gaugesToRemove.forEach(gauges::remove);
	}

	/**
	 * Get config tags from config 'metrics.reporter.dghttp.tags'.
	 */
	private List<String> getTagsFromConfig(String str) {
		return Arrays.asList(str.split(","));
	}

	/**
	 * Get tags from MetricGroup#getAllVariables(), excluding 'host'.
	 */
	private List<String> getTagsFromMetricGroup(MetricGroup metricGroup) {
		List<String> tags = new ArrayList<>();

		for (Map.Entry<String, String> entry: metricGroup.getAllVariables().entrySet()) {
			if (!entry.getKey().equals(HOST_VARIABLE)) {
				tags.add(getVariableName(entry.getKey()) + ":" + entry.getValue());
			}
		}

		return tags;
	}

	private String getHostFromMetricGroup(MetricGroup metricGroup) {
		return metricGroup.getAllVariables().get(HOST_VARIABLE);
	}

	/**
	 * Removes leading and trailing angle brackets.
	 */
	private String getVariableName(String str) {
		return str.substring(1, str.length() - 1);
	}
}
