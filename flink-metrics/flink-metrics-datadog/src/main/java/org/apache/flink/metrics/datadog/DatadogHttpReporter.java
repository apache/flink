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
public class DatadogHttpReporter implements MetricReporter, Scheduled {
	private static final Logger LOGGER = LoggerFactory.getLogger(DatadogHttpReporter.class);
	private static final String HOST_VARIABLE = "<host>";
	private static final String TM_ID_VARIABLE = "<tm_id>";
	private static final String TM_ID_DEFAULT_VALUE = ""; // empty
	private static final String JOB_NAME_JOB_MANAGER_VARIABLE = "jobmanager";
	private static final String JOB_NAME_TASK_MANAGER_VARIABLE = "taskmanager";
	private static final String JOB_NAME_DEFAULT_VALUE = ""; // empty

	// Both Flink's Gauge and Meter values are taken as gauge in Datadog
	private final Map<Gauge, DGauge> gauges = new ConcurrentHashMap<>();
	private final Map<Counter, DCounter> counters = new ConcurrentHashMap<>();
	private final Map<Meter, DMeter> meters = new ConcurrentHashMap<>();

	private DatadogHttpClient client;
	private List<String> configTags;
	private String metricsPrefix;

	public static final String API_KEY = "apikey";
	public static final String TAGS = "tags";
	public static final String PREFIX = "prefix";

	public static final String DEFAULT_METRICS_PREFIX = "flink";

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {

		// expected format: <host>.<taskmanager|jobmanager>.<tm_id|empty>.<remained parts>
		String metricIdentifier = group.getMetricIdentifier(metricName);

		List<String> tags = new ArrayList<>(configTags);
		tags.addAll(getTagsFromMetricGroup(group));
		String host = getHostFromMetricGroup(group);

		// exclude host name, job_name, tm_id from metric name
		metricIdentifier = beautifyMetricIdentifier(metricIdentifier, host, group);

		if (metric instanceof Counter) {
			Counter c = (Counter) metric;
			counters.put(c, new DCounter(c, metricIdentifier, host, tags));
		} else if (metric instanceof Gauge) {
			Gauge g = (Gauge) metric;
			gauges.put(g, new DGauge(g, metricIdentifier, host, tags));
		} else if (metric instanceof Meter) {
			Meter m = (Meter) metric;
			// Only consider rate
			meters.put(m, new DMeter(m, metricIdentifier, host, tags));
		} else if (metric instanceof Histogram) {
			LOGGER.warn("Cannot add {} because Datadog HTTP API doesn't support Histogram", metricName);
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
			// No Histogram is registered
		} else {
			LOGGER.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
		}
	}

	@Override
	public void open(MetricConfig config) {
		client = new DatadogHttpClient(config.getString(API_KEY, null));
		LOGGER.info("Configured DatadogHttpReporter");

		// get tags
		configTags = getTagsFromConfig(config.getString(TAGS, ""));
		LOGGER.info(String.format("configTags: %s", String.join(",", configTags)));

		// get metrics prefix, default is: `flink.`
		metricsPrefix = config.getString(PREFIX, DEFAULT_METRICS_PREFIX);
		LOGGER.info(String.format("metricsPrefix: %s", metricsPrefix));
	}

	@Override
	public void close() {
		client.close();
		LOGGER.info("Shut down DatadogHttpReporter");
	}

	@Override
	public void report() {
		DatadogHttpRequest request = new DatadogHttpRequest();

		for (Map.Entry<Gauge, DGauge> entry : gauges.entrySet()) {
			DGauge g = entry.getValue();
			try {
				// Will throw exception if the Gauge is not of Number type
				// Flink uses Gauge to store many types other than Number
				g.getMetricValue();
				request.addGauge(g);
			} catch (Exception e) {
				// Remove that Gauge if it's not of Number type
				gauges.remove(entry.getKey());
			}
		}

		for (DCounter c : counters.values()) {
			request.addCounter(c);
		}

		for (DMeter m : meters.values()) {
			request.addMeter(m);
		}

		try {
			client.send(request);
		} catch (SocketTimeoutException e) {
			LOGGER.warn("Failed reporting metrics to Datadog because of socket timeout.", e.getMessage());
		} catch (Exception e) {
			LOGGER.warn("Failed reporting metrics to Datadog.", e);
		}
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

	private String getTmIdFromMetricGroup(MetricGroup metricGroup) {
		return metricGroup.getAllVariables().getOrDefault(TM_ID_VARIABLE, TM_ID_DEFAULT_VALUE);
	}

	/**
	 * @return: job_manager or task_manager name or default value (empty)
	 */
	private String getJobName(String metricIdentifier) {
		if(metricIdentifier.contains(JOB_NAME_JOB_MANAGER_VARIABLE))
			return JOB_NAME_JOB_MANAGER_VARIABLE;
		else if (metricIdentifier.contains(JOB_NAME_TASK_MANAGER_VARIABLE))
			return JOB_NAME_TASK_MANAGER_VARIABLE;
		else
			return JOB_NAME_DEFAULT_VALUE;
	}

	/**
	 * @return: metric_name after excluding host_name, task|job_manager and tm_id
	 */
	private String beautifyMetricIdentifier(String metricIdentifier, String host, MetricGroup metricGroup) {

		String jobName = getJobName(metricIdentifier);
		String tmId = getTmIdFromMetricGroup(metricGroup);

		StringBuilder replaceRegex = new StringBuilder();
		replaceRegex.append(host);

		if(!jobName.isEmpty())
			replaceRegex.append(String.format(".%s", jobName));

		if(!tmId.isEmpty())
			replaceRegex.append(String.format(".%s", tmId));

		// replace <host>.<job_name>.<tm_id> by metricsPrefix
		return metricIdentifier.replaceFirst(replaceRegex.toString(), metricsPrefix);
	}

	/**
	 * Removes leading and trailing angle brackets.
	 */
	private String getVariableName(String str) {
		return str.substring(1, str.length() - 1);
	}

	/**
	 * Compact metrics in batch, serialize them, and send to Datadog via HTTP.
	 */
	static class DatadogHttpRequest {
		private final DSeries series;

		public DatadogHttpRequest() {
			series = new DSeries();
		}

		public void addGauge(DGauge gauge) {
			series.addMetric(gauge);
		}

		public void addCounter(DCounter counter) {
			series.addMetric(counter);
		}

		public void addMeter(DMeter meter) {
			series.addMetric(meter);
		}

		public DSeries getSeries() {
			return series;
		}
	}
}
