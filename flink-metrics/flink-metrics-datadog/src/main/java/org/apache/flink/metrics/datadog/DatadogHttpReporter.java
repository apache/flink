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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.datadog.metric.DCounter;
import org.apache.flink.metrics.datadog.metric.DGauge;
import org.apache.flink.metrics.datadog.metric.DSeries;
import org.apache.flink.metrics.datadog.parser.MetricParser;
import org.apache.flink.metrics.datadog.parser.NameAndTags;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * Abbr: dghttp
 *
 * Metric Reporter for Datadog
 *
 * When used, metric scope formats have to be defined as followed:
 *
 * metrics.scope.jm: <host>.jobmanager
 * metrics.scope.jm.job: <host>.<job_name>.jobmanager.job
 * metrics.scope.tm: <host>.<tm_id>.taskmanager
 * metrics.scope.tm.job: <host>.<tm_id>.<job_name>.taskmanager.job
 * metrics.scope.task: <host>.<tm_id>.<job_name>.<subtask_index>.<task_name>.task
 * metrics.scope.operator: <host>.<tm_id>.<job_name>.<subtask_index>.<operator_name>.operator
 *
 * Variables will be separated from metric names, and sent to Datadog as tags
 * */
public class DatadogHttpReporter extends AbstractReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(DatadogHttpReporter.class);

	public static final String API_KEY = "apikey";
	public static final String TAGS = "tags";

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private DatadogHttpClient client;
	private List<String> tags;

	private MetricParser metricParser;

	@Override
	public void open(MetricConfig config) {
		client = new DatadogHttpClient(config.getString(API_KEY, null));
		log.info("Configured DatadogHttpReporter");

		tags = getTags(config.getString(TAGS, null));
		metricParser = new MetricParser(config);
	}

	@Override
	public void close() {
		log.info("Shut down DatadogHttpReporter");
	}

	@Override
	public void report() {
		try {
			DatadogHttpRequest request = new DatadogHttpRequest(this);

			for (Map.Entry<Gauge<?>, String> entry : gauges.entrySet()) {
				// Flink uses Gauge to store values more than numeric, like String, hashmap, etc.
				// Need to filter out those non-numeric ones
				if(entry.getKey().getValue() instanceof Number) {
					NameAndTags nat = metricParser.getNameAndTags(entry.getValue());

						request.addGauge(
							new DGauge(
								nat.getName(),
								(Number) entry.getKey().getValue(),
								nat.getTags()));
				}
			}

			for (Map.Entry<Counter, String> entry : counters.entrySet()) {
				NameAndTags nat = metricParser.getNameAndTags(entry.getValue());

				request.addCounter(
					new DCounter(
						nat.getName(),
						entry.getKey().getCount(),
						nat.getTags()));
			}

			request.send();
		} catch (Exception e) {
			LOG.warn("Failed reporting metrics to Datadog.", e);
		}
	}

	@Override
	public String filterCharacters(String metricName) {
		return metricName;
	}

	/**
	 * Get tags from config 'metrics.reporter.dghttp.tags'
	 * */
	private List<String> getTags(String str) {
		if(str != null) {
			return Lists.newArrayList(str.split(","));
		} else {
			return Lists.newArrayList();
		}
	}

	/**
	 * Serialize metrics and send to Datadog
	 * */
	private static class DatadogHttpRequest {
		private final DatadogHttpReporter datadogHttpReporter;
		private final DSeries series;

		public DatadogHttpRequest(DatadogHttpReporter reporter) throws IOException {
			datadogHttpReporter = reporter;
			series = new DSeries();
		}

		public void addGauge(DGauge gauge) throws IOException {
			series.addMetric(gauge);
		}

		public void addCounter(DCounter counter) throws IOException {
			series.addMetric(counter);
		}

		public void send() throws Exception {
			datadogHttpReporter.client.syncPost(MAPPER.writeValueAsString(series));
		}
	}
}
