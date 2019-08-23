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
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.GROUPING_KEY;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.PORT;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
 */
@PublicEvolving
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {

	private PushGateway pushGateway;
	private String jobName;
	private boolean deleteOnShutdown;
	private Map<String, String> groupingKey;

	@Override
	public void open(MetricConfig config) {
		super.open(config);

		String host = config.getString(HOST.key(), HOST.defaultValue());
		int port = config.getInteger(PORT.key(), PORT.defaultValue());
		String configuredJobName = config.getString(JOB_NAME.key(), JOB_NAME.defaultValue());
		boolean randomSuffix = config.getBoolean(RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());
		deleteOnShutdown = config.getBoolean(DELETE_ON_SHUTDOWN.key(), DELETE_ON_SHUTDOWN.defaultValue());
		groupingKey = parserGroupingKey(config.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));
		if (groupingKey != null) {
			log.info("PrometheusPushGatewayReporter groupingKey: {}", groupingKey);
		}

		if (host == null || host.isEmpty() || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		if (randomSuffix) {
			this.jobName = configuredJobName + new AbstractID();
		} else {
			this.jobName = configuredJobName;
		}

		pushGateway = new PushGateway(host + ':' + port);
		log.info("Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName: {}, randomJobNameSuffix:{}, deleteOnShutdown:{}}", host, port, jobName, randomSuffix, deleteOnShutdown);
	}

	@VisibleForTesting
	static Map<String, String> parserGroupingKey(final String groupingKeyConfig) {
		if (!groupingKeyConfig.isEmpty()) {
			Map<String, String> groupingKey = new HashMap<>();
			String[] kvs = groupingKeyConfig.split(",");
			for (String kv : kvs) {
				int idx = kv.indexOf("=");
				if (idx < 0) {
					continue;
				}
				groupingKey.put(kv.substring(0, idx), kv.substring(idx + 1));
			}

			return groupingKey;
		}
		return null;
	}

	@Override
	public void report() {
		try {
			pushGateway.push(CollectorRegistry.defaultRegistry, jobName, groupingKey);
		} catch (Exception e) {
			log.warn("Failed to push metrics to PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
		}
	}

	@Override
	public void close() {
		if (deleteOnShutdown && pushGateway != null) {
			try {
				pushGateway.delete(jobName, groupingKey);
			} catch (IOException e) {
				log.warn("Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
			}
		}
		super.close();
	}
}
