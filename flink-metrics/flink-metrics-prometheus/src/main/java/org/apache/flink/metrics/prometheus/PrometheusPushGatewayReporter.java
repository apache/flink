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
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * /**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus Pushgateway.
 */
@PublicEvolving
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(PrometheusPushGatewayReporter.class);

	public static final String ARG_HOST = "host";
	public static final String ARG_PORT = "port";

	public static final char JOB_NAME_SEPARATOR = '-';
	public static final String JOB_NAME_PREFIX = "flink" + JOB_NAME_SEPARATOR;

	private PushGateway pushGateway;
	private final String jobName;

	public PrometheusPushGatewayReporter() {
		String random = new AbstractID().toString();
		jobName = JOB_NAME_PREFIX + random;
	}

	@Override
	public void open(MetricConfig config) {

		String host = config.getString(ARG_HOST, null);
		int port = config.getInteger(ARG_PORT, -1);

		if (host == null || host.length() == 0 || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		pushGateway = new PushGateway(host + ":" + port);

		LOG.info("Configured PrometheusPushGatewayReporter with {host:{}, port:{}}", host, port);
	}

	@Override
	public void report() {
		try {
			pushGateway.push(CollectorRegistry.defaultRegistry, jobName);
		} catch (Exception e) {
			LOG.warn("Failed reporting metrics to Prometheus.", e);
		}
	}

}
