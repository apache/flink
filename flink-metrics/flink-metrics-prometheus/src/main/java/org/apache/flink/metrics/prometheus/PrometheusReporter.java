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

import com.codahale.metrics.ScheduledReporter;
import fi.iki.elonen.NanoHTTPD;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.dropwizard.ScheduledDropwizardReporter;
import org.apache.flink.metrics.MetricConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

@PublicEvolving
public class PrometheusReporter extends ScheduledDropwizardReporter {
	private static final Logger log = LoggerFactory.getLogger(PrometheusReporter.class);

	private static final int DEFAULT_PORT = 9249;

	private PrometheusEndpoint prometheusEndpoint;

	@Override
	public void open(MetricConfig config) {
		DropwizardExports collector = new DropwizardExports(registry);
		CollectorRegistry.defaultRegistry.register(collector);

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
	public ScheduledReporter getReporter(MetricConfig config) {
		return null; // no wrapped reporter
	}

	@Override
	public void report() {
		// nothing to do as Prometheus pulls metrics from the endpoint
	}

	@Override
	public void close() {
		prometheusEndpoint.stop();
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

}
