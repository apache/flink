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

package org.apache.flink.metrics.ganglia;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.dropwizard.ScheduledDropwizardReporter;
import org.apache.flink.metrics.MetricConfig;

import com.codahale.metrics.ScheduledReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This class acts as a factory for the {@link com.codahale.metrics.ganglia.GangliaReporter} and allows using it as a
 * Flink reporter.
 */
@PublicEvolving
public class GangliaReporter extends ScheduledDropwizardReporter {

	public static final String ARG_DMAX = "dmax";
	public static final String ARG_TMAX = "tmax";
	public static final String ARG_TTL = "ttl";
	public static final String ARG_MODE_ADDRESSING = "addressingMode";

	@Override
	public ScheduledReporter getReporter(MetricConfig config) {

		try {
			String host = config.getString(ARG_HOST, null);
			int port = config.getInteger(ARG_PORT, -1);
			if (host == null || host.length() == 0 || port < 1) {
				throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
			}
			String addressingMode = config.getString(ARG_MODE_ADDRESSING, "MULTICAST");
			int ttl = config.getInteger(ARG_TTL, 1);
			GMetric gMetric = new GMetric(host, port, GMetric.UDPAddressingMode.valueOf(addressingMode), ttl);

			String prefix = config.getString(ARG_PREFIX, null);
			String conversionRate = config.getString(ARG_CONVERSION_RATE, null);
			String conversionDuration = config.getString(ARG_CONVERSION_DURATION, null);
			int dMax = config.getInteger(ARG_DMAX, 0);
			int tMax = config.getInteger(ARG_TMAX, 60);

			com.codahale.metrics.ganglia.GangliaReporter.Builder builder =
				com.codahale.metrics.ganglia.GangliaReporter.forRegistry(registry);

			if (prefix != null) {
				builder.prefixedWith(prefix);
			}
			if (conversionRate != null) {
				builder.convertRatesTo(TimeUnit.valueOf(conversionRate));
			}
			if (conversionDuration != null) {
				builder.convertDurationsTo(TimeUnit.valueOf(conversionDuration));
			}
			builder.withDMax(dMax);
			builder.withTMax(tMax);

			log.info("Configured GangliaReporter with {host:{}, port:{}, dmax:{}, tmax:{}, ttl:{}, addressingMode:{}}",
				host, port, dMax, tMax, ttl, addressingMode);
			return builder.build(gMetric);
		} catch (IOException e) {
			throw new RuntimeException("Error while instantiating GangliaReporter.", e);
		}
	}
}
