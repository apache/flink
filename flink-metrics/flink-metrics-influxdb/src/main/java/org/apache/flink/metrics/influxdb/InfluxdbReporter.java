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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.dropwizard.ScheduledDropwizardReporter;
import org.apache.flink.metrics.MetricConfig;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import metrics_influxdb.HttpInfluxdbProtocol;
import metrics_influxdb.api.measurements.CategoriesMetricMeasurementTransformer;

import java.util.concurrent.TimeUnit;

/**
 * This class acts as a factory for the {@link metrics_influxdb.InfluxdbReporter} and allows using it as a Flink
 * reporter.
 */
@PublicEvolving
public class InfluxdbReporter extends ScheduledDropwizardReporter {

	public static final String ARG_USER = "user";
	public static final String ARG_PASSWORD = "password";
	public static final String ARG_DB = "db";

	@Override
	public ScheduledReporter getReporter(final MetricConfig config) {
		final String host = config.getString(ARG_HOST, "localhost");
		final Integer port = config.getInteger(ARG_PORT, HttpInfluxdbProtocol.DEFAULT_PORT);
		final String user = config.getString(ARG_USER, null);
		final String password = config.getString(ARG_PASSWORD, null);
		final String db = config.getString(ARG_DB, "flink");
		final String conversionRate = config.getString(ARG_CONVERSION_RATE, null);
		final String conversionDuration = config.getString(ARG_CONVERSION_DURATION, null);

		final metrics_influxdb.InfluxdbReporter.Builder builder =
			metrics_influxdb.InfluxdbReporter.forRegistry(registry);

		builder.protocol(new HttpInfluxdbProtocol(host, port, user, password, db));

		if (conversionRate != null) {
			builder.convertRatesTo(TimeUnit.valueOf(conversionRate));
		}

		if (conversionDuration != null) {
			builder.convertDurationsTo(TimeUnit.valueOf(conversionDuration));
		}

		builder
			.filter(MetricFilter.ALL)
			.skipIdleMetrics(false)
			.transformer(new CategoriesMetricMeasurementTransformer(
				"host", "process_type", "tm_id", "job_name", "task_name", "subtask_index"
			));

		return builder.build();
	}

}
