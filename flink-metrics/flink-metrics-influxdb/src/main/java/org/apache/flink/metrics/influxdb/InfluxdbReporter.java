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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.DB;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.HOST;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.PASSWORD;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.PORT;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.RETENTION_POLICY;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.USERNAME;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.getInteger;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.getString;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB.
 */
public class InfluxdbReporter extends AbstractReporter<MeasurementInfo> implements Scheduled {

	private String database;
	private String retentionPolicy;
	private InfluxDB influxDB;

	public InfluxdbReporter() {
		super(new MeasurementInfoProvider());
	}

	@Override
	public void open(MetricConfig config) {
		String host = getString(config, HOST);
		int port = getInteger(config, PORT);
		if (!isValidHost(host) || !isValidPort(port)) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}
		String database = getString(config, DB);
		if (database == null) {
			throw new IllegalArgumentException("'" + DB.key() + "' configuration option is not set");
		}
		String url = String.format("http://%s:%d", host, port);
		String username = getString(config, USERNAME);
		String password = getString(config, PASSWORD);

		this.database = database;
		this.retentionPolicy = getString(config, RETENTION_POLICY);
		if (username != null && password != null) {
			influxDB = InfluxDBFactory.connect(url, username, password);
		} else {
			influxDB = InfluxDBFactory.connect(url);
		}

		log.info("Configured InfluxDBReporter with {host:{}, port:{}, db:{}, and retentionPolicy:{}}", host, port, database, retentionPolicy);
	}

	@Override
	public void close() {
		if (influxDB != null) {
			influxDB.close();
			influxDB = null;
		}
	}

	@Override
	public void report() {
		BatchPoints report = buildReport();
		if (report != null) {
			influxDB.write(report);
		}
	}

	@Nullable
	private BatchPoints buildReport() {
		Instant timestamp = Instant.now();
		BatchPoints.Builder report = BatchPoints.database(database);
		report.retentionPolicy(retentionPolicy);
		try {
			for (Map.Entry<Gauge<?>, MeasurementInfo> entry : gauges.entrySet()) {
				report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
			}

			for (Map.Entry<Counter, MeasurementInfo> entry : counters.entrySet()) {
				report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
			}

			for (Map.Entry<Histogram, MeasurementInfo> entry : histograms.entrySet()) {
				report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
			}

			for (Map.Entry<Meter, MeasurementInfo> entry : meters.entrySet()) {
				report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
			}
		}
		catch (ConcurrentModificationException | NoSuchElementException e) {
			// ignore - may happen when metrics are concurrently added or removed
			// report next time
			return null;
		}
		return report.build();
	}

	private static boolean isValidHost(String host) {
		return host != null && !host.isEmpty();
	}

	private static boolean isValidPort(int port) {
		return 0 < port && port <= 65535;
	}
}
