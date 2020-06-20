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
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.NetUtils;

import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.CONNECT_TIMEOUT;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.CONSISTENCY;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.DB;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.HOST;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.PASSWORD;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.PORT;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.RETENTION_POLICY;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.USERNAME;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.WRITE_TIMEOUT;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.getConsistencyLevel;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.getInteger;
import static org.apache.flink.metrics.influxdb.InfluxdbReporterOptions.getString;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.influxdb.InfluxdbReporterFactory")
public class InfluxdbReporter extends AbstractReporter<MeasurementInfo> implements Scheduled {

	private String database;
	private String retentionPolicy;
	private InfluxDB.ConsistencyLevel consistency;
	private InfluxDB influxDB;

	public InfluxdbReporter() {
		super(new MeasurementInfoProvider());
	}

	@Override
	public void open(MetricConfig config) {
		String host = getString(config, HOST);
		int port = getInteger(config, PORT);
		if (!isValidHost(host) || !NetUtils.isValidClientPort(port)) {
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
		this.consistency = getConsistencyLevel(config, CONSISTENCY);

		int connectTimeout = getInteger(config, CONNECT_TIMEOUT);
		int writeTimeout = getInteger(config, WRITE_TIMEOUT);
		OkHttpClient.Builder client = new OkHttpClient.Builder()
			.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
			.writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);

		if (username != null && password != null) {
			influxDB = InfluxDBFactory.connect(url, username, password, client);
		} else {
			influxDB = InfluxDBFactory.connect(url, client);
		}

		log.info("Configured InfluxDBReporter with {host:{}, port:{}, db:{}, retentionPolicy:{} and consistency:{}}",
			host, port, database, retentionPolicy, consistency.name());
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
		report.consistency(consistency);
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

}
