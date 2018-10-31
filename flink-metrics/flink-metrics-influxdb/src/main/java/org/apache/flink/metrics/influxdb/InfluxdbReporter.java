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

import org.apache.flink.annotation.Experimental;
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
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB.
 */
@Experimental
public class InfluxdbReporter extends AbstractReporter<MeasurementInfo> implements Scheduled {

	private static final String ARG_PROTOCOL = "protocol";
	private static final List<String> SUPPORTED_PROTOCOLS = Arrays.asList("http", "https");
	private static final String ARG_HOST = "host";
	private static final String ARG_PORT = "port";
	private static final int DEFAULT_PORT = 8086;
	private static final String ARG_USERNAME = "username";
	private static final String ARG_PASSWORD = "password";
	private static final String ARG_DB = "db";
	private static final String DEFAULT_DB = "flink";

	private String database;
	private InfluxDB influxDB;

	public InfluxdbReporter() {
		super(new MeasurementInfoProvider());
	}

	@Override
	public void open(MetricConfig config) {
		String protocol = config.getString(ARG_PROTOCOL, "http").toLowerCase();
		if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
			throw new IllegalArgumentException("Not supported protocol: " + protocol);
		}
		String host = config.getString(ARG_HOST, null);
		int port = config.getInteger(ARG_PORT, DEFAULT_PORT);
		if (host == null || host.isEmpty() || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}
		String url = String.format("%s://%s:%d", protocol, host, port);
		String username = config.getString(ARG_USERNAME, null);
		String password = config.getString(ARG_PASSWORD, null);
		database = config.getString(ARG_DB, DEFAULT_DB);
		if (username != null && password != null) {
			influxDB = InfluxDBFactory.connect(url, username, password);
		} else {
			influxDB = InfluxDBFactory.connect(url);
		}
		log.info("Configured InfluxDBReporter with {protocol:{}, host:{}, port:{}, db:{}}", protocol, host, port, database);
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
		report.retentionPolicy("");
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

}
