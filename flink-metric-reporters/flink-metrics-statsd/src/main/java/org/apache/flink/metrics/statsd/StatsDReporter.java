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
package org.apache.flink.metrics.statsd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Largely based on the StatsDReporter class by ReadyTalk
 * https://github.com/ReadyTalk/metrics-statsd/blob/master/metrics3-statsd/src/main/java/com/readytalk/metrics/StatsDReporter.java
 *
 * Ported since it was not present in maven central.
 */
public class StatsDReporter extends AbstractReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(StatsDReporter.class);

	public static final String ARG_HOST = "host";
	public static final String ARG_PORT = "port";
	public static final String ARG_CONVERSION_RATE = "rateConversion";
	public static final String ARG_CONVERSION_DURATION = "durationConversion";

	private DatagramSocket socket;
	private InetSocketAddress address;

	private double durationFactor;
	private double rateFactor;

	@Override
	public void open(Configuration config) {
		String host = config.getString(ARG_HOST, null);
		int port = config.getInteger(ARG_PORT, -1);

		if (host == null || host.length() == 0 || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		String conversionRate = config.getString(ARG_CONVERSION_RATE, "SECONDS");
		String conversionDuration = config.getString(ARG_CONVERSION_DURATION, "MILLISECONDS");

		this.address = new InetSocketAddress(host, port);
		this.rateFactor = TimeUnit.valueOf(conversionRate).toSeconds(1);
		this.durationFactor = 1.0 / TimeUnit.valueOf(conversionDuration).toNanos(1);
		try {
			this.socket = new DatagramSocket(0);
		} catch (SocketException e) {
			throw new RuntimeException("Failure while creating socket. ", e);
		}
	}

	@Override
	public void close() {
		if (socket != null && !socket.isClosed()) {
			socket.close();
		}
	}

	@Override
	public String generateName(String name, List<String> scope) {
		StringBuilder sb = new StringBuilder();
		for (String s : scope) {
			sb.append(s);
			sb.append('.');
		}
		sb.append(name);
		return sb.toString();
	}

	public void send(final String name, final double value) {
		send(name, "" + value);
	}

	public void send(final String name, final String value) {
		try {
			String formatted = String.format("%s:%s|g", name, value);
			byte[] data = formatted.getBytes();
			socket.send(new DatagramPacket(data, data.length, this.address));
		} catch (IOException e) {
			LOG.error("unable to send packet to statsd at '{}:{}'", address.getHostName(), address.getPort());
		}
	}

	@Override
	public void report() {
		for (Map.Entry<String, Gauge<?>> entry : gauges.entrySet()) {
			reportGauge(entry.getKey(), entry.getValue());
		}

		for (Map.Entry<String, Counter> entry : counters.entrySet()) {
			reportCounter(entry.getKey(), entry.getValue());
		}
	}

	private void reportCounter(final String name, final Counter counter) {
		send(name, counter.getCount());
	}

	private void reportGauge(final String name, final Gauge<?> gauge) {
		final String value = gauge.getValue().toString();
		if (value != null) {
			send((name), value);
		}
	}
}
