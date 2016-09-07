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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Largely based on the StatsDReporter class by ReadyTalk
 * https://github.com/ReadyTalk/metrics-statsd/blob/master/metrics3-statsd/src/main/java/com/readytalk/metrics/StatsDReporter.java
 *
 * Ported since it was not present in maven central.
 */
@PublicEvolving
public class StatsDReporter extends AbstractReporter implements Scheduled {
	
	private static final Logger LOG = LoggerFactory.getLogger(StatsDReporter.class);

	public static final String ARG_HOST = "host";
	public static final String ARG_PORT = "port";
//	public static final String ARG_CONVERSION_RATE = "rateConversion";
//	public static final String ARG_CONVERSION_DURATION = "durationConversion";

	private boolean closed = false;

	private DatagramSocket socket;
	private InetSocketAddress address;

	@Override
	public void open(MetricConfig config) {
		String host = config.getString(ARG_HOST, null);
		int port = config.getInteger(ARG_PORT, -1);

		if (host == null || host.length() == 0 || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		this.address = new InetSocketAddress(host, port);

		LOG.info("Starting StatsDReporter to send metric reports to " + address);

//		String conversionRate = config.getString(ARG_CONVERSION_RATE, "SECONDS");
//		String conversionDuration = config.getString(ARG_CONVERSION_DURATION, "MILLISECONDS");
//		this.rateFactor = TimeUnit.valueOf(conversionRate).toSeconds(1);
//		this.durationFactor = 1.0 / TimeUnit.valueOf(conversionDuration).toNanos(1);

		try {
			this.socket = new DatagramSocket(0);
		} catch (SocketException e) {
			throw new RuntimeException("Could not create datagram socket. ", e);
		}
	}

	@Override
	public void close() {
		closed = true;
		if (socket != null && !socket.isClosed()) {
			socket.close();
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public void report() {
		// instead of locking here, we tolerate exceptions
		// we do this to prevent holding the lock for very long and blocking
		// operator creation and shutdown
		try {
			for (Map.Entry<Gauge<?>, String> entry : gauges.entrySet()) {
				if (closed) {
					return;
				}
				reportGauge(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Counter, String> entry : counters.entrySet()) {
				if (closed) {
					return;
				}
				reportCounter(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Histogram, String> entry : histograms.entrySet()) {
				reportHistogram(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Meter, String> entry : meters.entrySet()) {
				reportMeter(entry.getValue(), entry.getKey());
			}
		}
		catch (ConcurrentModificationException | NoSuchElementException e) {
			// ignore - may happen when metrics are concurrently added or removed
			// report next time
		}
	}

	// ------------------------------------------------------------------------
	
	private void reportCounter(final String name, final Counter counter) {
		send(name, String.valueOf(counter.getCount()));
	}

	private void reportGauge(final String name, final Gauge<?> gauge) {
		Object value = gauge.getValue();
		if (value != null) {
			send(name, value.toString());
		}
	}

	private void reportHistogram(final String name, final Histogram histogram) {
		if (histogram != null) {

			HistogramStatistics statistics = histogram.getStatistics();

			if (statistics != null) {
				send(prefix(name, "count"), String.valueOf(histogram.getCount()));
				send(prefix(name, "max"), String.valueOf(statistics.getMax()));
				send(prefix(name, "min"), String.valueOf(statistics.getMin()));
				send(prefix(name, "mean"), String.valueOf(statistics.getMean()));
				send(prefix(name, "stddev"), String.valueOf(statistics.getStdDev()));
				send(prefix(name, "p50"), String.valueOf(statistics.getQuantile(0.5)));
				send(prefix(name, "p75"), String.valueOf(statistics.getQuantile(0.75)));
				send(prefix(name, "p95"), String.valueOf(statistics.getQuantile(0.95)));
				send(prefix(name, "p98"), String.valueOf(statistics.getQuantile(0.98)));
				send(prefix(name, "p99"), String.valueOf(statistics.getQuantile(0.99)));
				send(prefix(name, "p999"), String.valueOf(statistics.getQuantile(0.999)));
			}
		}
	}

	private void reportMeter(final String name, final Meter meter) {
		if (meter != null) {
			send(prefix(name, "rate"), String.valueOf(meter.getRate()));
			send(prefix(name, "count"), String.valueOf(meter.getCount()));
		}
	}

	private String prefix(String ... names) {
		if (names.length > 0) {
			StringBuilder stringBuilder = new StringBuilder(names[0]);

			for (int i = 1; i < names.length; i++) {
				stringBuilder.append('.').append(names[i]);
			}

			return stringBuilder.toString();
		} else {
			return "";
		}
	}

	private void send(final String name, final String value) {
		try {
			String formatted = String.format("%s:%s|g", name, value);
			byte[] data = formatted.getBytes();
			socket.send(new DatagramPacket(data, data.length, this.address));
		}
		catch (IOException e) {
			LOG.error("unable to send packet to statsd at '{}:{}'", address.getHostName(), address.getPort());
		}
	}

	@Override
	public String filterCharacters(String input) {
		char[] chars = null;
		final int strLen = input.length();
		int pos = 0;

		for (int i = 0; i < strLen; i++) {
			final char c = input.charAt(i);
			switch (c) {
				case ':':
					if (chars == null) {
						chars = input.toCharArray();
					}
					chars[pos++] = '-';
					break;

				default:
					if (chars != null) {
						chars[pos] = c;
					}
					pos++;
			}
		}

		return chars == null ? input : new String(chars, 0, pos);
	}
}
