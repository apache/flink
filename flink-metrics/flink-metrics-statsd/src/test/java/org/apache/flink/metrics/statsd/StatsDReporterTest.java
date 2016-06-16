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
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

public class StatsDReporterTest extends TestLogger {

	/**
	 * Tests that histograms are properly reported via the StatsD reporter
	 */
	@Test
	public void testStatsDHistogramReporting() throws Exception {
		MetricRegistry registry = null;
		DatagramSocketReceiver receiver = null;
		Thread receiverThread = null;
		long timeout = 5000;
		long joinTimeout = 30000;

		String histogramName = "histogram";

		try {
			receiver = new DatagramSocketReceiver();

			receiverThread = new Thread(receiver);

			receiverThread.start();

			int port = receiver.getPort();

			Configuration config = new Configuration();
			config.setString(MetricRegistry.KEY_METRICS_REPORTER_CLASS, StatsDReporter.class.getName());
			config.setString(MetricRegistry.KEY_METRICS_REPORTER_INTERVAL, "1 SECONDS");
			config.setString(MetricRegistry.KEY_METRICS_REPORTER_ARGUMENTS, "--host localhost --port " + port);

			registry = new MetricRegistry(config);

			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");

			TestingHistogram histogram = new TestingHistogram();

			registry.register(histogram, histogramName, metricGroup);

			receiver.waitUntilNumLines(11, timeout);

			Set<String> lines = receiver.getLines();

			String prefix = metricGroup.getScopeString() + "." + histogramName;

			Set<String> expectedLines = new HashSet<>();

			expectedLines.add(prefix + ".count:42|g");
			expectedLines.add(prefix + ".mean:42.0|g");
			expectedLines.add(prefix + ".min:42|g");
			expectedLines.add(prefix + ".max:42|g");
			expectedLines.add(prefix + ".stddev:42.0|g");
			expectedLines.add(prefix + ".p75:42.0|g");
			expectedLines.add(prefix + ".p98:42.0|g");
			expectedLines.add(prefix + ".p99:42.0|g");
			expectedLines.add(prefix + ".p999:42.0|g");
			expectedLines.add(prefix + ".p95:42.0|g");
			expectedLines.add(prefix + ".p50:42.0|g");

			assertEquals(expectedLines, lines);

		} finally {
			if (registry != null) {
				registry.shutdown();
			}

			if (receiver != null) {
				receiver.stop();
			}

			if (receiverThread != null) {
				receiverThread.join(joinTimeout);
			}
		}
	}

	public static class TestingHistogram implements Histogram {

		@Override
		public void update(long value) {

		}

		@Override
		public long getCount() {
			return 42;
		}

		@Override
		public HistogramStatistics getStatistics() {
			return new HistogramStatistics() {
				@Override
				public double getValue(double quantile) {
					return 42;
				}

				@Override
				public long[] getValues() {
					return new long[0];
				}

				@Override
				public int size() {
					return 42;
				}

				@Override
				public double getMean() {
					return 42;
				}

				@Override
				public double getStdDev() {
					return 42;
				}

				@Override
				public long getMax() {
					return 42;
				}

				@Override
				public long getMin() {
					return 42;
				}
			};
		}
	}

	public static class DatagramSocketReceiver implements Runnable {
		private static final Object obj = new Object();

		private final DatagramSocket socket;
		private final ConcurrentHashMap<String, Object> lines;

		private boolean running = true;

		public DatagramSocketReceiver() throws SocketException {
			socket = new DatagramSocket();
			lines = new ConcurrentHashMap<>();
		}

		public int getPort() {
			return socket.getLocalPort();
		}

		public void stop() {
			running = false;
			socket.close();
		}

		public void waitUntilNumLines(int numberLines, long timeout) throws TimeoutException {
			long endTimeout = System.currentTimeMillis() + timeout;
			long remainingTimeout = timeout;

			while (numberLines > lines.size() && remainingTimeout > 0) {
				synchronized (lines) {
					try {
						lines.wait(remainingTimeout);
					} catch (InterruptedException e) {
						// ignore interruption exceptions
					}
				}

				remainingTimeout = endTimeout - System.currentTimeMillis();
			}

			if (remainingTimeout <= 0) {
				throw new TimeoutException("Have not received " + numberLines + " in time.");
			}
		}

		public Set<String> getLines() {
			return lines.keySet();
		}

		@Override
		public void run() {
			while (running) {
				try {
					byte[] buffer = new byte[1024];

					DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
					socket.receive(packet);

					String line = new String(packet.getData(), 0, packet.getLength());

					lines.put(line, obj);

					synchronized (lines) {
						lines.notifyAll();
					}
				} catch (IOException ex) {
					// ignore the exceptions
				}
			}
		}
	}
}
