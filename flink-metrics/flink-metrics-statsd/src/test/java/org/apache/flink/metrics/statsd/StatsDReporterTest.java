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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the StatsDReporter.
 */
public class StatsDReporterTest extends TestLogger {

	@Test
	public void testReplaceInvalidChars() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		StatsDReporter reporter = new StatsDReporter();

		assertEquals("", reporter.filterCharacters(""));
		assertEquals("abc", reporter.filterCharacters("abc"));
		assertEquals("a-b--", reporter.filterCharacters("a:b::"));
	}

	/**
	 * Tests that the registered metrics' names don't contain invalid characters.
	 */
	@Test
	public void testAddingMetrics() throws Exception {
		Configuration configuration = new Configuration();
		String taskName = "testTask";
		String jobName = "testJob:-!ax..?";
		String hostname = "local::host:";
		String taskManagerId = "tas:kMana::ger";
		String counterName = "testCounter";

		configuration.setString(
				ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
				"org.apache.flink.metrics.statsd.StatsDReporterTest$TestingStatsDReporter");

		configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");
		configuration.setString(MetricOptions.SCOPE_DELIMITER, "_");

		MetricRegistryImpl metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration));

		char delimiter = metricRegistry.getDelimiter();

		TaskManagerMetricGroup tmMetricGroup = new TaskManagerMetricGroup(metricRegistry, hostname, taskManagerId);
		TaskManagerJobMetricGroup tmJobMetricGroup = new TaskManagerJobMetricGroup(metricRegistry, tmMetricGroup, new JobID(), jobName);
		TaskMetricGroup taskMetricGroup = new TaskMetricGroup(metricRegistry, tmJobMetricGroup, new JobVertexID(), new AbstractID(), taskName, 0, 0);

		SimpleCounter myCounter = new SimpleCounter();

		taskMetricGroup.counter(counterName, myCounter);

		List<MetricReporter> reporters = metricRegistry.getReporters();

		assertTrue(reporters.size() == 1);

		MetricReporter metricReporter = reporters.get(0);

		assertTrue("Reporter should be of type StatsDReporter", metricReporter instanceof StatsDReporter);

		TestingStatsDReporter reporter = (TestingStatsDReporter) metricReporter;

		Map<Counter, String> counters = reporter.getCounters();

		assertTrue(counters.containsKey(myCounter));

		String expectedCounterName = reporter.filterCharacters(hostname)
			+ delimiter
			+ reporter.filterCharacters(taskManagerId)
			+ delimiter
			+ reporter.filterCharacters(jobName)
			+ delimiter
			+ reporter.filterCharacters(counterName);

		assertEquals(expectedCounterName, counters.get(myCounter));

		metricRegistry.shutdown().get();
	}

	/**
	 * Tests that histograms are properly reported via the StatsD reporter.
	 */
	@Test
	public void testStatsDHistogramReporting() throws Exception {
		MetricRegistryImpl registry = null;
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
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "1 SECONDS");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);

			registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");

			TestingHistogram histogram = new TestingHistogram();

			metricGroup.histogram(histogramName, histogram);

			receiver.waitUntilNumLines(11, timeout);

			Set<String> lines = receiver.getLines();

			String prefix = metricGroup.getMetricIdentifier(histogramName);

			Set<String> expectedLines = new HashSet<>();

			expectedLines.add(prefix + ".count:1|g");
			expectedLines.add(prefix + ".mean:3.0|g");
			expectedLines.add(prefix + ".min:6|g");
			expectedLines.add(prefix + ".max:5|g");
			expectedLines.add(prefix + ".stddev:4.0|g");
			expectedLines.add(prefix + ".p75:0.75|g");
			expectedLines.add(prefix + ".p98:0.98|g");
			expectedLines.add(prefix + ".p99:0.99|g");
			expectedLines.add(prefix + ".p999:0.999|g");
			expectedLines.add(prefix + ".p95:0.95|g");
			expectedLines.add(prefix + ".p50:0.5|g");

			assertEquals(expectedLines, lines);

		} finally {
			if (registry != null) {
				registry.shutdown().get();
			}

			if (receiver != null) {
				receiver.stop();
			}

			if (receiverThread != null) {
				receiverThread.join(joinTimeout);
			}
		}
	}

	/**
	 * Tests that meters are properly reported via the StatsD reporter.
	 */
	@Test
	public void testStatsDMetersReporting() throws Exception {
		MetricRegistryImpl registry = null;
		DatagramSocketReceiver receiver = null;
		Thread receiverThread = null;
		long timeout = 5000;
		long joinTimeout = 30000;

		String meterName = "meter";

		try {
			receiver = new DatagramSocketReceiver();

			receiverThread = new Thread(receiver);

			receiverThread.start();

			int port = receiver.getPort();

			Configuration config = new Configuration();
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "1 SECONDS");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);

			registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));
			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");
			TestMeter meter = new TestMeter();
			metricGroup.meter(meterName, meter);
			String prefix = metricGroup.getMetricIdentifier(meterName);

			Set<String> expectedLines = new HashSet<>();

			expectedLines.add(prefix + ".rate:5.0|g");
			expectedLines.add(prefix + ".count:100|g");

			receiver.waitUntilNumLines(expectedLines.size(), timeout);

			Set<String> lines = receiver.getLines();

			assertEquals(expectedLines, lines);

		} finally {
			if (registry != null) {
				registry.shutdown().get();
			}

			if (receiver != null) {
				receiver.stop();
			}

			if (receiverThread != null) {
				receiverThread.join(joinTimeout);
			}
		}
	}

	/**
	 * Testing StatsDReporter which disables the socket creation.
	 */
	public static class TestingStatsDReporter extends StatsDReporter {
		@Override
		public void open(MetricConfig configuration) {
			// disable the socket creation
		}

		public Map<Counter, String> getCounters() {
			return counters;
		}
	}

	private static class TestingHistogram implements Histogram {

		@Override
		public void update(long value) {

		}

		@Override
		public long getCount() {
			return 1;
		}

		@Override
		public HistogramStatistics getStatistics() {
			return new HistogramStatistics() {
				@Override
				public double getQuantile(double quantile) {
					return quantile;
				}

				@Override
				public long[] getValues() {
					return new long[0];
				}

				@Override
				public int size() {
					return 2;
				}

				@Override
				public double getMean() {
					return 3;
				}

				@Override
				public double getStdDev() {
					return 4;
				}

				@Override
				public long getMax() {
					return 5;
				}

				@Override
				public long getMin() {
					return 6;
				}
			};
		}
	}

	private static class DatagramSocketReceiver implements Runnable {
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

					String line = new String(packet.getData(), 0, packet.getLength(), ConfigConstants.DEFAULT_CHARSET);

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
