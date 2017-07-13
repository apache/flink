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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
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
import java.util.HashMap;
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
		String triggerWindowName = "TriggerWindow(TumblingProcessingTimeWindows(5000), ReducingStateDescriptor{serializer=org.apache.flink.api.java.typeutils.runtime.PojoSerializer@f3395ffa, reduceFunction=org.apache.flink.streaming.examples.socket.SocketWindowWordCount$1@4201c465}, ProcessingTimeTrigger(), WindowedStream.reduce(WindowedStream.java-301))";
		String triggerWindowAltInstance = triggerWindowName.replace("@f3395ffa", "@f3395ffb").replace("@4201c465", "@564c1024");

		assertEquals("", reporter.filterCharacters(""));
		assertEquals("abc", reporter.filterCharacters("abc"));
		assertEquals("a_b", reporter.filterCharacters("a:b::"));
		assertEquals("metric_name", reporter.filterCharacters(" (metric -> name) "));
		assertEquals("TriggerWin_c2910b88", reporter.filterCharacters(triggerWindowName));
		assertEquals("TriggerWin_c2910b88", reporter.filterCharacters(triggerWindowAltInstance));
	}

	@Test
	public void testShortIds() {
		// set up a configured reporter:
		Configuration config = new Configuration();
		config.setString(MetricOptions.REPORTERS_LIST, "test");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.shortids", "true");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + 8125);

		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));
		TaskManagerMetricGroup ignored = new TaskManagerMetricGroup(registry, "localhost", "tmId");
		StatsDReporter reporter = (StatsDReporter) registry.getReporters().get(0);

		assertEquals("0c3d7952", reporter.filterCharacters("0c3d7952a3c76c029ecf80ef239d475b"));
		assertEquals("ad25ca60", reporter.filterCharacters("ad25ca6074f1ec1a20030e9b9e11c476"));
		assertEquals("this_isn_t_an_id_but_same_length", reporter.filterCharacters("this_isn_t_an_id_but_same_length"));

		registry.shutdown();
	}

	/**
	 * Tests that the registered metrics' names don't contain invalid characters.
	 */
	@Test
	public void testAddingMetrics() throws NoSuchFieldException, IllegalAccessException {
		Configuration configuration = new Configuration();
		String taskName = "testTask";
		String jobName = "testJob:-!ax..?";
		String hostname = "local::host:";
		String taskManagerId = "tas:kMana::ger";
		String counterName = "testCounter";

		configuration.setString(MetricOptions.REPORTERS_LIST, "test");
		configuration.setString(
				ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
				"org.apache.flink.metrics.statsd.StatsDReporterTest$TestingStatsDReporter");

		configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");
		configuration.setString(MetricOptions.SCOPE_DELIMITER, "_");

		MetricRegistry metricRegistry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(configuration));

		char delimiter = metricRegistry.getDelimiter();

		TaskManagerMetricGroup tmMetricGroup = new TaskManagerMetricGroup(metricRegistry, hostname, taskManagerId);
		TaskManagerJobMetricGroup tmJobMetricGroup = new TaskManagerJobMetricGroup(metricRegistry, tmMetricGroup, new JobID(), jobName);
		TaskMetricGroup taskMetricGroup = new TaskMetricGroup(metricRegistry, tmJobMetricGroup, new AbstractID(), new AbstractID(), taskName, 0, 0);

		SimpleCounter myCounter = new SimpleCounter();

		taskMetricGroup.counter(counterName, myCounter);

		List<MetricReporter> reporters = metricRegistry.getReporters();

		assertTrue(reporters.size() == 1);

		MetricReporter metricReporter = reporters.get(0);

		assertTrue("Reporter should be of type StatsDReporter", metricReporter instanceof StatsDReporter);

		TestingStatsDReporter reporter = (TestingStatsDReporter) metricReporter;

		Map<Counter, StatsDReporter.TaggedMetric> counters = reporter.getCounters();

		assertTrue(counters.containsKey(myCounter));

		String expectedCounterName = reporter.filterCharacters(hostname)
			+ delimiter
			+ reporter.filterCharacters(taskManagerId)
			+ delimiter
			+ reporter.filterCharacters(jobName)
			+ delimiter
			+ reporter.filterCharacters(counterName);

		assertEquals(expectedCounterName, counters.get(myCounter).getName());

		metricRegistry.shutdown();
	}

	/**
	 * Tests that statsd lines are valid.
	 */
	@Test
	public void testIgnoreInvalidValues() throws Exception {
		MetricRegistry registry = null;
		DatagramSocketReceiver receiver = null;
		Thread receiverThread = null;
		long timeout = 5000;
		long joinTimeout = 30000;

		try {
			receiver = new DatagramSocketReceiver();

			receiverThread = new Thread(receiver);

			receiverThread.start();

			int port = receiver.getPort();

			Configuration config = new Configuration();
			config.setString(MetricOptions.REPORTERS_LIST, "test");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "1 SECONDS");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);

			registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));

			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");

			Gauge<Integer> negOne = new Gauge<Integer>() {
				@Override
				public Integer getValue() {
					return -1;
				}
			};
			Gauge <Long> negMin = new Gauge<Long>() {
				@Override
				public Long getValue() {
					return Long.MIN_VALUE;
				}
			};
			Gauge <String> na = new Gauge<String>() {
				@Override
				public String getValue() {
					return "n/a";
				}
			};
			Gauge <Double> nan = new Gauge<Double>() {
				@Override
				public Double getValue() {
					return Double.NaN;
				}
			};
			Gauge <Double> valid = new Gauge<Double>() {
				@Override
				public Double getValue() {
					return 1.23;
				}
			};

			// these should be ignored
			metricGroup.gauge("negOne", negOne);
			metricGroup.gauge("negMin", negMin);
			metricGroup.gauge("na", na);
			metricGroup.gauge("nan", nan);

			// this should be the only received metric
			metricGroup.gauge("valid", valid);

			receiver.waitUntilNumLines(1, timeout);
			Set<String> lines = receiver.getLines();

			Set<String> expectedLines = new HashSet<>();
			expectedLines.add(metricGroup.getMetricIdentifier("valid") + ":1.23|g");

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

	@Test
	public void testTags() throws Exception {
		MetricRegistry registry = null;
		DatagramSocketReceiver receiver = null;
		Thread receiverThread = null;
		long timeout = 5000;
		long joinTimeout = 30000;

		try {
			receiver = new DatagramSocketReceiver();

			receiverThread = new Thread(receiver);

			receiverThread.start();

			int port = receiver.getPort();

			Configuration config = new Configuration();
			config.setString(MetricOptions.REPORTERS_LIST, "test");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "1 SECONDS");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.dogstatsd", "true");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.shortids", "true");

			registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));

			JobID jobID = new JobID();
			AbstractID taskId = new AbstractID();
			AbstractID attemptId = new AbstractID();

			TaskManagerMetricGroup mg = new TaskManagerMetricGroup(registry, "hostName", "taskManagerId");
			TaskManagerJobMetricGroup jmg = new TaskManagerJobMetricGroup(registry, mg, jobID, "jobName");
			TaskMetricGroup tmg = new TaskMetricGroup(registry, jmg, taskId, attemptId, "taskName", 1, 2);
			OperatorMetricGroup omg = new OperatorMetricGroup(registry, tmg, "operatorName");

			Gauge <Double> sample = new Gauge<Double>() {
				@Override
				public Double getValue() {
					return 1.23;
				}
			};

			omg.gauge("sample", sample);

			receiver.waitUntilNumLines(22, timeout);
			Set<String> lines = receiver.getLines();

			Map<String, String> expectedTags = new HashMap<>();
			expectedTags.put("host", "hostName");
			expectedTags.put("job_id", jobID.toString().substring(0, 8));
			expectedTags.put("job_name", "jobName");
			expectedTags.put("operator_name", "operatorName");
			expectedTags.put("subtask_index", "1");
			expectedTags.put("task_attempt_id", attemptId.toString().substring(0, 8));
			expectedTags.put("task_attempt_num", "2");
			expectedTags.put("task_id", taskId.toString().substring(0, 8));
			expectedTags.put("task_name", "taskName");
			expectedTags.put("tm_id", "taskManagerId");

			String expectedName = "flink.taskmanager.job.task.operator.sample";

			Boolean gotName = false;
			for (String line : lines) {
				if (line.startsWith(expectedName + ':')) {
					gotName = true;
					for (Map.Entry<String, String> tag: expectedTags.entrySet()) {
						String tagLine = tag.getKey() + ':' + tag.getValue();
						assertTrue("expecting to find " + tagLine + " in " + line, line.contains(tagLine));
					}
					break;
				}
			}
			assertTrue("expecting to find " + expectedName, gotName);

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


	/**
	 * Tests that latency is properly reported via the StatsD reporter.
	 */
	@Test
	public void testStatsDLatencyReporting() throws Exception {
		MetricRegistry registry = null;
		DatagramSocketReceiver receiver = null;
		Thread receiverThread = null;
		long timeout = 5000;
		long joinTimeout = 30000;

		String latencyName = "latency";

		try {
			receiver = new DatagramSocketReceiver();

			receiverThread = new Thread(receiver);

			receiverThread.start();

			int port = receiver.getPort();

			Configuration config = new Configuration();
			config.setString(MetricOptions.REPORTERS_LIST, "test");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "1 SECONDS");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);

			registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));

			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");

			LatencyGauge latency = new LatencyGauge();

			metricGroup.gauge(latencyName, latency);

			receiver.waitUntilNumLines(6, timeout);

			Set<String> lines = receiver.getLines();

			String prefix = metricGroup.getMetricIdentifier(latencyName);

			Set<String> expectedLines = new HashSet<>();

			expectedLines.add(prefix + ".min:1.0|g");
			expectedLines.add(prefix + ".mean:51.0|g");
			expectedLines.add(prefix + ".p50:50.0|g");
			expectedLines.add(prefix + ".p95:95.0|g");
			expectedLines.add(prefix + ".p99:99.0|g");
			expectedLines.add(prefix + ".max:999.0|g");

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


	/**
	 * Tests that histograms are properly reported via the StatsD reporter.
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
			config.setString(MetricOptions.REPORTERS_LIST, "test");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "1 SECONDS");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);

			registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));

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

	/**
	 * Tests that meters are properly reported via the StatsD reporter.
	 */
	@Test
	public void testStatsDMetersReporting() throws Exception {
		MetricRegistry registry = null;
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
			config.setString(MetricOptions.REPORTERS_LIST, "test");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, StatsDReporter.class.getName());
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "1 SECONDS");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);

			registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));
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

	/**
	 * Testing StatsDReporter which disables the socket creation.
	 */
	public static class TestingStatsDReporter extends StatsDReporter {
		@Override
		public void open(MetricConfig configuration) {
			// disable the socket creation
		}

		public Map<Counter, TaggedMetric> getCounters() {
			return counters;
		}
	}

	/**
	 * Imitate a LatencyGauge.
	 * eg: {LatencySourceDescriptor{vertexID=1, subtaskIndex=-1}={p99=79.0, p50=79.0, min=79.0, max=79.0, p95=79.0, mean=79.0}}
	 */
	public static class LatencyGauge implements Gauge<Map<String, HashMap<String, Double>>> {
		@Override
		public Map<String, HashMap<String, Double>> getValue() {
			Map<String, HashMap<String, Double>> ret = new HashMap<>();
			HashMap<String, Double> v = new HashMap<>();
			v.put("max", 999.);
			v.put("mean", 51.);
			v.put("min", 1.);
			v.put("p50", 50.);
			v.put("p95", 95.);
			v.put("p99", 99.);
			ret.put("{LatencySourceDescriptor{vertexID=1, subtaskIndex=-1}", v);
			return ret;
		}
	}

	/**
	 * TestingHistogram.
	 */
	public static class TestingHistogram implements Histogram {

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
