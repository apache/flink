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

package org.apache.flink.metrics.reporter;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.metrics.util.TestReporter;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;

public class JMXReporterTest extends TestLogger {

	@Test
	public void testReplaceInvalidChars() {
		assertEquals("", JMXReporter.replaceInvalidChars(""));
		assertEquals("abc", JMXReporter.replaceInvalidChars("abc"));
		assertEquals("abc", JMXReporter.replaceInvalidChars("abc\""));
		assertEquals("abc", JMXReporter.replaceInvalidChars("\"abc"));
		assertEquals("abc", JMXReporter.replaceInvalidChars("\"abc\""));
		assertEquals("abc", JMXReporter.replaceInvalidChars("\"a\"b\"c\""));
		assertEquals("", JMXReporter.replaceInvalidChars("\"\"\"\""));
		assertEquals("____", JMXReporter.replaceInvalidChars("    "));
		assertEquals("ab_-(c)-", JMXReporter.replaceInvalidChars("\"ab ;(c)'"));
		assertEquals("a_b_c", JMXReporter.replaceInvalidChars("a b c"));
		assertEquals("a_b_c_", JMXReporter.replaceInvalidChars("a b c "));
		assertEquals("a-b-c-", JMXReporter.replaceInvalidChars("a;b'c*"));
		assertEquals("a------b------c", JMXReporter.replaceInvalidChars("a,=;:?'b,=;:?'c"));
	}

	/**
	 * Verifies that the JMXReporter properly generates the JMX name.
	 */
	@Test
	public void testGenerateName() {
		String[] scope = {"value0", "value1", "\"value2 (test),=;:?'"};
		String jmxName = JMXReporter.generateJmxName("TestMetric", scope);

		assertEquals("org.apache.flink.metrics:key0=value0,key1=value1,key2=value2_(test)------,name=TestMetric", jmxName);
	}

	/**
	 * Verifies that multiple JMXReporters can be started on the same machine and register metrics at the MBeanServer.
	 *
	 * @throws Exception if the attribute/mbean could not be found or the test is broken
	 */
	@Test
	public void testPortConflictHandling() throws Exception {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.METRICS_REPORTER_CLASS, TestReporter.class.getName());
		MetricRegistry reg = new MetricRegistry(cfg);

		TaskManagerMetricGroup mg = new TaskManagerMetricGroup(reg, "host", "tm");

		JMXReporter rep1 = new JMXReporter();
		JMXReporter rep2 = new JMXReporter();

		Configuration cfg1 = new Configuration();
		cfg1.setString("port", "9020-9035");

		rep1.open(cfg1);
		rep2.open(cfg1);

		rep1.notifyOfAddedMetric(new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 1;
			}
		}, "rep1", new TaskManagerMetricGroup(reg, "host", "tm"));

		rep2.notifyOfAddedMetric(new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 2;
			}
		}, "rep2", new TaskManagerMetricGroup(reg, "host", "tm"));

		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

		ObjectName objectName1 = new ObjectName(JMXReporter.generateJmxName("rep1", mg.getScopeComponents()));
		ObjectName objectName2 = new ObjectName(JMXReporter.generateJmxName("rep2", mg.getScopeComponents()));

		assertEquals(1, mBeanServer.getAttribute(objectName1, "Value"));
		assertEquals(2, mBeanServer.getAttribute(objectName2, "Value"));

		rep1.close();
		rep2.close();
		reg.shutdown();
	}

	/**
	 * Verifies that we can connect to multiple JMXReporters running on the same machine.
	 *
	 * @throws Exception
	 */
	@Test
	public void testJMXAvailability() throws Exception {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.METRICS_REPORTER_CLASS, TestReporter.class.getName());
		MetricRegistry reg = new MetricRegistry(cfg);

		TaskManagerMetricGroup mg = new TaskManagerMetricGroup(reg, "host", "tm");

		JMXReporter rep1 = new JMXReporter();
		JMXReporter rep2 = new JMXReporter();

		Configuration cfg1 = new Configuration();
		cfg1.setString("port", "9040-9055");
		rep1.open(cfg1);
		rep2.open(cfg1);

		rep1.notifyOfAddedMetric(new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 1;
			}
		}, "rep1", new TaskManagerMetricGroup(reg, "host", "tm"));

		rep2.notifyOfAddedMetric(new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 2;
			}
		}, "rep2", new TaskManagerMetricGroup(reg, "host", "tm"));

		ObjectName objectName1 = new ObjectName(JMXReporter.generateJmxName("rep1", mg.getScopeComponents()));
		ObjectName objectName2 = new ObjectName(JMXReporter.generateJmxName("rep2", mg.getScopeComponents()));

		JMXServiceURL url1 = new JMXServiceURL("service:jmx:rmi://localhost:" + rep1.getPort() + "/jndi/rmi://localhost:" + rep1.getPort() + "/jmxrmi");
		JMXConnector jmxCon1 = JMXConnectorFactory.connect(url1);
		MBeanServerConnection mCon1 = jmxCon1.getMBeanServerConnection();

		assertEquals(1, mCon1.getAttribute(objectName1, "Value"));
		assertEquals(2, mCon1.getAttribute(objectName2, "Value"));

		url1 = null;
		jmxCon1.close();
		jmxCon1 = null;
		mCon1 = null;

		JMXServiceURL url2 = new JMXServiceURL("service:jmx:rmi://localhost:" + rep2.getPort() + "/jndi/rmi://localhost:" + rep2.getPort() + "/jmxrmi");
		JMXConnector jmxCon2 = JMXConnectorFactory.connect(url2);
		MBeanServerConnection mCon2 = jmxCon2.getMBeanServerConnection();

		assertEquals(1, mCon2.getAttribute(objectName1, "Value"));
		assertEquals(2, mCon2.getAttribute(objectName2, "Value"));

		url2 = null;
		jmxCon2.close();
		jmxCon2 = null;
		mCon2 = null;

		rep1.close();
		rep2.close();
		reg.shutdown();
	}

	/**
	 * Tests that histograms are properly reported via the JMXReporter.
	 */
	@Test
	public void testHistogramReporting() throws Exception {
		MetricRegistry registry = null;
		String histogramName = "histogram";

		try {
			Configuration config = new Configuration();
			config.setString(ConfigConstants.METRICS_REPORTER_CLASS, "org.apache.flink.metrics.reporter.JMXReporter");

			registry = new MetricRegistry(config);

			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");

			TestingHistogram histogram = new TestingHistogram();

			metricGroup.histogram(histogramName, histogram);

			MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

			ObjectName objectName = new ObjectName(JMXReporter.generateJmxName(histogramName, metricGroup.getScopeComponents()));

			MBeanInfo info = mBeanServer.getMBeanInfo(objectName);

			MBeanAttributeInfo[] attributeInfos = info.getAttributes();

			assertEquals(11, attributeInfos.length);

			assertEquals(histogram.getCount(), mBeanServer.getAttribute(objectName, "Count"));
			assertEquals(histogram.getStatistics().getMean(), mBeanServer.getAttribute(objectName, "Mean"));
			assertEquals(histogram.getStatistics().getStdDev(), mBeanServer.getAttribute(objectName, "StdDev"));
			assertEquals(histogram.getStatistics().getMax(), mBeanServer.getAttribute(objectName, "Max"));
			assertEquals(histogram.getStatistics().getMin(), mBeanServer.getAttribute(objectName, "Min"));
			assertEquals(histogram.getStatistics().getQuantile(0.5), mBeanServer.getAttribute(objectName, "Median"));
			assertEquals(histogram.getStatistics().getQuantile(0.75), mBeanServer.getAttribute(objectName, "75thPercentile"));
			assertEquals(histogram.getStatistics().getQuantile(0.95), mBeanServer.getAttribute(objectName, "95thPercentile"));
			assertEquals(histogram.getStatistics().getQuantile(0.98), mBeanServer.getAttribute(objectName, "98thPercentile"));
			assertEquals(histogram.getStatistics().getQuantile(0.99), mBeanServer.getAttribute(objectName, "99thPercentile"));
			assertEquals(histogram.getStatistics().getQuantile(0.999), mBeanServer.getAttribute(objectName, "999thPercentile"));

		} finally {
			if (registry != null) {
				registry.shutdown();
			}
		}
	}

	static class TestingHistogram implements Histogram {

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
					return 3;
				}

				@Override
				public double getMean() {
					return 4;
				}

				@Override
				public double getStdDev() {
					return 5;
				}

				@Override
				public long getMax() {
					return 6;
				}

				@Override
				public long getMin() {
					return 7;
				}
			};
		}
	}
}
