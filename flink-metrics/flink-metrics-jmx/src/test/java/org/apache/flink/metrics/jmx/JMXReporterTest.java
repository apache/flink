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

package org.apache.flink.metrics.jmx;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestReporter;
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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		cfg.setString(ConfigConstants.METRICS_REPORTERS_LIST, "test1,test2");

		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1.port", "9020-9035");

		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2.port", "9020-9035");

		MetricRegistry reg = new MetricRegistry(cfg);

		TaskManagerMetricGroup mg = new TaskManagerMetricGroup(reg, "host", "tm");

		List<MetricReporter> reporters = reg.getReporters();

		assertTrue(reporters.size() == 2);

		MetricReporter rep1 = reporters.get(0);
		MetricReporter rep2 = reporters.get(1);

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
		cfg.setString(ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter.class.getName());

		cfg.setString(ConfigConstants.METRICS_REPORTERS_LIST, "test1,test2");

		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1.port", "9040-9055");

		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2.port", "9040-9055");

		MetricRegistry reg = new MetricRegistry(cfg);

		TaskManagerMetricGroup mg = new TaskManagerMetricGroup(reg, "host", "tm");

		List<MetricReporter> reporters = reg.getReporters();

		assertTrue(reporters.size() == 2);

		MetricReporter rep1 = reporters.get(0);
		MetricReporter rep2 = reporters.get(1);

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

		JMXServiceURL url1 = new JMXServiceURL("service:jmx:rmi://localhost:" + ((JMXReporter)rep1).getPort() + "/jndi/rmi://localhost:" + ((JMXReporter)rep1).getPort() + "/jmxrmi");
		JMXConnector jmxCon1 = JMXConnectorFactory.connect(url1);
		MBeanServerConnection mCon1 = jmxCon1.getMBeanServerConnection();

		assertEquals(1, mCon1.getAttribute(objectName1, "Value"));
		assertEquals(2, mCon1.getAttribute(objectName2, "Value"));

		jmxCon1.close();

		JMXServiceURL url2 = new JMXServiceURL("service:jmx:rmi://localhost:" + ((JMXReporter)rep2).getPort() + "/jndi/rmi://localhost:" + ((JMXReporter)rep2).getPort() + "/jmxrmi");
		JMXConnector jmxCon2 = JMXConnectorFactory.connect(url2);
		MBeanServerConnection mCon2 = jmxCon2.getMBeanServerConnection();

		assertEquals(1, mCon2.getAttribute(objectName1, "Value"));
		assertEquals(2, mCon2.getAttribute(objectName2, "Value"));

		jmxCon2.close();

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
			config.setString(ConfigConstants.METRICS_REPORTERS_LIST, "jmx_test");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "jmx_test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());

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

	/**
	 * Tests that meters are properly reported via the JMXReporter.
	 */
	@Test
	public void testMeterReporting() throws Exception {
		MetricRegistry registry = null;
		String meterName = "meter";

		try {
			Configuration config = new Configuration();
			config.setString(ConfigConstants.METRICS_REPORTERS_LIST, "jmx_test");
			config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "jmx_test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());

			registry = new MetricRegistry(config);

			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");

			TestMeter meter = new TestMeter();

			metricGroup.meter(meterName, meter);

			MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

			ObjectName objectName = new ObjectName(JMXReporter.generateJmxName(meterName, metricGroup.getScopeComponents()));

			MBeanInfo info = mBeanServer.getMBeanInfo(objectName);

			MBeanAttributeInfo[] attributeInfos = info.getAttributes();

			assertEquals(2, attributeInfos.length);

			assertEquals(meter.getRate(),  mBeanServer.getAttribute(objectName, "Rate"));
			assertEquals(meter.getCount(), mBeanServer.getAttribute(objectName, "Count"));

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
