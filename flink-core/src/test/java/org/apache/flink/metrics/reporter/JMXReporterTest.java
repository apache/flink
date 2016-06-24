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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.*;

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
		String[] scope = { "value0", "value1", "\"value2 (test),=;:?'" };
		String jmxName = JMXReporter.generateJmxName("TestMetric", scope);

		assertEquals("org.apache.flink.metrics:key0=value0,key1=value1,key2=value2_(test)------,name=TestMetric", jmxName);
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
