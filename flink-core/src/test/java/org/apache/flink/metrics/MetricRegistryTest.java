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

package org.apache.flink.metrics;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.AbstractMetricGroup;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.metrics.groups.scope.ScopeFormats;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.metrics.util.TestReporter;

import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricRegistryTest extends TestLogger {
	
	/**
	 * Verifies that the reporter class argument is correctly used to instantiate and open the reporter.
	 */
	@Test
	public void testReporterInstantiation() {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_CLASS, TestReporter1.class.getName());

		new MetricRegistry(config);

		Assert.assertTrue(TestReporter1.wasOpened);
	}

	protected static class TestReporter1 extends TestReporter {
		public static boolean wasOpened = false;

		@Override
		public void open(Configuration config) {
			wasOpened = true;
		}
	}

	/**
	 * Verifies that configured arguments are properly forwarded to the reporter.
	 */
	@Test
	public void testReporterArgumentForwarding() {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_CLASS, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_ARGUMENTS, "--arg1 hello --arg2 world");

		new MetricRegistry(config);
	}

	protected static class TestReporter2 extends TestReporter {
		@Override
		public void open(Configuration config) {
			Assert.assertEquals("hello", config.getString("arg1", null));
			Assert.assertEquals("world", config.getString("arg2", null));
		}
	}

	/**
	 * Verifies that reporters implementing the Scheduled interface are regularly called to report the metrics.
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void testReporterScheduling() throws InterruptedException {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_CLASS, TestReporter3.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_INTERVAL, "50 MILLISECONDS");

		new MetricRegistry(config);

		long start = System.currentTimeMillis();
		for (int x = 0; x < 10; x++) {
			Thread.sleep(100);
			int reportCount = TestReporter3.reportCount;
			long curT = System.currentTimeMillis();
			/**
			 * Within a given time-frame T only T/500 reports may be triggered due to the interval between reports. 
			 * This value however does not not take the first triggered report into account (=> +1). 
			 * Furthermore we have to account for the mis-alignment between reports being triggered and our time 
			 * measurement (=> +1); for T=200 a total of 4-6 reports may have been
			 * triggered depending on whether the end of the interval for the first reports ends before
			 * or after T=50.
			 */
			long maxAllowedReports = (curT - start) / 50 + 2;
			Assert.assertTrue("Too many report were triggered.", maxAllowedReports >= reportCount);
		}
		Assert.assertTrue("No report was triggered.", TestReporter3.reportCount > 0);
	}

	protected static class TestReporter3 extends TestReporter implements Scheduled {
		public static int reportCount = 0;

		@Override
		public void report() {
			reportCount++;
		}
	}

	/**
	 * Verifies that reporters implementing the Listener interface are notified when Metrics are added or removed.
	 */
	@Test
	public void testListener() {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTER_CLASS, TestReporter6.class.getName());

		MetricRegistry registry = new MetricRegistry(config);

		MetricGroup root = new TaskManagerMetricGroup(registry, "host", "id");
		root.counter("rootCounter");
		root.close();

		assertTrue(TestReporter6.addCalled);
		assertTrue(TestReporter6.removeCalled);
	}

	protected static class TestReporter6 extends TestReporter {
		public static boolean addCalled = false;
		public static boolean removeCalled = false;

		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, AbstractMetricGroup group) {
			addCalled = true;
			assertTrue(metric instanceof Counter);
			assertEquals("rootCounter", metricName);
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String metricName, AbstractMetricGroup group) {
			removeCalled = true;
			Assert.assertTrue(metric instanceof Counter);
			Assert.assertEquals("rootCounter", metricName);
		}
	}

	/**
	 * Verifies that the scope configuration is properly extracted.
	 */
	@Test
	public void testScopeConfig() {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "A");
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM_JOB, "B");
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TASK, "C");
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_OPERATOR, "D");

		ScopeFormats scopeConfig = MetricRegistry.createScopeConfig(config);

		assertEquals("A", scopeConfig.getTaskManagerFormat().format());
		assertEquals("B", scopeConfig.getTaskManagerJobFormat().format());
		assertEquals("C", scopeConfig.getTaskFormat().format());
		assertEquals("D", scopeConfig.getOperatorFormat().format());
	}

	@Test
	public void testConfigurableDelimiter() {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_SCOPE_DELIMITER, "_");
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "A.B.C.D.E");

		MetricRegistry registry = new MetricRegistry(config);

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "host", "id");
		assertEquals("A_B_C_D_E_name", tmGroup.getMetricIdentifier("name"));

		registry.shutdown();
	}
}
