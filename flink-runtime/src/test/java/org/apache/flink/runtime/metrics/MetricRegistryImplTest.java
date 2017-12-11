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

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorNotFound;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link MetricRegistryImpl}.
 */
public class MetricRegistryImplTest extends TestLogger {

	private static final char GLOBAL_DEFAULT_DELIMITER = '.';

	@Test
	public void testIsShutdown() {
		MetricRegistryImpl metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		Assert.assertFalse(metricRegistry.isShutdown());

		metricRegistry.shutdown();

		Assert.assertTrue(metricRegistry.isShutdown());
	}

	/**
	 * Verifies that the reporter name list is correctly used to determine which reporters should be instantiated.
	 */
	@Test
	public void testReporterInclusion() {
		Configuration config = new Configuration();

		config.setString(MetricOptions.REPORTERS_LIST, "test");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter11.class.getName());

		MetricRegistryImpl metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		assertTrue(metricRegistry.getReporters().size() == 1);

		Assert.assertTrue(TestReporter1.wasOpened);
		Assert.assertFalse(TestReporter11.wasOpened);

		metricRegistry.shutdown();
	}

	/**
	 * Reporter that exposes whether open() was called.
	 */
	protected static class TestReporter1 extends TestReporter {
		public static boolean wasOpened = false;

		@Override
		public void open(MetricConfig config) {
			wasOpened = true;
		}
	}

	/**
	 * Verifies that multiple reporters are instantiated correctly.
	 */
	@Test
	public void testMultipleReporterInstantiation() {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter11.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter12.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test3." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter13.class.getName());

		MetricRegistryImpl metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		assertTrue(metricRegistry.getReporters().size() == 3);

		Assert.assertTrue(TestReporter11.wasOpened);
		Assert.assertTrue(TestReporter12.wasOpened);
		Assert.assertTrue(TestReporter13.wasOpened);

		metricRegistry.shutdown();
	}

	/**
	 * Reporter that exposes whether open() was called.
	 */
	protected static class TestReporter11 extends TestReporter {
		public static boolean wasOpened = false;

		@Override
		public void open(MetricConfig config) {
			wasOpened = true;
		}
	}

	/**
	 * Reporter that exposes whether open() was called.
	 */
	protected static class TestReporter12 extends TestReporter {
		public static boolean wasOpened = false;

		@Override
		public void open(MetricConfig config) {
			wasOpened = true;
		}
	}

	/**
	 * Reporter that exposes whether open() was called.
	 */
	protected static class TestReporter13 extends TestReporter {
		public static boolean wasOpened = false;

		@Override
		public void open(MetricConfig config) {
			wasOpened = true;
		}
	}

	/**
	 * Verifies that configured arguments are properly forwarded to the reporter.
	 */
	@Test
	public void testReporterArgumentForwarding() {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.arg1", "hello");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.arg2", "world");

		new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config)).shutdown();

		Assert.assertEquals("hello", TestReporter2.mc.getString("arg1", null));
		Assert.assertEquals("world", TestReporter2.mc.getString("arg2", null));
	}

	/**
	 * Reporter that exposes the {@link MetricConfig} it was given.
	 */
	protected static class TestReporter2 extends TestReporter {
		static MetricConfig mc;
		@Override
		public void open(MetricConfig config) {
			mc = config;
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

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter3.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.arg1", "hello");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "50 MILLISECONDS");

		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		long start = System.currentTimeMillis();

		// only start counting from now on
		TestReporter3.reportCount = 0;

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
			Assert.assertTrue("Too many reports were triggered.", maxAllowedReports >= reportCount);
		}
		Assert.assertTrue("No report was triggered.", TestReporter3.reportCount > 0);

		registry.shutdown();
	}

	/**
	 * Reporter that exposes how often report() was called.
	 */
	protected static class TestReporter3 extends TestReporter implements Scheduled {
		public static int reportCount = 0;

		@Override
		public void report() {
			reportCount++;
		}
	}

	/**
	 * Verifies that reporters are notified of added/removed metrics.
	 */
	@Test
	public void testReporterNotifications() {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter6.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter7.class.getName());

		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		TaskManagerMetricGroup root = new TaskManagerMetricGroup(registry, "host", "id");
		root.counter("rootCounter");

		assertTrue(TestReporter6.addedMetric instanceof Counter);
		assertEquals("rootCounter", TestReporter6.addedMetricName);

		assertTrue(TestReporter7.addedMetric instanceof Counter);
		assertEquals("rootCounter", TestReporter7.addedMetricName);

		root.close();

		assertTrue(TestReporter6.removedMetric instanceof Counter);
		assertEquals("rootCounter", TestReporter6.removedMetricName);

		assertTrue(TestReporter7.removedMetric instanceof Counter);
		assertEquals("rootCounter", TestReporter7.removedMetricName);

		registry.shutdown();
	}

	/**
	 * Reporter that exposes the name and metric instance of the last metric that was added or removed.
	 */
	protected static class TestReporter6 extends TestReporter {
		static Metric addedMetric;
		static String addedMetricName;

		static Metric removedMetric;
		static String removedMetricName;

		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			addedMetric = metric;
			addedMetricName = metricName;
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
			removedMetric = metric;
			removedMetricName = metricName;
		}
	}

	/**
	 * Reporter that exposes the name and metric instance of the last metric that was added or removed.
	 */
	protected static class TestReporter7 extends TestReporter {
		static Metric addedMetric;
		static String addedMetricName;

		static Metric removedMetric;
		static String removedMetricName;

		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			addedMetric = metric;
			addedMetricName = metricName;
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
			removedMetric = metric;
			removedMetricName = metricName;
		}
	}

	/**
	 * Verifies that the scope configuration is properly extracted.
	 */
	@Test
	public void testScopeConfig() {
		Configuration config = new Configuration();

		config.setString(MetricOptions.SCOPE_NAMING_TM, "A");
		config.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
		config.setString(MetricOptions.SCOPE_NAMING_TASK, "C");
		config.setString(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

		ScopeFormats scopeConfig = ScopeFormats.fromConfig(config);

		assertEquals("A", scopeConfig.getTaskManagerFormat().format());
		assertEquals("B", scopeConfig.getTaskManagerJobFormat().format());
		assertEquals("C", scopeConfig.getTaskFormat().format());
		assertEquals("D", scopeConfig.getOperatorFormat().format());
	}

	@Test
	public void testConfigurableDelimiter() {
		Configuration config = new Configuration();
		config.setString(MetricOptions.SCOPE_DELIMITER, "_");
		config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D.E");

		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "host", "id");
		assertEquals("A_B_C_D_E_name", tmGroup.getMetricIdentifier("name"));

		registry.shutdown();
	}

	@Test
	public void testConfigurableDelimiterForReporters() {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "_");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "-");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test3." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "AA");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test3." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter.class.getName());

		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter());
		assertEquals('_', registry.getDelimiter(0));
		assertEquals('-', registry.getDelimiter(1));
		assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter(2));
		assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter(3));
		assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter(-1));

		registry.shutdown();
	}

	@Test
	public void testConfigurableDelimiterForReportersInGroup() {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "_");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter8.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "-");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter8.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test3." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "AA");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test3." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter8.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test4." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter8.class.getName());
		config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B");

		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));
		List<MetricReporter> reporters = registry.getReporters();
		((TestReporter8) reporters.get(0)).expectedDelimiter = '_'; //test1  reporter
		((TestReporter8) reporters.get(1)).expectedDelimiter = '-'; //test2 reporter
		((TestReporter8) reporters.get(2)).expectedDelimiter = GLOBAL_DEFAULT_DELIMITER; //test3 reporter, because 'AA' - not correct delimiter
		((TestReporter8) reporters.get(3)).expectedDelimiter = GLOBAL_DEFAULT_DELIMITER; //for test4 reporter use global delimiter

		TaskManagerMetricGroup group = new TaskManagerMetricGroup(registry, "host", "id");
		group.counter("C");
		group.close();
		registry.shutdown();
		assertEquals(4, TestReporter8.numCorrectDelimitersForRegister);
		assertEquals(4, TestReporter8.numCorrectDelimitersForUnregister);
	}

	/**
	 * Tests that the query actor will be stopped when the MetricRegistry is shut down.
	 */
	@Test
	public void testQueryActorShutdown() throws Exception {
		final FiniteDuration timeout = new FiniteDuration(10L, TimeUnit.SECONDS);

		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		final ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

		registry.startQueryService(actorSystem, null);

		ActorRef queryServiceActor = registry.getQueryService();

		registry.shutdown();

		try {
			Await.result(actorSystem.actorSelection(queryServiceActor.path()).resolveOne(timeout), timeout);

			fail("The query actor should be terminated resulting in a ActorNotFound exception.");
		} catch (ActorNotFound e) {
			// we expect the query actor to be shut down
		}
	}

	/**
	 * Reporter that verifies that the configured delimiter is applied correctly when generating the metric identifier.
	 */
	public static class TestReporter8 extends TestReporter {
		char expectedDelimiter;
		public static int numCorrectDelimitersForRegister = 0;
		public static int numCorrectDelimitersForUnregister = 0;

		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			String expectedMetric = "A" + expectedDelimiter + "B" + expectedDelimiter + "C";
			assertEquals(expectedMetric, group.getMetricIdentifier(metricName, this));
			assertEquals(expectedMetric, group.getMetricIdentifier(metricName));
			numCorrectDelimitersForRegister++;
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
			String expectedMetric = "A" + expectedDelimiter + "B" + expectedDelimiter + "C";
			assertEquals(expectedMetric, group.getMetricIdentifier(metricName, this));
			assertEquals(expectedMetric, group.getMetricIdentifier(metricName));
			numCorrectDelimitersForUnregister++;
		}
	}

	@Test
	public void testExceptionIsolation() throws Exception {

		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, FailingReporter.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter7.class.getName());

		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		Counter metric = new SimpleCounter();
		registry.register(metric, "counter", new MetricGroupTest.DummyAbstractMetricGroup(registry));

		assertEquals(metric, TestReporter7.addedMetric);
		assertEquals("counter", TestReporter7.addedMetricName);

		registry.unregister(metric, "counter", new MetricGroupTest.DummyAbstractMetricGroup(registry));

		assertEquals(metric, TestReporter7.removedMetric);
		assertEquals("counter", TestReporter7.removedMetricName);

		registry.shutdown();
	}

	/**
	 * Reporter that throws an exception when it is notified of an added or removed metric.
	 */
	protected static class FailingReporter extends TestReporter {
		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			throw new RuntimeException();
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
			throw new RuntimeException();
		}
	}
}
