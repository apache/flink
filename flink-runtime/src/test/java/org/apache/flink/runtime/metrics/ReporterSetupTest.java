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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link ReporterSetup}.
 */
public class ReporterSetupTest extends TestLogger {

	/**
	 * TestReporter1 class only for type differentiation.
	 */
	static class TestReporter1 extends TestReporter {
	}

	/**
	 * TestReporter2 class only for type differentiation.
	 */
	static class TestReporter2 extends TestReporter {
	}

	/**
	 * Verifies that a reporter can be configured with all it's arguments being forwarded.
	 */
	@Test
	public void testReporterArgumentForwarding() {
		final Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, ReporterSetupTest.TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter.arg2", "value2");

		final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config);

		Assert.assertEquals(1, reporterSetups.size());

		final ReporterSetup reporterSetup = reporterSetups.get(0);
		Assert.assertEquals("reporter", reporterSetup.getName());
		Assert.assertEquals("value1", reporterSetup.getConfiguration().getString("arg1", null));
		Assert.assertEquals("value2", reporterSetup.getConfiguration().getString("arg2", null));
		Assert.assertEquals(ReporterSetupTest.TestReporter1.class.getName(), reporterSetup.getConfiguration().getString("class", null));
	}

	/**
	 * Verifies that multiple reporters can be configured with all their arguments being forwarded.
	 */
	@Test
	public void testSeveralReportersWithArgumentForwarding() {
		final Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, ReporterSetupTest.TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg2", "value2");

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, ReporterSetupTest.TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg3", "value3");

		final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config);

		Assert.assertEquals(2, reporterSetups.size());

		final Optional<ReporterSetup> reporter1Config = reporterSetups.stream()
			.filter(c -> "reporter1".equals(c.getName()))
			.findFirst();
		Assert.assertTrue(reporter1Config.isPresent());
		Assert.assertEquals("reporter1", reporter1Config.get().getName());
		Assert.assertEquals("value1", reporter1Config.get().getConfiguration().getString("arg1", ""));
		Assert.assertEquals("value2", reporter1Config.get().getConfiguration().getString("arg2", ""));
		Assert.assertEquals(ReporterSetupTest.TestReporter1.class.getName(), reporter1Config.get().getConfiguration().getString("class", null));

		final Optional<ReporterSetup> reporter2Config = reporterSetups.stream()
			.filter(c -> "reporter2".equals(c.getName()))
			.findFirst();
		Assert.assertTrue(reporter1Config.isPresent());
		Assert.assertEquals("reporter2", reporter2Config.get().getName());
		Assert.assertEquals("value1", reporter2Config.get().getConfiguration().getString("arg1", null));
		Assert.assertEquals("value3", reporter2Config.get().getConfiguration().getString("arg3", null));
		Assert.assertEquals(ReporterSetupTest.TestReporter2.class.getName(), reporter2Config.get().getConfiguration().getString("class", null));
	}

	/**
	 * Verifies that {@link MetricOptions#REPORTERS_LIST} is correctly used to filter configured reporters.
	 */
	@Test
	public void testActivateOneReporterAmongTwoDeclared() {
		final Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg2", "value2");

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg3", "value3");

		config.setString(MetricOptions.REPORTERS_LIST, "reporter2");

		final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config);

		Assert.assertEquals(1, reporterSetups.size());

		final ReporterSetup stringConfigurationTuple = reporterSetups.get(0);
		Assert.assertEquals("reporter2", stringConfigurationTuple.getName());
		Assert.assertEquals("value1", stringConfigurationTuple.getConfiguration().getString("arg1", null));
		Assert.assertEquals("value3", stringConfigurationTuple.getConfiguration().getString("arg3", null));
		Assert.assertEquals(TestReporter2.class.getName(), stringConfigurationTuple.getConfiguration().getString("class", null));
	}

	@Test
	public void testReporterSetupSupplier() throws Exception {
		final Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());

		final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config);

		Assert.assertEquals(1, reporterSetups.size());

		final ReporterSetup reporterSetup = reporterSetups.get(0);
		final MetricReporter metricReporter = reporterSetup.getReporter();
		Assert.assertThat(metricReporter, instanceOf(TestReporter1.class));
	}

	/**
	 * Verifies that multiple reporters are instantiated correctly.
	 */
	@Test
	public void testMultipleReporterInstantiation() throws Exception {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter11.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter12.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test3." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter13.class.getName());

		List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config);

		assertEquals(3, reporterSetups.size());

		Assert.assertTrue(TestReporter11.wasOpened);
		Assert.assertTrue(TestReporter12.wasOpened);
		Assert.assertTrue(TestReporter13.wasOpened);
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
}
