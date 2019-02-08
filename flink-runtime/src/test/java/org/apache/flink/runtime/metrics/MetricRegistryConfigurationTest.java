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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * Tests for the {@link MetricRegistryConfiguration}.
 */
public class MetricRegistryConfigurationTest extends TestLogger {

	/**
	 * TestReporter1 class only for type differentiation.
	 */
	private static class TestReporter1 extends TestReporter {
	}

	/**
	 * TestReporter2 class only for type differentiation.
	 */
	private static class TestReporter2 extends TestReporter {
	}

	/**
	 * Verifies that a reporter can be configured with all it's arguments being forwarded.
	 */
	@Test
	public void testReporterArgumentForwarding() {
		final Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter.arg2", "value2");

		final MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(config);

		Assert.assertEquals(1, metricRegistryConfiguration.getReporterConfigurations().size());

		final Tuple2<String, Configuration> stringConfigurationTuple = metricRegistryConfiguration.getReporterConfigurations().get(0);
		Assert.assertEquals("reporter", stringConfigurationTuple.f0);
		Assert.assertEquals("value1", stringConfigurationTuple.f1.getString("arg1", null));
		Assert.assertEquals("value2", stringConfigurationTuple.f1.getString("arg2", null));
		Assert.assertEquals(TestReporter1.class.getName(), stringConfigurationTuple.f1.getString("class", null));
	}

	/**
	 * Verifies that multiple reporters can be configured with all their arguments being forwarded.
	 */
	@Test
	public void testSeveralReportersWithArgumentForwarding() {
		final Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg2", "value2");

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg1", "value1");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg3", "value3");

		final MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(config);

		Assert.assertEquals(2, metricRegistryConfiguration.getReporterConfigurations().size());

		final Optional<Tuple2<String, Configuration>> reporter1Config = metricRegistryConfiguration.getReporterConfigurations().stream()
			.filter(c -> "reporter1".equals(c.f0))
			.findFirst();
		Assert.assertTrue(reporter1Config.isPresent());
		Assert.assertEquals("reporter1", reporter1Config.get().f0);
		Assert.assertEquals("value1", reporter1Config.get().f1.getString("arg1", ""));
		Assert.assertEquals("value2", reporter1Config.get().f1.getString("arg2", ""));
		Assert.assertEquals(TestReporter1.class.getName(), reporter1Config.get().f1.getString("class", null));

		final Optional<Tuple2<String, Configuration>> reporter2Config = metricRegistryConfiguration.getReporterConfigurations().stream()
			.filter(c -> "reporter2".equals(c.f0))
			.findFirst();
		Assert.assertTrue(reporter1Config.isPresent());
		Assert.assertEquals("reporter2", reporter2Config.get().f0);
		Assert.assertEquals("value1", reporter2Config.get().f1.getString("arg1", null));
		Assert.assertEquals("value3", reporter2Config.get().f1.getString("arg3", null));
		Assert.assertEquals(TestReporter2.class.getName(), reporter2Config.get().f1.getString("class", null));
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

		final MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(config);

		Assert.assertEquals(1, metricRegistryConfiguration.getReporterConfigurations().size());

		final Tuple2<String, Configuration> stringConfigurationTuple = metricRegistryConfiguration.getReporterConfigurations().get(0);
		Assert.assertEquals("reporter2", stringConfigurationTuple.f0);
		Assert.assertEquals("reporter2", stringConfigurationTuple.f0);
		Assert.assertEquals("value1", stringConfigurationTuple.f1.getString("arg1", null));
		Assert.assertEquals("value3", stringConfigurationTuple.f1.getString("arg3", null));
		Assert.assertEquals(TestReporter2.class.getName(), stringConfigurationTuple.f1.getString("class", null));
	}
}
