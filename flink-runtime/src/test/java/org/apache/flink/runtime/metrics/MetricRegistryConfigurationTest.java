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
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * Tests for the {@link MetricRegistryConfigurationTest}.
 */
public class MetricRegistryConfigurationTest {

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
	 * Verifies that a reporter is properly parse with all his arguments forwarded.
	 */
	@Test
	public void testReporterArgumentForwarding() {
		Configuration config = new Configuration();

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.arg1", "hello");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.arg2", "world");

		MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(config);

		Assert.assertEquals(1, metricRegistryConfiguration.getReporterConfigurations().size());

		Tuple2<String, Configuration> stringConfigurationTuple = metricRegistryConfiguration.getReporterConfigurations().get(0);
		Assert.assertEquals("test", stringConfigurationTuple.f0);
		Assert.assertEquals("hello", stringConfigurationTuple.f1.getString("arg1", ""));
		Assert.assertEquals("world", stringConfigurationTuple.f1.getString("arg2", ""));
		Assert.assertEquals(TestReporter1.class.getName(), stringConfigurationTuple.f1.getString("class", AbstractReporter.class.getName()));
	}

	/**
	 * Verifies that two reporters can be parse simultaneously with arguments forwarded.
	 */
	@Test
	public void testSeveralReportersWithArgumentForwarding() {
		Configuration config = new Configuration();

		/* test1 -> TestReporter1  with arg1 and arg2 */
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1.arg1", "hello");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1.arg2", "world");

		/* test2 -> TestReporter1  with arg1 and arg3 */
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2.arg1", "hallo");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2.arg3", "welt");


		MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(config);

		Assert.assertEquals(2, metricRegistryConfiguration.getReporterConfigurations().size());

		/* test1 check */
		Optional<Tuple2<String, Configuration>> test1Config = metricRegistryConfiguration.getReporterConfigurations().stream()
			.filter(c -> c.f0.equals("test1"))
			.findFirst();
		Assert.assertTrue(test1Config.isPresent());
		Assert.assertEquals("test1", test1Config.get().f0);
		Assert.assertEquals("hello", test1Config.get().f1.getString("arg1", ""));
		Assert.assertEquals("world", test1Config.get().f1.getString("arg2", ""));
		Assert.assertEquals(TestReporter1.class.getName(), test1Config.get().f1.getString("class", AbstractReporter.class.getName()));

		/* test2 check */
		Optional<Tuple2<String, Configuration>> test2Config = metricRegistryConfiguration.getReporterConfigurations().stream()
			.filter(c -> c.f0.equals("test2"))
			.findFirst();
		Assert.assertTrue(test1Config.isPresent());
		Assert.assertEquals("test2", test2Config.get().f0);
		Assert.assertEquals("hallo", test2Config.get().f1.getString("arg1", ""));
		Assert.assertEquals("welt", test2Config.get().f1.getString("arg3", ""));
		Assert.assertEquals(TestReporter2.class.getName(), test2Config.get().f1.getString("class", AbstractReporter.class.getName()));
	}

	/**
	 * Verifies that we can activate only one reporter among two declares in configuration.
	 */
	@Test
	public void testActivateOneReporterAmongTwoDeclared() {
		Configuration config = new Configuration();

		/* test1 -> TestReporter1  with arg1 and arg2 */
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1.arg1", "hello");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1.arg2", "world");

		/* test2 -> TestReporter1  with arg1 and arg3 */
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2.arg1", "hallo");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2.arg3", "welt");

		/* select reporter 2 */
		config.setString(MetricOptions.REPORTERS_LIST, "test2");

		MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(config);

		Assert.assertEquals(1, metricRegistryConfiguration.getReporterConfigurations().size());

		Tuple2<String, Configuration> stringConfigurationTuple = metricRegistryConfiguration.getReporterConfigurations().get(0);
		Assert.assertEquals("test2", stringConfigurationTuple.f0);
	}
}
