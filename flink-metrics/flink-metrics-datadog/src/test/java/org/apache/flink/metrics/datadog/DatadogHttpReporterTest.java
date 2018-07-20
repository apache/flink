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

package org.apache.flink.metrics.datadog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the DatadogHttpClient.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DatadogHttpReporter.class)
public class DatadogHttpReporterTest {

	class MockedClient extends DatadogHttpReporter {
		@Override
		DatadogHttpClient buildClient(MetricConfig config) {
			return PowerMockito.mock(DatadogHttpClient.class);
		}
	}

	@Test
	public void userKeyValueMetricGroupName() {
		Configuration cfg = new Configuration();
		cfg.setString(MetricOptions.SCOPE_NAMING_TM, "constant.<host>.foo.<host>");
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(cfg));
		TaskManagerMetricGroup group = new TaskManagerMetricGroup(registry, "host", "id");

		MetricConfig config = new MetricConfig();
		config.setProperty(DatadogHttpReporter.API_KEY, "notarealkey");
		config.setProperty(DatadogHttpReporter.REMOVE_VALUE_GROUPS, "true");
		DatadogHttpReporter reporter = new MockedClient();
		reporter.open(config);
		MetricGroup withKeys = group.addGroup("key1").addGroup("key2", "value2");
		String name = reporter.buildMetricName(withKeys, "name");
		assertEquals("constant.host.foo.host.key1.key2.name", name);
	}

	@Test
	public void overrideHostname() {
		PowerMockito.mockStatic(System.class);
		PowerMockito.when(System.getenv("HOSTNAME")).thenReturn("my_hostname");
		Configuration cfg = new Configuration();
		cfg.setString(MetricOptions.SCOPE_NAMING_TM, "<host>.tm");
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(cfg));
		TaskManagerMetricGroup group = new TaskManagerMetricGroup(registry, "host", "id");

		DatadogHttpReporter reporterDefault = new MockedClient();
		MetricConfig config = new MetricConfig();
		config.setProperty(DatadogHttpReporter.API_KEY, "notarealkey");
		config.setProperty(DatadogHttpReporter.OVERRIDE_HOSTNAME, "true");
		reporterDefault.open(config);
		String hostname = reporterDefault.getHostName(group);
		assertEquals("my_hostname", hostname);
	}
}
