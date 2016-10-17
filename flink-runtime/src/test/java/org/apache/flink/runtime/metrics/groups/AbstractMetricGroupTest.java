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
package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractMetricGroupTest {
	/**
	 * Verifies that no {@link NullPointerException} is thrown when {@link AbstractMetricGroup#getAllVariables()} is
	 * called and the parent is null.
	 */
	@Test
	public void testGetAllVariables() {
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		AbstractMetricGroup group = new AbstractMetricGroup<AbstractMetricGroup<?>>(registry, new String[0], null) {
			@Override
			protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
				return null;
			}
		};
		assertTrue(group.getAllVariables().isEmpty());
		
		registry.shutdown();
	}

	@Test
	public void filteringForMultipleReporters() {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "A.B.C.D");
		config.setString(ConfigConstants.METRICS_REPORTERS_LIST, "test1,test2");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "!");

		TestReporter1.filter = new CharacterFilter() {
			@Override
			public String filterCharacters(String input) {
				return input.replace("C", "RR");
			}
		};

		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));
		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "host", "id");
		tmGroup.counter(1);
		registry.shutdown();
		assert TestReporter1.countSuccess == 2;
	}

	public static class TestReporter1 extends TestReporter {
		protected static CharacterFilter filter; // for test case: one filter for different reporters with different of scope delimiter
		protected static int countSuccess = 0;

		@Override
		public String filterCharacters(String input) {
			return input.replace("A", "RR");
		}
		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			assertEquals("A.B.C.D.1", group.getMetricIdentifier(metricName));
			assertEquals("A.B.RR.D.1", group.getMetricIdentifier(metricName, TestReporter1.filter));
			assertEquals("RR.B.C.D.1", group.getMetricIdentifier(metricName, this));
			assertEquals("A.RR.C.D.1", group.getMetricIdentifier(metricName, new CharacterFilter() {
				@Override
				public String filterCharacters(String input) {
					return input.replace("B", "RR");
				}
			}));
			countSuccess++;
		}
	}

	public static class TestReporter2 extends TestReporter1 {
		@Override
		public String filterCharacters(String input) {
			return input.replace("B", "RR");
		}
		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			assertEquals("A!B!C!D!1", group.getMetricIdentifier(metricName));
			assertEquals("A!RR!C!D!1", group.getMetricIdentifier(metricName, this));
			assertEquals("A!B!RR!D!1", group.getMetricIdentifier(metricName, TestReporter1.filter));
			assertEquals("RR!B!C!D!1", group.getMetricIdentifier(metricName, new CharacterFilter() {
				@Override
				public String filterCharacters(String input) {
					return input.replace("A", "RR");
				}
			}));
			countSuccess++;
		}
	}
}
