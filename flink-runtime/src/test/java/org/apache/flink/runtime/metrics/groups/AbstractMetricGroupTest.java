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
import org.apache.flink.metrics.reporter.MetricReporter;
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

			@Override
			protected String getGroupName(CharacterFilter filter) {
				return "";
			}
		};
		assertTrue(group.getAllVariables().isEmpty());
		
		registry.shutdown();
	}

	// ========================================================================
	// Scope Caching
	// ========================================================================

	private static final CharacterFilter FILTER_C = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return input.replace("C", "X");
		}
	};
	private static final CharacterFilter FILTER_B = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return input.replace("B", "X");
		}
	};

	@Test
	public void testScopeCachingForMultipleReporters() throws Exception {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "A.B.C.D");
		config.setString(ConfigConstants.METRICS_REPORTERS_LIST, "test1,test2");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "-");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "!");

		MetricRegistry testRegistry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));
		try {
			MetricGroup tmGroup = new TaskManagerMetricGroup(testRegistry, "host", "id");
			tmGroup.counter("1");
			assertEquals("Reporters were not properly instantiated", 2, testRegistry.getReporters().size());
			for (MetricReporter reporter : testRegistry.getReporters()) {
				ScopeCheckingTestReporter typedReporter = (ScopeCheckingTestReporter) reporter;
				if (typedReporter.failureCause != null) {
					throw typedReporter.failureCause;
				}
			}
		} finally {
			testRegistry.shutdown();
		}
	}

	private abstract static class ScopeCheckingTestReporter extends TestReporter {
		protected Exception failureCause;

		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			try {
				checkScopes(metric, metricName, group);
			} catch (Exception e) {
				if (failureCause == null) {
					failureCause = e;
				}
			}
		}

		public abstract void checkScopes(Metric metric, String metricName, MetricGroup group);

	}

	public static class TestReporter1 extends ScopeCheckingTestReporter {
		@Override
		public String filterCharacters(String input) {
			return FILTER_B.filterCharacters(input);
		}
		@Override
		public void checkScopes(Metric metric, String metricName, MetricGroup group) {
			// the first call determines which filter is applied to all future calls; in this case no filter is used at all
			assertEquals("A-B-C-D-1", group.getMetricIdentifier(metricName));
			// from now on the scope string is cached and should not be reliant on the given filter
			assertEquals("A-B-C-D-1", group.getMetricIdentifier(metricName, FILTER_C));
			assertEquals("A-B-C-D-1", group.getMetricIdentifier(metricName, this));
			// the metric name however is still affected by the filter as it is not cached
			assertEquals("A-B-C-D-4", group.getMetricIdentifier(metricName, new CharacterFilter() {
				@Override
				public String filterCharacters(String input) {
					return input.replace("B", "X").replace("1", "4");
				}
			}));
		}
	}

	public static class TestReporter2 extends ScopeCheckingTestReporter {
		@Override
		public String filterCharacters(String input) {
			return FILTER_C.filterCharacters(input);
		}

		@Override
		public void checkScopes(Metric metric, String metricName, MetricGroup group) {
			// the first call determines which filter is applied to all future calls
			assertEquals("A!B!X!D!1", group.getMetricIdentifier(metricName, this));
			// from now on the scope string is cached and should not be reliant on the given filter
			assertEquals("A!B!X!D!1", group.getMetricIdentifier(metricName));
			assertEquals("A!B!X!D!1", group.getMetricIdentifier(metricName, FILTER_C));
			// the metric name however is still affected by the filter as it is not cached
			assertEquals("A!B!X!D!3", group.getMetricIdentifier(metricName, new CharacterFilter() {
				@Override
				public String filterCharacters(String input) {
					return input.replace("A", "X").replace("1", "3");
				}
			}));
		}
	}

	@Test
	public void testScopeGenerationWithoutReporters() {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "A.B.C.D");
		MetricRegistry testRegistry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));

		try {
			TaskManagerMetricGroup group = new TaskManagerMetricGroup(testRegistry, "host", "id");
			assertEquals("MetricReporters list should be empty", 0, testRegistry.getReporters().size());
			
			// default delimiter should be used
			assertEquals("A.B.X.D.1", group.getMetricIdentifier("1", FILTER_C));
			// no caching should occur
			assertEquals("A.X.C.D.1", group.getMetricIdentifier("1", FILTER_B));
			// invalid reporter indices do not throw errors
			assertEquals("A.X.C.D.1", group.getMetricIdentifier("1", FILTER_B, -1));
			assertEquals("A.X.C.D.1", group.getMetricIdentifier("1", FILTER_B, 2));
		} finally {
			testRegistry.shutdown();
		}
	}
}
