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
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.TestReporter;

import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link AbstractMetricGroup}.
 */
public class AbstractMetricGroupTest {
	/**
	 * Verifies that no {@link NullPointerException} is thrown when {@link AbstractMetricGroup#getAllVariables()} is
	 * called and the parent is null.
	 */
	@Test
	public void testGetAllVariables() throws Exception {
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

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

		registry.shutdown().get();
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
		config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "-");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "!");

		MetricRegistryImpl testRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));
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
			testRegistry.shutdown().get();
		}
	}

	@Test
	public void testLogicalScopeCachingForMultipleReporters() throws Exception {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, LogicalScopeReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, LogicalScopeReporter2.class.getName());

		MetricRegistryImpl testRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));
		try {
			MetricGroup tmGroup = new TaskManagerMetricGroup(testRegistry, "host", "id")
				.addGroup("B")
				.addGroup("C");
			tmGroup.counter("1");
			assertEquals("Reporters were not properly instantiated", 2, testRegistry.getReporters().size());
			for (MetricReporter reporter : testRegistry.getReporters()) {
				ScopeCheckingTestReporter typedReporter = (ScopeCheckingTestReporter) reporter;
				if (typedReporter.failureCause != null) {
					throw typedReporter.failureCause;
				}
			}
		} finally {
			testRegistry.shutdown().get();
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

	/**
	 * Reporter that verifies the scope caching behavior.
	 */
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

	/**
	 * Reporter that verifies the scope caching behavior.
	 */
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

	/**
	 * Reporter that verifies the logical-scope caching behavior.
	 */
	public static final class LogicalScopeReporter1 extends ScopeCheckingTestReporter {
		@Override
		public String filterCharacters(String input) {
			return FILTER_B.filterCharacters(input);
		}

		@Override
		public void checkScopes(Metric metric, String metricName, MetricGroup group) {
			final String logicalScope = ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(this, '-');
			assertEquals("taskmanager-X-C", logicalScope);
		}
	}

	/**
	 * Reporter that verifies the logical-scope caching behavior.
	 */
	public static final class LogicalScopeReporter2 extends ScopeCheckingTestReporter {
		@Override
		public String filterCharacters(String input) {
			return FILTER_C.filterCharacters(input);
		}

		@Override
		public void checkScopes(Metric metric, String metricName, MetricGroup group) {
			final String logicalScope = ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(this, ',');
			assertEquals("taskmanager,B,X", logicalScope);
		}
	}

	@Test
	public void testScopeGenerationWithoutReporters() throws Exception {
		Configuration config = new Configuration();
		config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D");
		MetricRegistryImpl testRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

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
			testRegistry.shutdown().get();
		}
	}

	@Test
	public void testGetAllVariablesDoesNotDeadlock() throws InterruptedException {
		final TestMetricRegistry registry = new TestMetricRegistry();

		final MetricGroup parent = new GenericMetricGroup(registry, UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(), "parent");
		final MetricGroup child = parent.addGroup("child");

		final Thread parentRegisteringThread = new Thread(() -> parent.counter("parent_counter"));
		final Thread childRegisteringThread = new Thread(() -> child.counter("child_counter"));

		final BlockerSync parentSync = new BlockerSync();
		final BlockerSync childSync = new BlockerSync();

		try {
			// start both threads and have them block in the registry, so they acquire the lock of their respective group
			registry.setOnRegistrationAction(childSync::blockNonInterruptible);
			childRegisteringThread.start();
			childSync.awaitBlocker();

			registry.setOnRegistrationAction(parentSync::blockNonInterruptible);
			parentRegisteringThread.start();
			parentSync.awaitBlocker();

			// the parent thread remains blocked to simulate the child thread holding some lock in the registry/reporter
			// the child thread continues execution and calls getAllVariables()
			// in the past this would block indefinitely since the method acquires the locks of all parent groups
			childSync.releaseBlocker();
			// wait with a timeout to ensure the finally block is executed _at some point_, un-blocking the parent
			childRegisteringThread.join(1000 * 10);

			parentSync.releaseBlocker();
			parentRegisteringThread.join();
		} finally {
			parentSync.releaseBlocker();
			childSync.releaseBlocker();
			parentRegisteringThread.join();
			childRegisteringThread.join();
		}
	}

	private static final class TestMetricRegistry implements MetricRegistry {

		private Runnable onRegistrationAction;

		void setOnRegistrationAction(Runnable onRegistrationAction) {
			this.onRegistrationAction = onRegistrationAction;
		}

		@Override
		public char getDelimiter() {
			return 0;
		}

		@Override
		public char getDelimiter(int index) {
			return 0;
		}

		@Override
		public int getNumberReporters() {
			return 0;
		}

		@Override
		public void register(Metric metric, String metricName, AbstractMetricGroup group) {
			onRegistrationAction.run();
			group.getAllVariables();
		}

		@Override
		public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
		}

		@Override
		public ScopeFormats getScopeFormats() {
			return null;
		}

		@Nullable
		@Override
		public String getMetricQueryServicePath() {
			return null;
		}
	}
}
