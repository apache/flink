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

import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.TestReporter;

import org.junit.Test;

import java.util.Arrays;

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
	public void testLogicalScopeCachingForMultipleReporters() throws Exception {
		MetricRegistryImpl testRegistry = new MetricRegistryImpl(
			MetricRegistryConfiguration.defaultMetricRegistryConfiguration(),
			Arrays.asList(
				ReporterSetup.forReporter("test1", new LogicalScopeReporter1()),
				ReporterSetup.forReporter("test2", new LogicalScopeReporter2())));
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
	}
}
