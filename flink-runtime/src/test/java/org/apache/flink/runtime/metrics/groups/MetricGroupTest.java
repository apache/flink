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

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link MetricGroup}.
 */
public class MetricGroupTest extends TestLogger {

	private static final MetricRegistryConfiguration defaultMetricRegistryConfiguration = MetricRegistryConfiguration.defaultMetricRegistryConfiguration();

	private MetricRegistry registry;

	private final MetricRegistry exceptionOnRegister = new ExceptionOnRegisterRegistry();

	@Before
	public void createRegistry() {
		this.registry = new MetricRegistry(defaultMetricRegistryConfiguration);
	}

	@After
	public void shutdownRegistry() {
		this.registry.shutdown();
		this.registry = null;
	}

	@Test
	public void sameGroupOnNameCollision() {
		GenericMetricGroup group = new GenericMetricGroup(
		registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String groupName = "sometestname";
		MetricGroup subgroup1 = group.addGroup(groupName);
		MetricGroup subgroup2 = group.addGroup(groupName);

		assertNotNull(subgroup1);
		assertNotNull(subgroup2);
		assertTrue(subgroup1 == subgroup2);
	}

	@Test
	public void closedGroupDoesNotRegisterMetrics() {
		GenericMetricGroup group = new GenericMetricGroup(
				exceptionOnRegister, new DummyAbstractMetricGroup(exceptionOnRegister), "testgroup");
		assertFalse(group.isClosed());

		group.close();
		assertTrue(group.isClosed());

		// these will fail is the registration is propagated
		group.counter("testcounter");
		group.gauge("testgauge", new Gauge<Object>() {
			@Override
			public Object getValue() {
				return null;
			}
		});
	}

	@Test
	public void closedGroupCreatesClosedGroups() {
		GenericMetricGroup group = new GenericMetricGroup(exceptionOnRegister,
				new DummyAbstractMetricGroup(exceptionOnRegister), "testgroup");
		assertFalse(group.isClosed());

		group.close();
		assertTrue(group.isClosed());

		AbstractMetricGroup subgroup = (AbstractMetricGroup) group.addGroup("test subgroup");
		assertTrue(subgroup.isClosed());
	}

	@Test
	public void tolerateMetricNameCollisions() {
		final String name = "abctestname";
		GenericMetricGroup group = new GenericMetricGroup(
				registry, new DummyAbstractMetricGroup(registry), "testgroup");

		assertNotNull(group.counter(name));
		assertNotNull(group.counter(name));
	}

	@Test
	public void tolerateMetricAndGroupNameCollisions() {
		final String name = "abctestname";
		GenericMetricGroup group = new GenericMetricGroup(
				registry, new DummyAbstractMetricGroup(registry), "testgroup");

		assertNotNull(group.addGroup(name));
		assertNotNull(group.counter(name));
	}

	@Test
	public void testCreateQueryServiceMetricInfo() {
		JobID jid = new JobID();
		AbstractID vid = new AbstractID();
		AbstractID eid = new AbstractID();
		MetricRegistry registry = new MetricRegistry(defaultMetricRegistryConfiguration);
		TaskManagerMetricGroup tm = new TaskManagerMetricGroup(registry, "host", "id");
		TaskManagerJobMetricGroup job = new TaskManagerJobMetricGroup(registry, tm, jid, "jobname");
		TaskMetricGroup task = new TaskMetricGroup(registry, job, vid, eid, "taskName", 4, 5);
		GenericMetricGroup userGroup1 = new GenericMetricGroup(registry, task, "hello");
		GenericMetricGroup userGroup2 = new GenericMetricGroup(registry, userGroup1, "world");

		QueryScopeInfo.TaskQueryScopeInfo info1 = (QueryScopeInfo.TaskQueryScopeInfo) userGroup1.createQueryServiceMetricInfo(new DummyCharacterFilter());
		assertEquals("hello", info1.scope);
		assertEquals(jid.toString(), info1.jobID);
		assertEquals(vid.toString(), info1.vertexID);
		assertEquals(4, info1.subtaskIndex);

		QueryScopeInfo.TaskQueryScopeInfo info2 = (QueryScopeInfo.TaskQueryScopeInfo) userGroup2.createQueryServiceMetricInfo(new DummyCharacterFilter());
		assertEquals("hello.world", info2.scope);
		assertEquals(jid.toString(), info2.jobID);
		assertEquals(vid.toString(), info2.vertexID);
		assertEquals(4, info2.subtaskIndex);
	}

	// ------------------------------------------------------------------------

	private static class ExceptionOnRegisterRegistry extends MetricRegistry {

		public ExceptionOnRegisterRegistry() {
			super(defaultMetricRegistryConfiguration);
		}

		@Override
		public void register(Metric metric, String name, AbstractMetricGroup parent) {
			fail("Metric should never be registered");
		}

		@Override
		public void unregister(Metric metric, String name, AbstractMetricGroup parent) {
			fail("Metric should never be un-registered");
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A dummy {@link AbstractMetricGroup} to be used when a group is required as an argument but not actually used.
	 */
	public static class DummyAbstractMetricGroup extends AbstractMetricGroup {

		public DummyAbstractMetricGroup(MetricRegistry registry) {
			super(registry, new String[0], null);
		}

		@Override
		protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
			return null;
		}

		@Override
		protected String getGroupName(CharacterFilter filter) {
			return "";
		}

		@Override
		protected void addMetric(String name, Metric metric) {
		}

		@Override
		public MetricGroup addGroup(String name) {
			return new DummyAbstractMetricGroup(registry);
		}
	}
}
