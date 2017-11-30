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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

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

	private MetricRegistryImpl registry;

	private final MetricRegistryImpl exceptionOnRegister = new ExceptionOnRegisterRegistry();

	@Before
	public void createRegistry() {
		this.registry = new MetricRegistryImpl(defaultMetricRegistryConfiguration);
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
	public void createGroupWithUserDefinedVariables() {
		GenericMetricGroup group = new GenericMetricGroup(
			registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String keyName = "sometestkey";
		String valueName1 = "sometestvalue1";
		MetricGroup subgroup1 = group.addGroup(keyName, valueName1);
		Map<String, String> variables1 = subgroup1.getAllVariables();

		assertNotNull(subgroup1);
		assertTrue(subgroup1 instanceof GenericValueMetricGroup);
		assertTrue(((AbstractMetricGroup) subgroup1).parent instanceof GenericKeyMetricGroup);
		assertTrue(variables1.containsKey(ScopeFormat.asVariable(keyName)));
		assertEquals(valueName1, variables1.get(ScopeFormat.asVariable(keyName)));

		String valueName2 = "sometestvalue2";
		MetricGroup subgroup2 = group.addGroup(keyName, valueName2);
		Map<String, String> variables2 = subgroup2.getAllVariables();

		assertNotNull(subgroup2);
		assertTrue(subgroup2 instanceof GenericValueMetricGroup);
		assertEquals(((AbstractMetricGroup) subgroup1).parent, ((AbstractMetricGroup) subgroup2).parent);
		assertTrue(variables2.containsKey(ScopeFormat.asVariable(keyName)));
		assertEquals(valueName2, variables2.get(ScopeFormat.asVariable(keyName)));
	}

	@Test
	public void forbidToCreateGenericKeyMetricGroupAfterGenericKeyMetricGroup() {
		GenericMetricGroup group = new GenericMetricGroup(
			registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String keyName = "somekeyname";
		String valueName = "somevaluename";
		group.addGroup(keyName, valueName);

		String keyName2 = "somekeyname2";
		String valueName2 = "somevaluename2";
		MetricGroup subgroup = group.addGroup(keyName).addGroup(keyName2, valueName2);

		assertTrue(((AbstractMetricGroup) subgroup).parent instanceof GenericMetricGroup);
		assertTrue(subgroup instanceof GenericMetricGroup);
	}

	@Test
	public void forbidToCreateGenericValueMetricGroupAfterGenericMetricGroup() {
		GenericMetricGroup group = new GenericMetricGroup(
			registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String groupName = "sometestname";
		group.addGroup(groupName);

		String valueName = "somevaluename";
		MetricGroup subgroup = group.addGroup(groupName, valueName);

		assertTrue(subgroup instanceof  GenericMetricGroup);
	}

	@Test
	public void alwaysCanCreateGenericMetricGroup() {
		GenericMetricGroup group = new GenericMetricGroup(
			registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String groupName = "sometestname";
		MetricGroup group1 = group.addGroup(groupName);
		assertTrue(group1 instanceof GenericMetricGroup);

		String keyName = "somekeyname";
		String valueName = "somevaluename";
		MetricGroup valueGroup = group.addGroup(keyName, valueName);
		MetricGroup group2 = group.addGroup(keyName).addGroup(groupName);
		assertTrue(group2 instanceof GenericMetricGroup);

		MetricGroup group3 = valueGroup.addGroup(groupName);
		assertTrue(group3 instanceof GenericMetricGroup);
	}

	@Test
	public void tolerateGroupNameCollisionsWhenGenericMetricGroupCreatedFirst() {
		GenericMetricGroup group = new GenericMetricGroup(
			registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String groupName = "sometestname";
		String valueName = "somevaluename";
		MetricGroup subgroup1 = group.addGroup(groupName);
		MetricGroup subgroup2 = group.addGroup(groupName, valueName);

		assertEquals(subgroup1, ((AbstractMetricGroup) subgroup2).parent);
		assertTrue(subgroup2 instanceof GenericMetricGroup);

		String groupName2 = "sometestname2";
		String valueName2 = "somevaluename2";
		group.addGroup(groupName2, valueName);
		MetricGroup subgroup3 = group.addGroup(groupName2).addGroup(valueName2);
		MetricGroup subgroup4 = group.addGroup(groupName2, valueName2);

		assertEquals(subgroup3, subgroup4);
		assertTrue(subgroup4 instanceof GenericMetricGroup);
	}

	@Test
	public void tolerateGroupNameCollisionsWhenGenericKeyMetricGroupCreatedFirst() {
		GenericMetricGroup group = new GenericMetricGroup(
			registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String keyName = "sometestname";
		String valueName = "somevaluename";
		MetricGroup subgroup1 = group.addGroup(keyName, valueName);
		MetricGroup subgroup2 = group.addGroup(keyName);

		assertEquals(((AbstractMetricGroup) subgroup1).parent, subgroup2);
		assertTrue(subgroup2 instanceof GenericKeyMetricGroup);
	}

	@Test
	public void tolerateGroupNameCollisionsWhenGenericValueMetricGroupCreatedFirst() {
		GenericMetricGroup group = new GenericMetricGroup(
			registry, new DummyAbstractMetricGroup(registry), "somegroup");

		String keyName = "sometestname";
		String valueName = "somevaluename";
		MetricGroup subgroup1 = group.addGroup(keyName, valueName);
		MetricGroup subgroup2 = group.addGroup(keyName).addGroup(valueName);

		assertEquals(subgroup1, subgroup2);
		assertTrue(subgroup2 instanceof GenericValueMetricGroup);
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
		JobVertexID vid = new JobVertexID();
		AbstractID eid = new AbstractID();
		MetricRegistryImpl registry = new MetricRegistryImpl(defaultMetricRegistryConfiguration);
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

	private static class ExceptionOnRegisterRegistry extends MetricRegistryImpl {

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
