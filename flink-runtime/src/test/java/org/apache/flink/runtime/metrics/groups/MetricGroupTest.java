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
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
	public void shutdownRegistry() throws Exception {
		this.registry.shutdown().get();
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

	/**
	 * Verifies the basic behavior when defining user-defined variables.
	 */
	@Test
	public void testUserDefinedVariable() {
		MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
		GenericMetricGroup root = new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

		String key = "key";
		String value = "value";
		MetricGroup group = root.addGroup(key, value);

		String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
		assertEquals(value, variableValue);

		String identifier = group.getMetricIdentifier("metric");
		assertTrue("Key is missing from metric identifier.", identifier.contains("key"));
		assertTrue("Value is missing from metric identifier.", identifier.contains("value"));

		String logicalScope = ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
		assertTrue("Key is missing from logical scope.", logicalScope.contains(key));
		assertFalse("Value is present in logical scope.", logicalScope.contains(value));
	}

	/**
	 * Verifies that calling {@link MetricGroup#addGroup(String, String)} on a {@link GenericKeyMetricGroup} goes
	 * through the generic code path.
	 */
	@Test
	public void testUserDefinedVariableOnKeyGroup() {
		MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
		GenericMetricGroup root = new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

		String key1 = "key1";
		String value1 = "value1";
		root.addGroup(key1, value1);

		String key2 = "key2";
		String value2 = "value2";
		MetricGroup group = root.addGroup(key1).addGroup(key2, value2);

		String variableValue = group.getAllVariables().get("value2");
		assertNull(variableValue);

		String identifier = group.getMetricIdentifier("metric");
		assertTrue("Key1 is missing from metric identifier.", identifier.contains("key1"));
		assertTrue("Key2 is missing from metric identifier.", identifier.contains("key2"));
		assertTrue("Value2 is missing from metric identifier.", identifier.contains("value2"));

		String logicalScope = ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
		assertTrue("Key1 is missing from logical scope.", logicalScope.contains(key1));
		assertTrue("Key2 is missing from logical scope.", logicalScope.contains(key2));
		assertTrue("Value2 is missing from logical scope.", logicalScope.contains(value2));
	}

	/**
	 * Verifies that calling {@link MetricGroup#addGroup(String, String)} if a generic group with the key name already
	 * exists goes through the generic code path.
	 */
	@Test
	public void testNameCollisionForKeyAfterGenericGroup() {
		MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
		GenericMetricGroup root = new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

		String key = "key";
		String value = "value";

		root.addGroup(key);
		MetricGroup group = root.addGroup(key, value);

		String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
		assertNull(variableValue);

		String identifier = group.getMetricIdentifier("metric");
		assertTrue("Key is missing from metric identifier.", identifier.contains("key"));
		assertTrue("Value is missing from metric identifier.", identifier.contains("value"));

		String logicalScope = ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
		assertTrue("Key is missing from logical scope.", logicalScope.contains(key));
		assertTrue("Value is missing from logical scope.", logicalScope.contains(value));
	}

	/**
	 * Verifies that calling {@link MetricGroup#addGroup(String, String)} if a generic group with the key and value name
	 * already exists goes through the generic code path.
	 */
	@Test
	public void testNameCollisionForKeyAndValueAfterGenericGroup() {
		MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
		GenericMetricGroup root = new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

		String key = "key";
		String value = "value";

		root.addGroup(key).addGroup(value);
		MetricGroup group = root.addGroup(key, value);

		String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
		assertNull(variableValue);

		String identifier = group.getMetricIdentifier("metric");
		assertTrue("Key is missing from metric identifier.", identifier.contains("key"));
		assertTrue("Value is missing from metric identifier.", identifier.contains("value"));

		String logicalScope = ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
		assertTrue("Key is missing from logical scope.", logicalScope.contains(key));
		assertTrue("Value is missing from logical scope.", logicalScope.contains(value));
	}

	/**
	 * Verifies that existing key/value groups are returned when calling {@link MetricGroup#addGroup(String)}.
	 */
	@Test
	public void testNameCollisionAfterKeyValueGroup() {
		MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
		GenericMetricGroup root = new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

		String key = "key";
		String value = "value";

		root.addGroup(key, value);
		MetricGroup group = root.addGroup(key).addGroup(value);

		String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
		assertEquals(value, variableValue);

		String identifier = group.getMetricIdentifier("metric");
		assertTrue("Key is missing from metric identifier.", identifier.contains("key"));
		assertTrue("Value is missing from metric identifier.", identifier.contains("value"));

		String logicalScope = ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
		assertTrue("Key is missing from logical scope.", logicalScope.contains(key));
		assertFalse("Value is present in logical scope.", logicalScope.contains(value));
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
