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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MetricGroupTest {
	
	private MetricRegistry registry;

	private final MetricRegistry exceptionOnRegister = new ExceptionOnRegisterRegistry();

	@Before
	public void createRegistry() {
		this.registry = new MetricRegistry(new Configuration());
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
			public Object getValue() { return null; }
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
	
	// ------------------------------------------------------------------------
	
	private static class ExceptionOnRegisterRegistry extends MetricRegistry {
		
		public ExceptionOnRegisterRegistry() {
			super(new Configuration());
		}

		@Override
		public void register(Metric metric, String name, MetricGroup parent) {
			fail("Metric should never be registered");
		}

		@Override
		public void unregister(Metric metric, String name, MetricGroup parent) {
			fail("Metric should never be un-registered");
		}
	}

	// ------------------------------------------------------------------------
	
	private static class DummyAbstractMetricGroup extends AbstractMetricGroup {

		public DummyAbstractMetricGroup(MetricRegistry registry) {
			super(registry, new String[0], null);
		}

		@Override
		protected void addMetric(String name, Metric metric) {}

		@Override
		public MetricGroup addGroup(String name) {
			return new DummyAbstractMetricGroup(registry);
		}
	}
}
