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

package org.apache.flink.table.factories;

import org.apache.flink.table.api.NoMatchingTableFactoryException;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.PlannerDescriptor;
import org.apache.flink.table.factories.utils.TestPlannerFactory;
import org.apache.flink.table.utils.TableTestBase;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ComponentFactoryService}.
 */
public class ComponentFactoryServiceTest extends TableTestBase {
	@Test
	public void testLookingUpAmbiguousPlanners() {
		Map<String, String> properties = new HashMap<>();
		properties.put(PlannerDescriptor.CLASS_NAME, TestPlannerFactory.class.getCanonicalName());
		properties.put(PlannerDescriptor.BATCH_MODE, Boolean.toString(true));
		properties.put(TestPlannerFactory.PLANNER_TYPE_KEY, TestPlannerFactory.PLANNER_TYPE_VALUE);

		PlannerFactory plannerFactory = ComponentFactoryService.find(PlannerFactory.class, properties);

		assertThat(plannerFactory, instanceOf(TestPlannerFactory.class));
	}

	@Test
	public void testLookingUpNonExistentClass() {
		expectedException().expect(NoMatchingTableFactoryException.class);

		Map<String, String> properties = new HashMap<>();
		properties.put(PlannerDescriptor.CLASS_NAME, "NoSuchClass");
		properties.put(PlannerDescriptor.BATCH_MODE, Boolean.toString(true));
		properties.put(TestPlannerFactory.PLANNER_TYPE_KEY, TestPlannerFactory.PLANNER_TYPE_VALUE);

		ComponentFactoryService.find(PlannerFactory.class, properties);
	}
}
