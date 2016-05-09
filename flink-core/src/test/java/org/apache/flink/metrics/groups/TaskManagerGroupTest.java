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
package org.apache.flink.metrics.groups;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricRegistry;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TaskManagerGroupTest {
	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerMetricGroup operator = new TaskManagerMetricGroup(registry, "host", "id");

		List<String> scope = operator.generateScope();
		assertEquals(3, scope.size());
		assertEquals("host", scope.get(0));
		assertEquals("taskmanager", scope.get(1));
		assertEquals("id", scope.get(2));
	}

	@Test
	public void testGenerateScopeWildcard() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerMetricGroup operator = new TaskManagerMetricGroup(registry, "host", "id");

		Scope.ScopeFormat format = new Scope.ScopeFormat();
		format.setTaskManagerFormat(Scope.concat(Scope.SCOPE_WILDCARD, "superhost", TaskManagerMetricGroup.SCOPE_TM_HOST));

		List<String> scope = operator.generateScope(format);
		assertEquals(2, scope.size());
		assertEquals("superhost", scope.get(0));
		assertEquals("host", scope.get(1));
	}

	@Test
	public void testGenerateScopeCustom() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerMetricGroup operator = new TaskManagerMetricGroup(registry, "host", "id");

		Scope.ScopeFormat format = new Scope.ScopeFormat();
		format.setTaskManagerFormat(Scope.concat("h", TaskManagerMetricGroup.SCOPE_TM_HOST, "t", TaskManagerMetricGroup.SCOPE_TM_ID));

		List<String> scope = operator.generateScope(format);
		assertEquals(4, scope.size());
		assertEquals("h", scope.get(0));
		assertEquals("host", scope.get(1));
		assertEquals("t", scope.get(2));
		assertEquals("id", scope.get(3));
	}
}
