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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.util.AbstractID;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JobGroupTest {
	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		TaskMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "host", "id")
				.addTaskForJob(new JobID(), "job", new AbstractID(), new AbstractID(), 0, "task");
		JobMetricGroup jmGroup = tmGroup.parent();

		List<String> scope = jmGroup.generateScope();
		assertEquals(4, scope.size());
		assertEquals("job", scope.get(3));
	}

	@Test
	public void testGenerateScopeWildcard() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		TaskMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "host", "id")
				.addTaskForJob(new JobID(), "job", new AbstractID(), new AbstractID(), 0, "task");
		JobMetricGroup jmGroup = tmGroup.parent();

		Scope.ScopeFormat format = new Scope.ScopeFormat();
		format.setJobFormat(Scope.concat(Scope.SCOPE_WILDCARD, "superjob", JobMetricGroup.SCOPE_JOB_NAME));

		List<String> scope = jmGroup.generateScope(format);
		assertEquals(5, scope.size());
		assertEquals("superjob", scope.get(3));
		assertEquals("job", scope.get(4));
	}

	@Test
	public void testGenerateScopeCustom() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		TaskMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "host", "id")
				.addTaskForJob(new JobID(), "job", new AbstractID(), new AbstractID(), 0, "task");
		JobMetricGroup jmGroup = tmGroup.parent();

		Scope.ScopeFormat format = new Scope.ScopeFormat();
		format.setJobFormat(Scope.concat(TaskManagerMetricGroup.SCOPE_TM_HOST, "superjob", JobMetricGroup.SCOPE_JOB_NAME));

		List<String> scope = jmGroup.generateScope(format);
		assertEquals(3, scope.size());
		assertEquals("host", scope.get(0));
		assertEquals("superjob", scope.get(1));
		assertEquals("job", scope.get(2));
	}
}
