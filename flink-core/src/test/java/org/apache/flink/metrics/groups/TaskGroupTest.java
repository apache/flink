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
import org.apache.flink.metrics.groups.scope.ScopeFormat;
import org.apache.flink.metrics.groups.scope.ScopeFormat.TaskManagerJobScopeFormat;
import org.apache.flink.metrics.groups.scope.ScopeFormat.TaskManagerScopeFormat;
import org.apache.flink.metrics.groups.scope.ScopeFormat.TaskScopeFormat;
import org.apache.flink.util.AbstractID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TaskGroupTest {

	// ------------------------------------------------------------------------
	//  scope tests
	// ------------------------------------------------------------------------
	private MetricRegistry registry;

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
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		AbstractID vertexId = new AbstractID();
		AbstractID executionId = new AbstractID();

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");
		TaskMetricGroup taskGroup = new TaskMetricGroup(registry, jmGroup, vertexId, executionId, "aTaskName", 13, 2);

		assertArrayEquals(
				new String[] { "theHostName", "taskmanager", "test-tm-id", "myJobName", "aTaskName", "13"},
				taskGroup.getScopeComponents());

		assertEquals(
				"theHostName.taskmanager.test-tm-id.myJobName.aTaskName.13",
				taskGroup.getScopeString());
		registry.shutdown();
	}

	@Test
	public void testGenerateScopeCustom() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		TaskManagerScopeFormat tmFormat = new TaskManagerScopeFormat("abc");
		TaskManagerJobScopeFormat jmFormat = new TaskManagerJobScopeFormat("def", tmFormat);
		TaskScopeFormat taskFormat = new TaskScopeFormat("<tm_id>.<job_id>.<task_id>.<task_attempt_id>", jmFormat);

		JobID jid = new JobID();
		AbstractID vertexId = new AbstractID();
		AbstractID executionId = new AbstractID();

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");
		TaskMetricGroup taskGroup = new TaskMetricGroup(
				registry, jmGroup, taskFormat, vertexId, executionId, "aTaskName", 13, 2);

		assertArrayEquals(
				new String[] { "test-tm-id", jid.toString(), vertexId.toString(), executionId.toString() },
				taskGroup.getScopeComponents());

		assertEquals(
				String.format("test-tm-id.%s.%s.%s", jid, vertexId, executionId),
				taskGroup.getScopeString());
		registry.shutdown();
	}

	@Test
	public void testGenerateScopeWilcard() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		TaskManagerScopeFormat tmFormat = new TaskManagerScopeFormat(
				ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_GROUP);
		TaskManagerJobScopeFormat jmFormat = new TaskManagerJobScopeFormat(
				ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP, tmFormat);

		TaskScopeFormat format = new TaskScopeFormat("*.<task_attempt_id>.<subtask_index>", jmFormat);

		AbstractID executionId = new AbstractID();

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");

		TaskMetricGroup taskGroup = new TaskMetricGroup(
				registry, jmGroup, format, new AbstractID(), executionId, "aTaskName", 13, 1);

		assertArrayEquals(
				new String[] { "theHostName", "taskmanager", "test-tm-id", "myJobName", executionId.toString(), "13" },
				taskGroup.getScopeComponents());

		assertEquals(
				"theHostName.taskmanager.test-tm-id.myJobName." + executionId + ".13",
				taskGroup.getScopeString());
		registry.shutdown();
	}
}
