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
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OperatorGroupTest extends TestLogger {

	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");
		TaskMetricGroup taskGroup = new TaskMetricGroup(
				registry, jmGroup,  new AbstractID(),  new AbstractID(), "aTaskName", 11, 0);
		OperatorMetricGroup opGroup = new OperatorMetricGroup(registry, taskGroup, "myOpName");

		assertArrayEquals(
				new String[] { "theHostName", "taskmanager", "test-tm-id", "myJobName", "myOpName", "11" },
				opGroup.getScopeComponents());

		assertEquals(
				"theHostName.taskmanager.test-tm-id.myJobName.myOpName.11.name",
				opGroup.getMetricIdentifier("name"));

		registry.shutdown();
	}

	@Test
	public void testIOMetricGroupInstantiation() {
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");
		TaskMetricGroup taskGroup = new TaskMetricGroup(
			registry, jmGroup, new AbstractID(), new AbstractID(), "aTaskName", 11, 0);
		OperatorMetricGroup opGroup = new OperatorMetricGroup(registry, taskGroup, "myOpName");

		assertNotNull(opGroup.getIOMetricGroup());
		assertNotNull(opGroup.getIOMetricGroup().getNumRecordsInCounter());
		assertNotNull(opGroup.getIOMetricGroup().getNumRecordsOutCounter());

		registry.shutdown();
	}

	@Test
	public void testVariables() {
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		JobID jid = new JobID();
		AbstractID tid = new AbstractID();
		AbstractID eid = new AbstractID();
		
		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");
		TaskMetricGroup taskGroup = new TaskMetricGroup(
			registry, jmGroup,  tid,  eid, "aTaskName", 11, 0);
		OperatorMetricGroup opGroup = new OperatorMetricGroup(registry, taskGroup, "myOpName");

		Map<String, String> variables = opGroup.getAllVariables();

		testVariable(variables, ScopeFormat.SCOPE_HOST, "theHostName");
		testVariable(variables, ScopeFormat.SCOPE_TASKMANAGER_ID, "test-tm-id");
		testVariable(variables, ScopeFormat.SCOPE_JOB_ID, jid.toString());
		testVariable(variables, ScopeFormat.SCOPE_JOB_NAME, "myJobName");
		testVariable(variables, ScopeFormat.SCOPE_TASK_VERTEX_ID, tid.toString());
		testVariable(variables, ScopeFormat.SCOPE_TASK_NAME, "aTaskName");
		testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_ID, eid.toString());
		testVariable(variables, ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, "11");
		testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, "0");
		testVariable(variables, ScopeFormat.SCOPE_OPERATOR_NAME, "myOpName");

		registry.shutdown();
	}

	private static void testVariable(Map<String, String> variables, String key, String expectedValue) {
		String actualValue = variables.get(key);
		assertNotNull(actualValue);
		assertEquals(expectedValue, actualValue);
	}

	@Test
	public void testCreateQueryServiceMetricInfo() {
		JobID jid = new JobID();
		AbstractID vid = new AbstractID();
		AbstractID eid = new AbstractID();
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		TaskManagerMetricGroup tm = new TaskManagerMetricGroup(registry, "host", "id");
		TaskManagerJobMetricGroup job = new TaskManagerJobMetricGroup(registry, tm, jid, "jobname");
		TaskMetricGroup task = new TaskMetricGroup(registry, job, vid, eid, "taskName", 4, 5);
		OperatorMetricGroup operator = new OperatorMetricGroup(registry, task, "operator");

		QueryScopeInfo.OperatorQueryScopeInfo info = operator.createQueryServiceMetricInfo(new DummyCharacterFilter());
		assertEquals("", info.scope);
		assertEquals(jid.toString(), info.jobID);
		assertEquals(vid.toString(), info.vertexID);
		assertEquals(4, info.subtaskIndex);
		assertEquals("operator", info.operatorName);
	}
}
