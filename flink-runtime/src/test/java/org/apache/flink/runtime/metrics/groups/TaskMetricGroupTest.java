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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskMetricGroupTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  scope tests
	// -----------------------------------------------------------------------

	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		AbstractID vertexId = new AbstractID();
		AbstractID executionId = new AbstractID();

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");
		TaskMetricGroup taskGroup = new TaskMetricGroup(registry, jmGroup, vertexId, executionId, "aTaskName", 13, 2);

		assertArrayEquals(
				new String[] { "theHostName", "taskmanager", "test-tm-id", "myJobName", "aTaskName", "13"},
				taskGroup.getScopeComponents());

		assertEquals(
				"theHostName.taskmanager.test-tm-id.myJobName.aTaskName.13.name",
				taskGroup.getMetricIdentifier("name"));
		registry.shutdown();
	}

	@Test
	public void testGenerateScopeCustom() {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "abc");
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM_JOB, "def");
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_TASK, "<tm_id>.<job_id>.<task_id>.<task_attempt_id>");
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(cfg));

		JobID jid = new JobID();
		AbstractID vertexId = new AbstractID();
		AbstractID executionId = new AbstractID();

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");
		TaskMetricGroup taskGroup = new TaskMetricGroup(
				registry, jmGroup, vertexId, executionId, "aTaskName", 13, 2);

		assertArrayEquals(
				new String[] { "test-tm-id", jid.toString(), vertexId.toString(), executionId.toString() },
				taskGroup.getScopeComponents());

		assertEquals(
				String.format("test-tm-id.%s.%s.%s.name", jid, vertexId, executionId),
				taskGroup.getMetricIdentifier("name"));
		registry.shutdown();
	}

	@Test
	public void testGenerateScopeWilcard() {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_TASK, "*.<task_attempt_id>.<subtask_index>");
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(cfg));

		AbstractID executionId = new AbstractID();

		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id");
		TaskManagerJobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");

		TaskMetricGroup taskGroup = new TaskMetricGroup(
				registry, jmGroup, new AbstractID(), executionId, "aTaskName", 13, 1);

		assertArrayEquals(
				new String[] { "theHostName", "taskmanager", "test-tm-id", "myJobName", executionId.toString(), "13" },
				taskGroup.getScopeComponents());

		assertEquals(
				"theHostName.taskmanager.test-tm-id.myJobName." + executionId + ".13.name",
				taskGroup.getMetricIdentifier("name"));
		registry.shutdown();
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

		QueryScopeInfo.TaskQueryScopeInfo info = task.createQueryServiceMetricInfo(new DummyCharacterFilter());
		assertEquals("", info.scope);
		assertEquals(jid.toString(), info.jobID);
		assertEquals(vid.toString(), info.vertexID);
		assertEquals(4, info.subtaskIndex);
	}

	@Test
	public void testTaskMetricGroupCleanup() {
		CountingMetricRegistry registry = new CountingMetricRegistry(new Configuration());
		TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(registry, "localhost", "0");
		TaskManagerJobMetricGroup taskManagerJobMetricGroup = new TaskManagerJobMetricGroup(registry, taskManagerMetricGroup, new JobID(), "job");
		TaskMetricGroup taskMetricGroup = new TaskMetricGroup(registry, taskManagerJobMetricGroup, new AbstractID(), new AbstractID(), "task", 0, 0);

		// the io metric should have registered predefined metrics
		assertTrue(registry.getNumberRegisteredMetrics() > 0);

		taskMetricGroup.close();

		// now alle registered metrics should have been unregistered
		assertEquals(0, registry.getNumberRegisteredMetrics());

		registry.shutdown();
	}

	private static class CountingMetricRegistry extends MetricRegistry {

		private int counter = 0;

		CountingMetricRegistry(Configuration config) {
			super(MetricRegistryConfiguration.fromConfiguration(config));
		}

		@Override
		public void register(Metric metric, String metricName, MetricGroup group) {
			super.register(metric, metricName, group);
			counter++;
		}

		@Override
		public void unregister(Metric metric, String metricName, MetricGroup group) {
			super.unregister(metric, metricName, group);
			counter--;
		}

		int getNumberRegisteredMetrics() {
			return counter;
		}
	}
}
