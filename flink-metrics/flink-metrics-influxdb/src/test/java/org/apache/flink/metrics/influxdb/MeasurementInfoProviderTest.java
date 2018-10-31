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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.util.AbstractID;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyChar;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link MeasurementInfoProvider}.
 */
public class MeasurementInfoProviderTest {
	private static final Random RANDOM = new Random();

	private final MeasurementInfoProvider provider = new MeasurementInfoProvider();

	@Test
	public void testGetMetricInfo() {
		// MetricRegistry, required as the first argument in metric groups.
		MetricRegistryImpl metricRegistry = new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(new Configuration()));

		// Create an example, nested metric group: taskmanager -> job -> task
		// Variables: <host>, <tm_id>
		String hostname = "loc<>al\"::host\".:";
		String taskManagerId = "tas:kMana::ger";
		TaskManagerMetricGroup tmMetricGroup = new TaskManagerMetricGroup(
			metricRegistry, hostname, taskManagerId);

		// Variables: <job_id>, <job_name>
		JobID jobID = new JobID();
		String jobName = "testJ\"ob:-!ax..?";
		TaskManagerJobMetricGroup tmJobMetricGroup = new TaskManagerJobMetricGroup(
			metricRegistry, tmMetricGroup, jobID, jobName);

		// Variables: <task_id>, <task_attempt_id>, <task_name>, <subtask_index>, <task_attempt_num>
		JobVertexID taskId = new JobVertexID();
		AbstractID taskAttemptID = new AbstractID();
		String taskName = "test\"Ta\"..sk";
		int subtaskIndex = RANDOM.nextInt();
		int taskAttemptNum = RANDOM.nextInt();
		TaskMetricGroup taskMetricGroup = new TaskMetricGroup(
			metricRegistry, tmJobMetricGroup, taskId, taskAttemptID, taskName, subtaskIndex, taskAttemptNum);

		String metricName = "testCounter";
		MetricGroup metricGroup = new FrontMetricGroup<>(0, taskMetricGroup);

		MeasurementInfo info = provider.getMetricInfo(metricName, metricGroup);
		assertNotNull(info);
		assertEquals(
			String.join("" + MeasurementInfoProvider.SCOPE_SEPARATOR, "taskmanager", "job", "task", metricName),
			info.getName());
		assertThat(info.getTags(), hasEntry("host", hostname));
		assertThat(info.getTags(), hasEntry("tm_id", taskManagerId));
		assertThat(info.getTags(), hasEntry("job_id", jobID.toString()));
		assertThat(info.getTags(), hasEntry("job_name", jobName));
		assertThat(info.getTags(), hasEntry("task_id", taskId.toString()));
		assertThat(info.getTags(), hasEntry("task_attempt_id", taskAttemptID.toString()));
		assertThat(info.getTags(), hasEntry("task_name", taskName));
		assertThat(info.getTags(), hasEntry("subtask_index", String.valueOf(subtaskIndex)));
		assertThat(info.getTags(), hasEntry("task_attempt_num", String.valueOf(taskAttemptNum)));
		assertEquals(9, info.getTags().size());

		try {
			metricRegistry.shutdown().get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void simpleTestGetMetricInfo() {
		String logicalScope = "myService.Status.JVM.ClassLoader";
		Map<String, String> variables = new HashMap<>();
		variables.put("<A>", "a");
		variables.put("<B>", "b");
		variables.put("<C>", "c");
		String metricName = "ClassesLoaded";
		FrontMetricGroup metricGroup = mock(FrontMetricGroup.class);
		when(metricGroup.getAllVariables()).thenReturn(variables);
		when(metricGroup.getLogicalScope(any(), anyChar())).thenReturn(logicalScope);

		MeasurementInfo info = provider.getMetricInfo(metricName, metricGroup);
		assertNotNull(info);
		assertEquals(
			String.join("" + MeasurementInfoProvider.SCOPE_SEPARATOR, logicalScope, metricName),
			info.getName());
		assertThat(info.getTags(), hasEntry("A", "a"));
		assertThat(info.getTags(), hasEntry("B", "b"));
		assertThat(info.getTags(), hasEntry("C", "c"));
		assertEquals(3, info.getTags().size());
	}
}
