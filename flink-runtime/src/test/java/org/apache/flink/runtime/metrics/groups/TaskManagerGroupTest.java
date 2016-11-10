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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.scope.TaskManagerScopeFormat;
import org.apache.flink.util.AbstractID;

import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.*;

public class TaskManagerGroupTest {

	// ------------------------------------------------------------------------
	//  adding and removing jobs
	// ------------------------------------------------------------------------

	@Test
	public void addAndRemoveJobs() throws IOException {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		final TaskManagerMetricGroup group = new TaskManagerMetricGroup(
				registry, "localhost", new AbstractID().toString());
		
		
		final JobID jid1 = new JobID();
		final JobID jid2 = new JobID();
		
		final String jobName1 = "testjob";
		final String jobName2 = "anotherJob";
		
		final JobVertexID vertex11 = new JobVertexID();
		final JobVertexID vertex12 = new JobVertexID();
		final JobVertexID vertex13 = new JobVertexID();
		final JobVertexID vertex21 = new JobVertexID();

		final ExecutionAttemptID execution11 = new ExecutionAttemptID();
		final ExecutionAttemptID execution12 = new ExecutionAttemptID();
		final ExecutionAttemptID execution13 = new ExecutionAttemptID();
		final ExecutionAttemptID execution21 = new ExecutionAttemptID();

		TaskDeploymentDescriptor tdd1 = createTaskDeploymentDescriptor(
			jid1, 
			jobName1, 
			vertex11, 
			execution11, 
			new SerializedValue<>(new ExecutionConfig()), 
			"test", 
			17, 18, 0, 
			new Configuration(), new Configuration(), 
			"", 
			new ArrayList<ResultPartitionDeploymentDescriptor>(), 
			new ArrayList<InputGateDeploymentDescriptor>(), 
			new ArrayList<BlobKey>(), 
			new ArrayList<URL>(), 0);

		TaskDeploymentDescriptor tdd2 = createTaskDeploymentDescriptor(
			jid1,
			jobName1,
			vertex12,
			execution12,
			new SerializedValue<>(new ExecutionConfig()),
			"test",
			13, 18, 1,
			new Configuration(), new Configuration(),
			"",
			new ArrayList<ResultPartitionDeploymentDescriptor>(),
			new ArrayList<InputGateDeploymentDescriptor>(),
			new ArrayList<BlobKey>(),
			new ArrayList<URL>(), 0);

		TaskDeploymentDescriptor tdd3 = createTaskDeploymentDescriptor(
			jid2,
			jobName2,
			vertex21,
			execution21,
			new SerializedValue<>(new ExecutionConfig()),
			"test",
			7, 18, 2,
			new Configuration(), new Configuration(),
			"",
			new ArrayList<ResultPartitionDeploymentDescriptor>(),
			new ArrayList<InputGateDeploymentDescriptor>(),
			new ArrayList<BlobKey>(),
			new ArrayList<URL>(), 0);

		TaskDeploymentDescriptor tdd4 = createTaskDeploymentDescriptor(
			jid1,
			jobName1,
			vertex13,
			execution13,
			new SerializedValue<>(new ExecutionConfig()),
			"test",
			0, 18, 0,
			new Configuration(), new Configuration(),
			"",
			new ArrayList<ResultPartitionDeploymentDescriptor>(),
			new ArrayList<InputGateDeploymentDescriptor>(),
			new ArrayList<BlobKey>(),
			new ArrayList<URL>(), 0);
		
		TaskMetricGroup tmGroup11 = group.addTaskForJob(
			jid1, jobName1, vertex11, execution11, "test", 17, 0);
		TaskMetricGroup tmGroup12 = group.addTaskForJob(
			jid1, jobName1, vertex12, execution12, "test", 13, 1);
		TaskMetricGroup tmGroup21 = group.addTaskForJob(
			jid2, jobName2, vertex21, execution21, "test", 7, 2);
		
		assertEquals(2, group.numRegisteredJobMetricGroups());
		assertFalse(tmGroup11.parent().isClosed());
		assertFalse(tmGroup12.parent().isClosed());
		assertFalse(tmGroup21.parent().isClosed());
		
		// close all for job 2 and one from job 1
		tmGroup11.close();
		tmGroup21.close();
		assertTrue(tmGroup11.isClosed());
		assertTrue(tmGroup21.isClosed());
		
		// job 2 should be removed, job should still be there
		assertFalse(tmGroup11.parent().isClosed());
		assertFalse(tmGroup12.parent().isClosed());
		assertTrue(tmGroup21.parent().isClosed());
		assertEquals(1, group.numRegisteredJobMetricGroups());
		
		// add one more to job one
		TaskMetricGroup tmGroup13 = group.addTaskForJob(
			jid1, jobName1, vertex13, execution13, "test", 0, 0);
		tmGroup12.close();
		tmGroup13.close();

		assertTrue(tmGroup11.parent().isClosed());
		assertTrue(tmGroup12.parent().isClosed());
		assertTrue(tmGroup13.parent().isClosed());
		
		assertEquals(0, group.numRegisteredJobMetricGroups());

		registry.shutdown();
	}

	@Test
	public void testCloseClosesAll() throws IOException {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		final TaskManagerMetricGroup group = new TaskManagerMetricGroup(
				registry, "localhost", new AbstractID().toString());


		final JobID jid1 = new JobID();
		final JobID jid2 = new JobID();

		final String jobName1 = "testjob";
		final String jobName2 = "anotherJob";

		final JobVertexID vertex11 = new JobVertexID();
		final JobVertexID vertex12 = new JobVertexID();
		final JobVertexID vertex21 = new JobVertexID();

		final ExecutionAttemptID execution11 = new ExecutionAttemptID();
		final ExecutionAttemptID execution12 = new ExecutionAttemptID();
		final ExecutionAttemptID execution21 = new ExecutionAttemptID();

		TaskDeploymentDescriptor tdd1 = createTaskDeploymentDescriptor(
			jid1,
			jobName1,
			vertex11,
			execution11,
			new SerializedValue<>(new ExecutionConfig()),
			"test",
			17, 18, 0,
			new Configuration(), new Configuration(),
			"",
			new ArrayList<ResultPartitionDeploymentDescriptor>(),
			new ArrayList<InputGateDeploymentDescriptor>(),
			new ArrayList<BlobKey>(),
			new ArrayList<URL>(), 0);

		TaskDeploymentDescriptor tdd2 = createTaskDeploymentDescriptor(
			jid1,
			jobName1,
			vertex12,
			execution12,
			new SerializedValue<>(new ExecutionConfig()),
			"test",
			13, 18, 1,
			new Configuration(), new Configuration(),
			"",
			new ArrayList<ResultPartitionDeploymentDescriptor>(),
			new ArrayList<InputGateDeploymentDescriptor>(),
			new ArrayList<BlobKey>(),
			new ArrayList<URL>(), 0);

		TaskDeploymentDescriptor tdd3 = createTaskDeploymentDescriptor(
			jid2,
			jobName2,
			vertex21,
			execution21,
			new SerializedValue<>(new ExecutionConfig()),
			"test",
			7, 18, 1,
			new Configuration(), new Configuration(),
			"",
			new ArrayList<ResultPartitionDeploymentDescriptor>(),
			new ArrayList<InputGateDeploymentDescriptor>(),
			new ArrayList<BlobKey>(),
			new ArrayList<URL>(), 0);

		TaskMetricGroup tmGroup11 = group.addTaskForJob(
			jid1, jobName1, vertex11, execution11, "test", 17, 0);
		TaskMetricGroup tmGroup12 = group.addTaskForJob(
			jid1, jobName1, vertex12, execution12, "test", 13, 1);
		TaskMetricGroup tmGroup21 = group.addTaskForJob(
			jid2, jobName2, vertex21, execution21, "test", 7, 1);
		
		group.close();
		
		assertTrue(tmGroup11.isClosed());
		assertTrue(tmGroup12.isClosed());
		assertTrue(tmGroup21.isClosed());

		registry.shutdown();
	}
	
	// ------------------------------------------------------------------------
	//  scope name tests
	// ------------------------------------------------------------------------

	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerMetricGroup group = new TaskManagerMetricGroup(registry, "localhost", "id");

		assertArrayEquals(new String[] { "localhost", "taskmanager", "id" }, group.getScopeComponents());
		assertEquals("localhost.taskmanager.id.name", group.getMetricIdentifier("name"));
		registry.shutdown();
	}

	@Test
	public void testGenerateScopeCustom() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerScopeFormat format = new TaskManagerScopeFormat("constant.<host>.foo.<host>");
		TaskManagerMetricGroup group = new TaskManagerMetricGroup(registry, format, "host", "id");

		assertArrayEquals(new String[] { "constant", "host", "foo", "host" }, group.getScopeComponents());
		assertEquals("constant.host.foo.host.name", group.getMetricIdentifier("name"));
		registry.shutdown();
	}

	private static TaskDeploymentDescriptor createTaskDeploymentDescriptor(
		JobID jobId,
		String jobName,
		JobVertexID jobVertexId,
		ExecutionAttemptID executionAttemptId,
		SerializedValue<ExecutionConfig> serializedExecutionConfig,
		String taskName,
		int subtaskIndex,
		int parallelism,
		int attemptNumber,
		Configuration jobConfiguration,
		Configuration taskConfiguration,
		String invokableClassName,
		Collection<ResultPartitionDeploymentDescriptor> producedPartitions,
		Collection<InputGateDeploymentDescriptor> inputGates,
		Collection<BlobKey> requiredJarFiles,
		Collection<URL> requiredClasspaths,
		int targetSlotNumber) throws IOException {

		JobInformation jobInformation = new JobInformation(
			jobId,
			jobName,
			serializedExecutionConfig,
			jobConfiguration,
			requiredJarFiles,
			requiredClasspaths);

		TaskInformation taskInformation = new TaskInformation(
			jobVertexId,
			taskName,
			parallelism,
			invokableClassName,
			taskConfiguration);

		SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(jobInformation);
		SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(taskInformation);

		return new TaskDeploymentDescriptor(
			serializedJobInformation,
			serializedJobVertexInformation,
			executionAttemptId,
			subtaskIndex,
			attemptNumber,
			targetSlotNumber,
			null,
			producedPartitions,
			inputGates);
	}
}
