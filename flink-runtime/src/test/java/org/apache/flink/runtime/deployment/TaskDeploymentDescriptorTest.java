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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link TaskDeploymentDescriptor}.
 */
public class TaskDeploymentDescriptorTest extends TestLogger {

	private static final JobID jobID = new JobID();
	private static final JobVertexID vertexID = new JobVertexID();
	private static final ExecutionAttemptID execId = new ExecutionAttemptID();
	private static final AllocationID allocationId = new AllocationID();
	private static final String jobName = "job name";
	private static final String taskName = "task name";
	private static final int numberOfKeyGroups = 1;
	private static final int indexInSubtaskGroup = 0;
	private static final int currentNumberOfSubtasks = 1;
	private static final int attemptNumber = 0;
	private static final Configuration jobConfiguration = new Configuration();
	private static final Configuration taskConfiguration = new Configuration();
	private static final Class<? extends AbstractInvokable> invokableClass = BatchTask.class;
	private static final List<ResultPartitionDeploymentDescriptor> producedResults = new ArrayList<ResultPartitionDeploymentDescriptor>(0);
	private static final List<InputGateDeploymentDescriptor> inputGates = new ArrayList<InputGateDeploymentDescriptor>(0);
	private static final List<PermanentBlobKey> requiredJars = new ArrayList<>(0);
	private static final List<URL> requiredClasspaths = new ArrayList<>(0);
	private static final int targetSlotNumber = 47;
	private static final TaskStateSnapshot taskStateHandles = new TaskStateSnapshot();
	private static final JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(1L, taskStateHandles);

	private final SerializedValue<ExecutionConfig> executionConfig = new SerializedValue<>(new ExecutionConfig());
	private final SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(new JobInformation(
		jobID, jobName, executionConfig, jobConfiguration, requiredJars, requiredClasspaths));
	private final SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(new TaskInformation(
		vertexID, taskName, currentNumberOfSubtasks, numberOfKeyGroups, invokableClass.getName(), taskConfiguration));

	public TaskDeploymentDescriptorTest() throws IOException {}

	@Test
	public void testSerialization() throws Exception {
		final TaskDeploymentDescriptor orig = createTaskDeploymentDescriptor(
			new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
			new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobVertexInformation));

		final TaskDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);

		assertFalse(orig.getSerializedJobInformation() == copy.getSerializedJobInformation());
		assertFalse(orig.getSerializedTaskInformation() == copy.getSerializedTaskInformation());
		assertFalse(orig.getExecutionAttemptId() == copy.getExecutionAttemptId());
		assertFalse(orig.getTaskRestore() == copy.getTaskRestore());
		assertFalse(orig.getProducedPartitions() == copy.getProducedPartitions());
		assertFalse(orig.getInputGates() == copy.getInputGates());

		assertEquals(orig.getSerializedJobInformation(), copy.getSerializedJobInformation());
		assertEquals(orig.getSerializedTaskInformation(), copy.getSerializedTaskInformation());
		assertEquals(orig.getExecutionAttemptId(), copy.getExecutionAttemptId());
		assertEquals(orig.getAllocationId(), copy.getAllocationId());
		assertEquals(orig.getSubtaskIndex(), copy.getSubtaskIndex());
		assertEquals(orig.getAttemptNumber(), copy.getAttemptNumber());
		assertEquals(orig.getTargetSlotNumber(), copy.getTargetSlotNumber());
		assertEquals(orig.getTaskRestore().getRestoreCheckpointId(), copy.getTaskRestore().getRestoreCheckpointId());
		assertEquals(orig.getTaskRestore().getTaskStateSnapshot(), copy.getTaskRestore().getTaskStateSnapshot());
		assertEquals(orig.getProducedPartitions(), copy.getProducedPartitions());
		assertEquals(orig.getInputGates(), copy.getInputGates());
	}

	@Test
	public void testOffLoadedAndNonOffLoadedPayload() {
		final TaskDeploymentDescriptor taskDeploymentDescriptor = createTaskDeploymentDescriptor(
			new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
			new TaskDeploymentDescriptor.Offloaded<>(new PermanentBlobKey()));

		SerializedValue<JobInformation> actualSerializedJobInformation = taskDeploymentDescriptor.getSerializedJobInformation();
		assertThat(actualSerializedJobInformation, is(serializedJobInformation));

		try {
			taskDeploymentDescriptor.getSerializedTaskInformation();
			fail("Expected to fail since the task information should be offloaded.");
		} catch (IllegalStateException expected) {
			// expected
		}
	}

	@Nonnull
	private TaskDeploymentDescriptor createTaskDeploymentDescriptor(TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> jobInformation, TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> taskInformation) {
		return new TaskDeploymentDescriptor(
			jobID,
			jobInformation,
			taskInformation,
			execId,
			allocationId,
			indexInSubtaskGroup,
			attemptNumber,
			targetSlotNumber,
			taskRestore,
			producedResults,
			inputGates);
	}
}
