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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

public class TaskDeploymentDescriptorTest {
	@Test
	public void testSerialization() {
		try {
			final JobID jobID = new JobID();
			final JobVertexID vertexID = new JobVertexID();
			final ExecutionAttemptID execId = new ExecutionAttemptID();
			final String jobName = "job name";
			final String taskName = "task name";
			final int numberOfKeyGroups = 1;
			final int indexInSubtaskGroup = 0;
			final int currentNumberOfSubtasks = 1;
			final int attemptNumber = 0;
			final Configuration jobConfiguration = new Configuration();
			final Configuration taskConfiguration = new Configuration();
			final Class<? extends AbstractInvokable> invokableClass = BatchTask.class;
			final List<ResultPartitionDeploymentDescriptor> producedResults = new ArrayList<ResultPartitionDeploymentDescriptor>(0);
			final List<InputGateDeploymentDescriptor> inputGates = new ArrayList<InputGateDeploymentDescriptor>(0);
			final List<BlobKey> requiredJars = new ArrayList<BlobKey>(0);
			final List<URL> requiredClasspaths = new ArrayList<URL>(0);
			final SerializedValue<ExecutionConfig> executionConfig = new SerializedValue<>(new ExecutionConfig());
			final SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(new JobInformation(
				jobID, jobName, executionConfig, jobConfiguration, requiredJars, requiredClasspaths));
			final SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(new TaskInformation(
				vertexID, taskName, currentNumberOfSubtasks, numberOfKeyGroups, invokableClass.getName(), taskConfiguration));
			final int targetSlotNumber = 47;
			final TaskStateHandles taskStateHandles = new TaskStateHandles();

			final TaskDeploymentDescriptor orig = new TaskDeploymentDescriptor(
				serializedJobInformation,
				serializedJobVertexInformation,
				execId,
				indexInSubtaskGroup,
				attemptNumber,
				targetSlotNumber,
				taskStateHandles,
				producedResults,
				inputGates);
	
			final TaskDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);
	
			assertFalse(orig.getSerializedJobInformation() == copy.getSerializedJobInformation());
			assertFalse(orig.getSerializedTaskInformation() == copy.getSerializedTaskInformation());
			assertFalse(orig.getExecutionAttemptId() == copy.getExecutionAttemptId());
			assertFalse(orig.getTaskStateHandles() == copy.getTaskStateHandles());
			assertFalse(orig.getProducedPartitions() == copy.getProducedPartitions());
			assertFalse(orig.getInputGates() == copy.getInputGates());

			assertEquals(orig.getSerializedJobInformation(), copy.getSerializedJobInformation());
			assertEquals(orig.getSerializedTaskInformation(), copy.getSerializedTaskInformation());
			assertEquals(orig.getExecutionAttemptId(), copy.getExecutionAttemptId());
			assertEquals(orig.getSubtaskIndex(), copy.getSubtaskIndex());
			assertEquals(orig.getAttemptNumber(), copy.getAttemptNumber());
			assertEquals(orig.getTargetSlotNumber(), copy.getTargetSlotNumber());
			assertEquals(orig.getTaskStateHandles(), copy.getTaskStateHandles());
			assertEquals(orig.getProducedPartitions(), copy.getProducedPartitions());
			assertEquals(orig.getInputGates(), copy.getInputGates());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
