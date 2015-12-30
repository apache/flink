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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.junit.Test;

public class TaskDeploymentDescriptorTest {
	@Test
	public void testSerialization() {
		try {
			final JobID jobID = new JobID();
			final JobVertexID vertexID = new JobVertexID();
			final ExecutionAttemptID execId = new ExecutionAttemptID();
			final String taskName = "task name";
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
	
			final TaskDeploymentDescriptor orig = new TaskDeploymentDescriptor(jobID, vertexID, execId, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, attemptNumber, jobConfiguration, taskConfiguration,
				invokableClass.getName(), producedResults, inputGates, requiredJars, requiredClasspaths, 47);
	
			final TaskDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);

			assertFalse(orig.getJobID() == copy.getJobID());
			assertFalse(orig.getVertexID() == copy.getVertexID());
			assertFalse(orig.getTaskName() == copy.getTaskName());
			assertFalse(orig.getJobConfiguration() == copy.getJobConfiguration());
			assertFalse(orig.getTaskConfiguration() == copy.getTaskConfiguration());
	
			assertEquals(orig.getJobID(), copy.getJobID());
			assertEquals(orig.getVertexID(), copy.getVertexID());
			assertEquals(orig.getTaskName(), copy.getTaskName());
			assertEquals(orig.getIndexInSubtaskGroup(), copy.getIndexInSubtaskGroup());
			assertEquals(orig.getNumberOfSubtasks(), copy.getNumberOfSubtasks());
			assertEquals(orig.getAttemptNumber(), copy.getAttemptNumber());
			assertEquals(orig.getProducedPartitions(), copy.getProducedPartitions());
			assertEquals(orig.getInputGates(), copy.getInputGates());

			assertEquals(orig.getRequiredJarFiles(), copy.getRequiredJarFiles());
			assertEquals(orig.getRequiredClasspaths(), copy.getRequiredClasspaths());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
