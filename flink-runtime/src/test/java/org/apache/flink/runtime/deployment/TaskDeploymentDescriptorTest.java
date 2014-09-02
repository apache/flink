/**
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

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.testutils.ServerTestUtils;
import org.apache.flink.runtime.util.SerializableArrayList;
import org.apache.flink.util.StringUtils;
import org.junit.Test;

/**
 * This class contains unit tests for the {@link TaskDeploymentDescriptor} class.
 * 
 */
public class TaskDeploymentDescriptorTest {

	/**
	 * Tests the constructor of the {@link TaskDeploymentDescriptor} class with valid arguments.
	 */
	@Test
	public void testConstructorWithValidArguments() {

		final JobID jobID = new JobID();
		final ExecutionVertexID vertexID = new ExecutionVertexID();
		final String taskName = "task name";
		final int indexInSubtaskGroup = 0;
		final int currentNumberOfSubtasks = 1;
		final Configuration jobConfiguration = new Configuration();
		final Configuration taskConfiguration = new Configuration();
		final Class<? extends AbstractInvokable> invokableClass =  RegularPactTask.class;
		final SerializableArrayList<GateDeploymentDescriptor> outputGates = new SerializableArrayList<GateDeploymentDescriptor>(
			0);
		final SerializableArrayList<GateDeploymentDescriptor> inputGates = new SerializableArrayList<GateDeploymentDescriptor>(
			0);

		final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(jobID, vertexID, taskName,
			indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
			invokableClass, outputGates, inputGates);

		assertEquals(jobID, tdd.getJobID());
		assertEquals(vertexID, tdd.getVertexID());
		assertEquals(taskName, tdd.getTaskName());
		assertEquals(indexInSubtaskGroup, tdd.getIndexInSubtaskGroup());
		assertEquals(currentNumberOfSubtasks, tdd.getCurrentNumberOfSubtasks());
		assertEquals(jobConfiguration, tdd.getJobConfiguration());
		assertEquals(taskConfiguration, tdd.getTaskConfiguration());
		assertEquals(invokableClass, tdd.getInvokableClass());
		assertEquals(outputGates.size(), tdd.getNumberOfOutputGateDescriptors());
		assertEquals(inputGates.size(), tdd.getNumberOfInputGateDescriptors());
	}

	/**
	 * Tests the constructor of the {@link GateDeploymentDescriptor} class with valid arguments.
	 */
	@Test
	public void testConstructorWithInvalidArguments() {

		final JobID jobID = new JobID();
		final ExecutionVertexID vertexID = new ExecutionVertexID();
		final String taskName = "task name";
		final int indexInSubtaskGroup = 0;
		final int currentNumberOfSubtasks = 1;
		final Configuration jobConfiguration = new Configuration();
		final Configuration taskConfiguration = new Configuration();
		final Class<? extends AbstractInvokable> invokableClass = RegularPactTask.class;
		final SerializableArrayList<GateDeploymentDescriptor> outputGates = new SerializableArrayList<GateDeploymentDescriptor>(
			0);
		final SerializableArrayList<GateDeploymentDescriptor> inputGates = new SerializableArrayList<GateDeploymentDescriptor>(
			0);

		boolean firstExceptionCaught = false;
		boolean secondExceptionCaught = false;
		boolean thirdExceptionCaught = false;
		boolean forthExceptionCaught = false;
		boolean fifthExceptionCaught = false;
		boolean sixthExceptionCaught = false;
		boolean seventhExceptionCaught = false;
		boolean eighthExceptionCaught = false;
		boolean ninethExeceptionCaught = false;
		boolean tenthExceptionCaught = false;

		try {
			new TaskDeploymentDescriptor(null, vertexID, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
				invokableClass, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			firstExceptionCaught = true;
		}

		try {
			new TaskDeploymentDescriptor(jobID, null, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
				invokableClass, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			secondExceptionCaught = true;
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, null,
				indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
				invokableClass, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			thirdExceptionCaught = true;
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, taskName,
				-1, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
				invokableClass, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			forthExceptionCaught = true;
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, taskName,
				indexInSubtaskGroup, -1, jobConfiguration, taskConfiguration,
				invokableClass, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			fifthExceptionCaught = true;
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, null, taskConfiguration,
				invokableClass, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			sixthExceptionCaught = true;
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, null,
				invokableClass, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			seventhExceptionCaught = true;
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
				null, outputGates, inputGates);
		} catch (IllegalArgumentException e) {
			eighthExceptionCaught = true;
			
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
				invokableClass, null, inputGates);
		} catch (IllegalArgumentException e) {
			ninethExeceptionCaught = true;
			
		}

		try {
			new TaskDeploymentDescriptor(jobID, vertexID, taskName,
				indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
				invokableClass, outputGates, null);
		} catch (IllegalArgumentException e) {
			tenthExceptionCaught = true;
		}

		if (!firstExceptionCaught) {
			fail("First argument was illegal but not detected");
		}

		if (!secondExceptionCaught) {
			fail("Second argument was illegal but not detected");
		}

		if (!thirdExceptionCaught) {
			fail("Third argument was illegal but not detected");
		}

		if (!forthExceptionCaught) {
			fail("Forth argument was illegal but not detected");
		}

		if (!fifthExceptionCaught) {
			fail("Fifth argument was illegal but not detected");
		}

		if (!sixthExceptionCaught) {
			fail("Sixth argument was illegal but not detected");
		}

		if (!seventhExceptionCaught) {
			fail("Seventh argument was illegal but not detected");
		}

		if (!eighthExceptionCaught) {
			fail("Eighth argument was illegal but not detected");
		}

		if (!ninethExeceptionCaught) {
			fail("Nineth argument was illegal but not detected");
		}

		if (!tenthExceptionCaught) {
			fail("Tenth argument was illegal but not detected");
		}

	}

	/**
	 * Tests the serialization/deserialization of the {@link TaskDeploymentDescriptor} class.
	 */
	@Test
	public void testSerialization() {

		final JobID jobID = new JobID();
		final ExecutionVertexID vertexID = new ExecutionVertexID();
		final String taskName = "task name";
		final int indexInSubtaskGroup = 0;
		final int currentNumberOfSubtasks = 1;
		final Configuration jobConfiguration = new Configuration();
		final Configuration taskConfiguration = new Configuration();
		final Class<? extends AbstractInvokable> invokableClass = RegularPactTask.class;
		final SerializableArrayList<GateDeploymentDescriptor> outputGates = new SerializableArrayList<GateDeploymentDescriptor>(
			0);
		final SerializableArrayList<GateDeploymentDescriptor> inputGates = new SerializableArrayList<GateDeploymentDescriptor>(
			0);

		final TaskDeploymentDescriptor orig = new TaskDeploymentDescriptor(jobID, vertexID, taskName,
			indexInSubtaskGroup, currentNumberOfSubtasks, jobConfiguration, taskConfiguration,
			invokableClass, outputGates, inputGates);

		TaskDeploymentDescriptor copy = null;

		try {
			LibraryCacheManager.register(jobID, new String[] {});
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		try {
			copy = ServerTestUtils.createCopy(orig);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		assertFalse(orig.getJobID() == copy.getJobID());
		assertFalse(orig.getVertexID() == copy.getVertexID());
		assertFalse(orig.getTaskName() == copy.getTaskName());
		assertFalse(orig.getJobConfiguration() == copy.getJobConfiguration());
		assertFalse(orig.getTaskConfiguration() == copy.getTaskConfiguration());

		assertEquals(orig.getJobID(), copy.getJobID());
		assertEquals(orig.getVertexID(), copy.getVertexID());
		assertEquals(orig.getTaskName(), copy.getTaskName());
		assertEquals(orig.getIndexInSubtaskGroup(), copy.getIndexInSubtaskGroup());
		assertEquals(orig.getCurrentNumberOfSubtasks(), copy.getCurrentNumberOfSubtasks());
		assertEquals(orig.getNumberOfOutputGateDescriptors(), copy.getNumberOfOutputGateDescriptors());
		assertEquals(orig.getNumberOfInputGateDescriptors(), copy.getNumberOfInputGateDescriptors());

		try {
			LibraryCacheManager.register(jobID, new String[] {});
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}
	}
}
