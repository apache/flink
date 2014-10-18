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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.types.StringValue;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 */
public final class TaskDeploymentDescriptor implements IOReadableWritable {

	/** The ID of the job the tasks belongs to. */
	private final JobID jobID;

	/** The task's job vertex ID. */
	private final JobVertexID vertexID;
	
	/** The ID referencing the attempt to execute the task. */
	private final ExecutionAttemptID executionId;

	/** The task's name. */
	private String taskName;

	/** The task's index in the subtask group. */
	private int indexInSubtaskGroup;

	/** The current number of subtasks. */
	private int currentNumberOfSubtasks;

	/** The configuration of the job the task belongs to. */
	private Configuration jobConfiguration;

	/** The task's configuration object. */
	private Configuration taskConfiguration;

	/** The name of the class containing the task code to be executed. */
	private String invokableClassName;
	/** The list of output gate deployment descriptors. */
	private List<GateDeploymentDescriptor> outputGates;

	/** The list of input gate deployment descriptors. */
	private List<GateDeploymentDescriptor> inputGates;
	
	private int targetSlotNumber;

	/**
	 * The list of JAR files required to run this task.
	 */
	private final List<BlobKey> requiredJarFiles;

	/**
	 * Constructs a task deployment descriptor.
	 * 
	 * @param jobID
	 *        the ID of the job the tasks belongs to
	 * @param vertexID
	 *        the task's execution vertex ID
	 * @param taskName
	 *        the task's name the task's index in the subtask group
	 * @param indexInSubtaskGroup
	 *        he task's index in the subtask group
	 * @param currentNumberOfSubtasks
	 *        the current number of subtasks
	 * @param jobConfiguration
	 *        the configuration of the job the task belongs to
	 * @param taskConfiguration
	 *        the task's configuration object
	 * @param invokableClassName
	 *        the class containing the task code to be executed
	 * @param outputGates
	 *        list of output gate deployment descriptors
	 * @param requiredJarFiles
	 *        list of JAR files required to run this task
	 */
	public TaskDeploymentDescriptor(JobID jobID, JobVertexID vertexID, ExecutionAttemptID execuionId,
			String taskName, int indexInSubtaskGroup, int currentNumberOfSubtasks, 
			Configuration jobConfiguration, Configuration taskConfiguration,
			String invokableClassName,
			List<GateDeploymentDescriptor> outputGates,
			List<GateDeploymentDescriptor> inputGates,
			final List<BlobKey> requiredJarFiles, int targetSlotNumber){
		if (jobID == null || vertexID == null || execuionId == null || taskName == null || indexInSubtaskGroup < 0 ||
				currentNumberOfSubtasks <= indexInSubtaskGroup || jobConfiguration == null ||
				taskConfiguration == null || invokableClassName == null || outputGates == null || inputGates == null)
		{
			throw new IllegalArgumentException();
		}
		
		if (requiredJarFiles == null) {
			throw new IllegalArgumentException("Argument requiredJarFiles must not be null");
		}

		this.jobID = jobID;
		this.vertexID = vertexID;
		this.executionId = execuionId;
		this.taskName = taskName;
		this.indexInSubtaskGroup = indexInSubtaskGroup;
		this.currentNumberOfSubtasks = currentNumberOfSubtasks;
		this.jobConfiguration = jobConfiguration;
		this.taskConfiguration = taskConfiguration;
		this.invokableClassName = invokableClassName;
		this.outputGates = outputGates;
		this.inputGates = inputGates;
		this.requiredJarFiles = requiredJarFiles;
		this.targetSlotNumber = targetSlotNumber;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public TaskDeploymentDescriptor() {
		this.jobID = new JobID();
		this.vertexID = new JobVertexID();
		this.executionId = new ExecutionAttemptID();
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.outputGates = Collections.emptyList();
		this.inputGates = Collections.emptyList();
		this.requiredJarFiles = new ArrayList<BlobKey>();
	}

	/**
	 * Returns the ID of the job the tasks belongs to.
	 * 
	 * @return the ID of the job the tasks belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the task's execution vertex ID.
	 * 
	 * @return the task's execution vertex ID
	 */
	public JobVertexID getVertexID() {
		return this.vertexID;
	}
	
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	/**
	 * Returns the task's name.
	 * 
	 * @return the task's name
	 */
	public String getTaskName() {
		return this.taskName;
	}

	/**
	 * Returns the task's index in the subtask group.
	 * 
	 * @return the task's index in the subtask group
	 */
	public int getIndexInSubtaskGroup() {
		return this.indexInSubtaskGroup;
	}

	/**
	 * Returns the current number of subtasks.
	 * 
	 * @return the current number of subtasks
	 */
	public int getCurrentNumberOfSubtasks() {
		return this.currentNumberOfSubtasks;
	}

	/**
	 * Returns the configuration of the job the task belongs to.
	 * 
	 * @return the configuration of the job the tasks belongs to
	 */
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	/**
	 * Returns the task's configuration object.
	 * 
	 * @return the task's configuration object
	 */
	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	/**
	 * Returns the name of the class containing the task code to be executed.
	 * 
	 * @return The name of the class containing the task code to be executed
	 */
	public String getInvokableClassName() {
		return this.invokableClassName;
	}

	public List<GateDeploymentDescriptor> getOutputGates() {
		return outputGates;
	}
	
	public List<GateDeploymentDescriptor> getInputGates() {
		return inputGates;
	}

	public List<BlobKey> getRequiredJarFiles() { return requiredJarFiles; }
	
	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void write(final DataOutputView out) throws IOException {
		jobID.write(out);
		vertexID.write(out);
		executionId.write(out);

		StringValue.writeString(taskName, out);
		StringValue.writeString(invokableClassName, out);

		out.writeInt(indexInSubtaskGroup);
		out.writeInt(currentNumberOfSubtasks);
		out.writeInt(targetSlotNumber);

		jobConfiguration.write(out);
		taskConfiguration.write(out);

		writeGateList(inputGates, out);
		writeGateList(outputGates, out);


		// Write out the BLOB keys of the required JAR files
		out.writeInt(this.requiredJarFiles.size());
		for (final Iterator<BlobKey> it = this.requiredJarFiles.iterator(); it.hasNext(); ) {
			it.next().write(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		jobID.read(in);
		vertexID.read(in);
		executionId.read(in);
		
		taskName = StringValue.readString(in);
		invokableClassName = StringValue.readString(in);
		
		indexInSubtaskGroup = in.readInt();
		currentNumberOfSubtasks = in.readInt();
		targetSlotNumber = in.readInt();
		
		jobConfiguration.read(in);
		taskConfiguration.read(in);

		inputGates = readGateList(in);
		outputGates = readGateList(in);

		// Read BLOB keys of required jar files
		final int numberOfJarFiles = in.readInt();
		for (int i = 0; i < numberOfJarFiles; ++i) {
			final BlobKey key = new BlobKey();
			key.read(in);
			this.requiredJarFiles.add(key);
		}
	}
	
	private static final void writeGateList(List<GateDeploymentDescriptor> list, DataOutputView out) throws IOException {
		out.writeInt(list.size());
		for (GateDeploymentDescriptor gdd : list) {
			gdd.write(out);
		}
	}
	
	private static final List<GateDeploymentDescriptor> readGateList(DataInputView in) throws IOException {
		final int len = in.readInt();
		ArrayList<GateDeploymentDescriptor> list = new ArrayList<GateDeploymentDescriptor>(len);
		
		for (int i = 0; i < len; i++) {
			GateDeploymentDescriptor gdd = new GateDeploymentDescriptor();
			gdd.read(in);
			list.add(gdd);
		}
		
		return list;
	}
}