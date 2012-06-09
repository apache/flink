/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.deployment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.nephele.util.SerializableArrayList;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 * <p>
 * This class is not thread-safe in general.
 * 
 * @author warneke
 */
public final class TaskDeploymentDescriptor implements IOReadableWritable {

	/**
	 * The ID of the job the tasks belongs to.
	 */
	private final JobID jobID;

	/**
	 * The task's execution vertex ID.
	 */
	private final ExecutionVertexID vertexID;

	/**
	 * The task's name.
	 */
	private String taskName;

	/**
	 * The task's index in the subtask group.
	 */
	private int indexInSubtaskGroup;

	/**
	 * The current number of subtasks.
	 */
	private int currentNumberOfSubtasks;

	/**
	 * The configuration of the job the task belongs to.
	 */
	private final Configuration jobConfiguration;

	/**
	 * The task's configuration object.
	 */
	private final Configuration taskConfiguration;

	/**
	 * The task's initial checkpoint state.
	 */
	private CheckpointState initialCheckpointState;

	/**
	 * The class containing the task code to be executed.
	 */
	private Class<? extends AbstractInvokable> invokableClass;

	/**
	 * The list of output gate deployment descriptors.
	 */
	private final SerializableArrayList<GateDeploymentDescriptor> outputGates;

	/**
	 * The list of input gate deployment descriptors.
	 */
	private final SerializableArrayList<GateDeploymentDescriptor> inputGates;

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
	 * @param initialCheckpointState
	 *        the task's initial checkpoint state
	 * @param invokableClass
	 *        the class containing the task code to be executed
	 * @param outputGates
	 *        list of output gate deployment descriptors
	 * @param inputGateIDs
	 *        list of input gate deployment descriptors
	 */
	public TaskDeploymentDescriptor(final JobID jobID, final ExecutionVertexID vertexID, final String taskName,
			final int indexInSubtaskGroup, final int currentNumberOfSubtasks, final Configuration jobConfiguration,
			final Configuration taskConfiguration, final CheckpointState initialCheckpointState,
			final Class<? extends AbstractInvokable> invokableClass,
			final SerializableArrayList<GateDeploymentDescriptor> outputGates,
			final SerializableArrayList<GateDeploymentDescriptor> inputGates) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		if (vertexID == null) {
			throw new IllegalArgumentException("Argument vertexID must not be null");
		}

		if (taskName == null) {
			throw new IllegalArgumentException("Argument taskName must not be null");
		}

		if (indexInSubtaskGroup < 0) {
			throw new IllegalArgumentException("Argument indexInSubtaskGroup must not be smaller than zero");
		}

		if (currentNumberOfSubtasks < indexInSubtaskGroup) {
			throw new IllegalArgumentException(
				"Argument currentNumberOfSubtasks must not be smaller than argument indexInSubtaskGroup");
		}

		if (jobConfiguration == null) {
			throw new IllegalArgumentException("Argument jobConfiguration must not be null");
		}

		if (taskConfiguration == null) {
			throw new IllegalArgumentException("Argument taskConfiguration must not be null");
		}

		if (initialCheckpointState == null) {
			throw new IllegalArgumentException("Argument initialCheckpointState must not be null");
		}

		if (invokableClass == null) {
			throw new IllegalArgumentException("Argument invokableClass must not be null");
		}

		this.jobID = jobID;
		this.vertexID = vertexID;
		this.taskName = taskName;
		this.indexInSubtaskGroup = indexInSubtaskGroup;
		this.currentNumberOfSubtasks = currentNumberOfSubtasks;
		this.jobConfiguration = jobConfiguration;
		this.taskConfiguration = taskConfiguration;
		this.initialCheckpointState = initialCheckpointState;
		this.invokableClass = invokableClass;
		this.outputGates = outputGates;
		this.inputGates = inputGates;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public TaskDeploymentDescriptor() {

		this.jobID = new JobID();
		this.vertexID = new ExecutionVertexID();
		this.taskName = null;
		this.indexInSubtaskGroup = 0;
		this.currentNumberOfSubtasks = 0;
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.initialCheckpointState = CheckpointState.NONE;
		this.invokableClass = null;
		this.outputGates = new SerializableArrayList<GateDeploymentDescriptor>();
		this.inputGates = new SerializableArrayList<GateDeploymentDescriptor>();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.jobID.write(out);
		this.vertexID.write(out);
		StringRecord.writeString(out, this.taskName);
		out.writeInt(this.indexInSubtaskGroup);
		out.writeInt(this.currentNumberOfSubtasks);
		this.jobConfiguration.write(out);
		this.taskConfiguration.write(out);
		EnumUtils.writeEnum(out, this.initialCheckpointState);

		// Write out the names of the required jar files
		final String[] requiredJarFiles = LibraryCacheManager.getRequiredJarFiles(this.jobID);

		out.writeInt(requiredJarFiles.length);
		for (int i = 0; i < requiredJarFiles.length; i++) {
			StringRecord.writeString(out, requiredJarFiles[i]);
		}

		// Write out the name of the invokable class
		if (this.invokableClass == null) {
			throw new IOException("this.invokableClass is null");
		}

		StringRecord.writeString(out, this.invokableClass.getName());

		this.outputGates.write(out);
		this.inputGates.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInput in) throws IOException {

		this.jobID.read(in);
		this.vertexID.read(in);
		this.taskName = StringRecord.readString(in);
		this.indexInSubtaskGroup = in.readInt();
		this.currentNumberOfSubtasks = in.readInt();
		this.jobConfiguration.read(in);
		this.taskConfiguration.read(in);
		this.initialCheckpointState = EnumUtils.readEnum(in, CheckpointState.class);

		// Read names of required jar files
		final String[] requiredJarFiles = new String[in.readInt()];
		for (int i = 0; i < requiredJarFiles.length; i++) {
			requiredJarFiles[i] = StringRecord.readString(in);
		}

		// Now register data with the library manager
		LibraryCacheManager.register(this.jobID, requiredJarFiles);

		// Get ClassLoader from Library Manager
		final ClassLoader cl = LibraryCacheManager.getClassLoader(this.jobID);

		// Read the name of the invokable class;
		final String invokableClassName = StringRecord.readString(in);

		if (invokableClassName == null) {
			throw new IOException("invokableClassName is null");
		}

		try {
			this.invokableClass = (Class<? extends AbstractInvokable>) Class.forName(invokableClassName, true, cl);
		} catch (ClassNotFoundException cnfe) {
			throw new IOException("Class " + invokableClassName + " not found in one of the supplied jar files: "
				+ StringUtils.stringifyException(cnfe));
		}

		this.outputGates.read(in);
		this.inputGates.read(in);
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
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
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
	 * Returns the task's initial checkpoint state.
	 * 
	 * @return the tasks's initial checkpoint state
	 */
	public CheckpointState getInitialCheckpointState() {

		return this.initialCheckpointState;
	}

	/**
	 * Returns the class containing the task code to be executed.
	 * 
	 * @return the class containing the task code to be executed
	 */
	public Class<? extends AbstractInvokable> getInvokableClass() {

		return this.invokableClass;
	}

	/**
	 * Returns the number of output gate deployment descriptors contained in this task deployment descriptor.
	 * 
	 * @return the number of output gate deployment descriptors
	 */
	public int getNumberOfOutputGateDescriptors() {

		return this.outputGates.size();
	}

	/**
	 * Returns the output gate descriptor with the given index
	 * 
	 * @param index
	 *        the index if the output gate descriptor to return
	 * @return the output gate descriptor with the given index
	 */
	public GateDeploymentDescriptor getOutputGateDescriptor(final int index) {

		return this.outputGates.get(index);
	}

	/**
	 * Returns the number of output gate deployment descriptors contained in this task deployment descriptor.
	 * 
	 * @return the number of output gate deployment descriptors
	 */
	public int getNumberOfInputGateDescriptors() {

		return this.inputGates.size();
	}

	/**
	 * Returns the input gate descriptor with the given index
	 * 
	 * @param index
	 *        the index if the input gate descriptor to return
	 * @return the input gate descriptor with the given index
	 */
	public GateDeploymentDescriptor getInputGateDescriptor(final int index) {

		return this.inputGates.get(index);
	}
}
