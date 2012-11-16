/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.IOException;
import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class TaskDeploymentDescriptor implements KryoSerializable {

	/**
	 * The ID of the job the tasks belongs to.
	 */
	private JobID jobID;

	/**
	 * The task's execution vertex ID.
	 */
	private ExecutionVertexID vertexID;

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
	private Configuration jobConfiguration;

	/**
	 * The task's configuration object.
	 */
	private Configuration taskConfiguration;

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
	private ArrayList<GateDeploymentDescriptor> outputGates;

	/**
	 * The list of input gate deployment descriptors.
	 */
	private ArrayList<GateDeploymentDescriptor> inputGates;

	/**
	 * Stores if the task has already been deployed at least once.
	 */
	private boolean hasAlreadyBeenDeployed;

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
	 * @param hasAlreadyBeenDeployed
	 *        stores if the task has already been deployed at least once
	 */
	public TaskDeploymentDescriptor(final JobID jobID, final ExecutionVertexID vertexID, final String taskName,
			final int indexInSubtaskGroup, final int currentNumberOfSubtasks, final Configuration jobConfiguration,
			final Configuration taskConfiguration, final CheckpointState initialCheckpointState,
			final Class<? extends AbstractInvokable> invokableClass,
			final ArrayList<GateDeploymentDescriptor> outputGates,
			final ArrayList<GateDeploymentDescriptor> inputGates, final boolean hasAlreadyBeenDeployed) {

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

		if (outputGates == null) {
			throw new IllegalArgumentException("Argument outputGates must not be null");
		}

		if (inputGates == null) {
			throw new IllegalArgumentException("Argument inputGates must not be null");
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
		this.hasAlreadyBeenDeployed = hasAlreadyBeenDeployed;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public TaskDeploymentDescriptor() {

		this.jobID = null;
		this.vertexID = null;
		this.taskName = null;
		this.indexInSubtaskGroup = -1;
		this.currentNumberOfSubtasks = -1;
		this.jobConfiguration = null;
		this.taskConfiguration = null;
		this.initialCheckpointState = null;
		this.invokableClass = null;
		this.outputGates = null;
		this.inputGates = null;
		this.hasAlreadyBeenDeployed = false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		kryo.writeObject(output, this.jobID);
		kryo.writeObject(output, this.vertexID);
		output.writeString(this.taskName);
		output.writeInt(this.indexInSubtaskGroup);
		output.writeInt(this.currentNumberOfSubtasks);
		EnumUtils.writeEnum(output, this.initialCheckpointState);

		// Write out the names of the required jar files
		String[] requiredJarFiles = null;
		try {
			requiredJarFiles = LibraryCacheManager.getRequiredJarFiles(this.jobID);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		output.writeInt(requiredJarFiles.length);
		for (int i = 0; i < requiredJarFiles.length; i++) {
			output.writeString(requiredJarFiles[i]);
		}

		// Write out the name of the invokable class
		if (this.invokableClass == null) {
			throw new RuntimeException("this.invokableClass is null");
		}

		output.writeString(this.invokableClass.getName());

		this.jobConfiguration.write(kryo, output);
		this.taskConfiguration.write(kryo, output);

		kryo.writeObject(output, this.outputGates);
		kryo.writeObject(output, this.inputGates);

		output.writeBoolean(this.hasAlreadyBeenDeployed);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final Kryo kryo, final Input input) {

		this.jobID = kryo.readObject(input, JobID.class);
		this.vertexID = kryo.readObject(input, ExecutionVertexID.class);
		this.taskName = input.readString();
		this.indexInSubtaskGroup = input.readInt();
		this.currentNumberOfSubtasks = input.readInt();
		this.initialCheckpointState = EnumUtils.readEnum(input, CheckpointState.class);

		// Read names of required jar files
		final String[] requiredJarFiles = new String[input.readInt()];
		for (int i = 0; i < requiredJarFiles.length; i++) {
			requiredJarFiles[i] = input.readString();
		}

		// Now register data with the library manager
		try {
			LibraryCacheManager.register(this.jobID, requiredJarFiles);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		// Get ClassLoader from Library Manager
		ClassLoader cl = null;
		try {
			cl = LibraryCacheManager.getClassLoader(this.jobID);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		// Read the name of the invokable class;
		final String invokableClassName = input.readString();

		if (invokableClassName == null) {
			throw new RuntimeException("invokableClassName is null");
		}

		try {
			this.invokableClass = (Class<? extends AbstractInvokable>) Class.forName(invokableClassName, true, cl);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Class " + invokableClassName + " not found in one of the supplied jar files: "
				+ StringUtils.stringifyException(cnfe));
		}

		this.jobConfiguration = new Configuration(cl);
		this.jobConfiguration.read(kryo, input);
		this.taskConfiguration = new Configuration(cl);
		this.taskConfiguration.read(kryo, input);

		this.outputGates = kryo.readObject(input, ArrayList.class);
		this.inputGates = kryo.readObject(input, ArrayList.class);

		this.hasAlreadyBeenDeployed = input.readBoolean();
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

	/**
	 * Checks if the task has already been deployed at least once.
	 * 
	 * @return <code>true</code> if the task has already been at least once before this deployment, <code>false</code>
	 *         otherwise
	 */
	public boolean hasAlreadyBeenDeployed() {

		return this.hasAlreadyBeenDeployed;
	}
}
