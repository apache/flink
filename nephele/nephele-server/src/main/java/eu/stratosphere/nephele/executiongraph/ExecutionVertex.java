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

package eu.stratosphere.nephele.executiongraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ExecutionStateTransition;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.AtomicEnum;
import eu.stratosphere.nephele.util.SerializableHashSet;

/**
 * An execution vertex represents an instance of a task in a Nephele job. An execution vertex
 * is initially created from a job vertex and always belongs to exactly one group vertex.
 * It is possible to duplicate execution vertices in order to distribute a task to several different
 * task managers and process the task in parallel.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ExecutionVertex {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionVertex.class);

	/**
	 * The class of the task to be executed by this vertex.
	 */
	private final Class<? extends AbstractInvokable> invokableClass;

	/**
	 * The ID of the vertex.
	 */
	private final ExecutionVertexID vertexID;

	/**
	 * The environment created to execute the vertex's task.
	 */
	private final RuntimeEnvironment environment;

	/**
	 * The group vertex this vertex belongs to.
	 */
	private final ExecutionGroupVertex groupVertex;

	/**
	 * The execution graph is vertex belongs to.
	 */
	private final ExecutionGraph executionGraph;

	/**
	 * The allocated resources assigned to this vertex.
	 */
	private volatile AllocatedResource allocatedResource = null;

	/**
	 * The allocation ID identifying the allocated resources used by this vertex
	 * within the instance.
	 */
	private volatile AllocationID allocationID = null;

	/**
	 * A list of {@link VertexAssignmentListener} objects to be notified about changes in the instance assignment.
	 */
	private final CopyOnWriteArrayList<VertexAssignmentListener> vertexAssignmentListeners = new CopyOnWriteArrayList<VertexAssignmentListener>();

	/**
	 * A list of {@link CheckpointStateListener} objects to be notified about state changes of the vertex's
	 * checkpoint.
	 */
	private final CopyOnWriteArrayList<CheckpointStateListener> checkpointStateListeners = new CopyOnWriteArrayList<CheckpointStateListener>();

	/**
	 * A list of {@link ExecutionListener} objects to be notified about state changes of the vertex's
	 * checkpoint.
	 */
	private final CopyOnWriteArrayList<ExecutionListener> executionListeners = new CopyOnWriteArrayList<ExecutionListener>();

	/**
	 * The current execution state of the task represented by this vertex
	 */
	private final AtomicEnum<ExecutionState> executionState = new AtomicEnum<ExecutionState>(ExecutionState.CREATED);

	/**
	 * The current checkpoint state of this vertex.
	 */
	private final AtomicEnum<CheckpointState> checkpointState = new AtomicEnum<CheckpointState>(CheckpointState.NONE);

	/**
	 * The execution pipeline this vertex is part of.
	 */
	private final AtomicReference<ExecutionPipeline> executionPipeline = new AtomicReference<ExecutionPipeline>(null);

	/**
	 * Create a new execution vertex and instantiates its environment.
	 * 
	 * @param jobID
	 *        the ID of the job this execution vertex is created from
	 * @param invokableClass
	 *        the task that is assigned to this execution vertex
	 * @param executionGraph
	 *        the execution graph the new vertex belongs to
	 * @param groupVertex
	 *        the group vertex the new vertex belongs to
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public ExecutionVertex(final JobID jobID, final Class<? extends AbstractInvokable> invokableClass,
			final ExecutionGraph executionGraph, final ExecutionGroupVertex groupVertex) throws Exception {
		this(new ExecutionVertexID(), invokableClass, executionGraph, groupVertex, new RuntimeEnvironment(jobID,
			groupVertex.getName(), invokableClass, groupVertex.getConfiguration()));

		this.groupVertex.addInitialSubtask(this);

		if (invokableClass == null) {
			LOG.error("Vertex " + groupVertex.getName() + " does not specify a task");
		}

		// Register the vertex itself as a listener for state changes
		registerExecutionListener(this.executionGraph);
		this.environment.instantiateInvokable();
	}

	/**
	 * Private constructor used to duplicate execution vertices.
	 * 
	 * @param vertexID
	 *        the ID of the new execution vertex.
	 * @param invokableClass
	 *        the task that is assigned to this execution vertex
	 * @param executionGraph
	 *        the execution graph the new vertex belongs to
	 * @param groupVertex
	 *        the group vertex the new vertex belongs to
	 * @param environment
	 *        the environment for the newly created vertex
	 */
	private ExecutionVertex(final ExecutionVertexID vertexID, final Class<? extends AbstractInvokable> invokableClass,
			final ExecutionGraph executionGraph, final ExecutionGroupVertex groupVertex,
			final RuntimeEnvironment environment) {
		this.vertexID = vertexID;
		this.invokableClass = invokableClass;
		this.executionGraph = executionGraph;
		this.groupVertex = groupVertex;
		this.environment = environment;

		// Register the vertex itself as a listener for state changes
		registerExecutionListener(this.executionGraph);

		// Duplication of environment is done in method duplicateVertex
	}

	/**
	 * Returns the environment of this execution vertex.
	 * 
	 * @return the environment of this execution vertex
	 */
	public RuntimeEnvironment getEnvironment() {
		return this.environment;
	}

	/**
	 * Returns the group vertex this execution vertex belongs to.
	 * 
	 * @return the group vertex this execution vertex belongs to
	 */
	public ExecutionGroupVertex getGroupVertex() {
		return this.groupVertex;
	}

	/**
	 * Returns the name of the execution vertex.
	 * 
	 * @return the name of the execution vertex
	 */
	public String getName() {
		return this.groupVertex.getName();
	}

	/**
	 * Returns a duplicate of this execution vertex.
	 * 
	 * @param preserveVertexID
	 *        <code>true</code> to copy the vertex's ID to the duplicated vertex, <code>false</code> to create a new ID
	 * @return a duplicate of this execution vertex
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public ExecutionVertex duplicateVertex(final boolean preserveVertexID) throws Exception {

		ExecutionVertexID newVertexID;
		if (preserveVertexID) {
			newVertexID = this.vertexID;
		} else {
			newVertexID = new ExecutionVertexID();
		}

		final RuntimeEnvironment duplicatedEnvironment = this.environment.duplicateEnvironment();

		final ExecutionVertex duplicatedVertex = new ExecutionVertex(newVertexID, this.invokableClass,
			this.executionGraph, this.groupVertex, duplicatedEnvironment);

		// TODO set new profiling record with new vertex id
		duplicatedVertex.setAllocatedResource(this.allocatedResource);

		return duplicatedVertex;
	}

	/**
	 * Returns a duplicate of this execution vertex. The duplicated vertex receives
	 * a new vertex ID.
	 * 
	 * @return a duplicate of this execution vertex.
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public ExecutionVertex splitVertex() throws Exception {

		return duplicateVertex(false);
	}

	/**
	 * Returns this execution vertex's current execution status.
	 * 
	 * @return this execution vertex's current execution status
	 */
	public ExecutionState getExecutionState() {
		return this.executionState.get();
	}

	/**
	 * Updates the vertex's current execution state.
	 * 
	 * @param newExecutionState
	 *        the new execution state
	 */
	public void updateExecutionState(final ExecutionState newExecutionState) {
		updateExecutionState(newExecutionState, null);
	}

	/**
	 * Updates the vertex's current execution state.
	 * 
	 * @param newExecutionState
	 *        the new execution state
	 * @param optionalMessage
	 *        an optional message related to the state change
	 */
	public void updateExecutionState(final ExecutionState newExecutionState, final String optionalMessage) {

		if (newExecutionState == null) {
			throw new IllegalArgumentException("Argument newExecutionState must not be null");
		}

		// Check the transition
		ExecutionStateTransition.checkTransition(getName(), this.executionState.get(), newExecutionState);

		// Check and save the new execution state
		if (this.executionState.getAndSet(newExecutionState) == newExecutionState) {
			return;
		}

		// Notify the listener objects
		final Iterator<ExecutionListener> it = this.executionListeners.iterator();
		while (it.hasNext()) {
			it.next().executionStateChanged(this.executionGraph.getJobID(), this.vertexID, newExecutionState,
				optionalMessage);
		}
	}

	public boolean compareAndUpdateExecutionState(final ExecutionState expected, final ExecutionState update) {

		if (update == null) {
			throw new IllegalArgumentException("Argument update must not be null");
		}

		// Check the transition
		ExecutionStateTransition.checkTransition(getName(), this.executionState.get(), update);

		if (!this.executionState.compareAndSet(expected, update)) {
			return false;
		}

		// Notify the listener objects
		final Iterator<ExecutionListener> it = this.executionListeners.iterator();
		while (it.hasNext()) {
			it.next().executionStateChanged(this.executionGraph.getJobID(), this.vertexID, update,
				null);
		}

		return true;
	}

	public void updateCheckpointState(final CheckpointState newCheckpointState) {

		if (newCheckpointState == null) {
			throw new IllegalArgumentException("Argument newCheckpointState must not be null");
		}

		// Check and save the new checkpoint state
		if (this.checkpointState.getAndSet(newCheckpointState) == newCheckpointState) {
			return;
		}

		// Notify the listener objects
		final Iterator<CheckpointStateListener> it = this.checkpointStateListeners.iterator();
		while (it.hasNext()) {
			it.next().checkpointStateChanged(this.getExecutionGraph().getJobID(), this.vertexID, newCheckpointState);
		}
	}

	public void initialExecutionResourcesExhausted(
			final ResourceUtilizationSnapshot resourceUtilizationSnapshot) {

		// Notify the listener objects
		final Iterator<ExecutionListener> it = this.executionListeners.iterator();
		while (it.hasNext()) {
			it.next().initialExecutionResourcesExhausted(this.environment.getJobID(), this.vertexID,
				resourceUtilizationSnapshot);
		}
	}

	/**
	 * Assigns the execution vertex with an {@link AllocatedResource}.
	 * 
	 * @param allocatedResource
	 *        the resources which are supposed to be allocated to this vertex
	 */
	public void setAllocatedResource(final AllocatedResource allocatedResource) {

		if (allocatedResource == null) {
			throw new IllegalArgumentException("Argument allocatedResource must not be null");
		}

		this.allocatedResource = allocatedResource;

		// Notify all listener objects
		final Iterator<VertexAssignmentListener> it = this.vertexAssignmentListeners.iterator();
		while (it.hasNext()) {
			it.next().vertexAssignmentChanged(this.vertexID, allocatedResource);
		}
	}

	/**
	 * Returns the allocated resources assigned to this execution vertex.
	 * 
	 * @return the allocated resources assigned to this execution vertex
	 */
	public AllocatedResource getAllocatedResource() {
		return this.allocatedResource;
	}

	/**
	 * Returns the allocation ID which identifies the resources used
	 * by this vertex within the assigned instance.
	 * 
	 * @return the allocation ID which identifies the resources used
	 *         by this vertex within the assigned instance or <code>null</code> if the instance is still assigned to a
	 *         {@link eu.stratosphere.nephele.instance.DummyInstance}.
	 */
	public AllocationID getAllocationID() {
		return this.allocationID;
	}

	/**
	 * Returns the ID of this execution vertex.
	 * 
	 * @return the ID of this execution vertex
	 */
	public ExecutionVertexID getID() {
		return this.vertexID;
	}

	/**
	 * Returns the number of predecessors, i.e. the number of vertices
	 * which connect to this vertex.
	 * 
	 * @return the number of predecessors
	 */
	public int getNumberOfPredecessors() {

		int numberOfPredecessors = 0;

		for (int i = 0; i < this.environment.getNumberOfInputGates(); i++) {
			numberOfPredecessors += this.environment.getInputGate(i).getNumberOfInputChannels();
		}

		return numberOfPredecessors;
	}

	/**
	 * Returns the number of successors, i.e. the number of vertices
	 * this vertex is connected to.
	 * 
	 * @return the number of successors
	 */
	public int getNumberOfSuccessors() {

		int numberOfSuccessors = 0;

		for (int i = 0; i < this.environment.getNumberOfOutputGates(); i++) {
			numberOfSuccessors += this.environment.getOutputGate(i).getNumberOfOutputChannels();
		}

		return numberOfSuccessors;
	}

	public ExecutionVertex getPredecessor(int index) {

		if (index < 0) {
			throw new IllegalArgumentException("Argument index must be greather or equal to 0");
		}

		for (int i = 0; i < this.environment.getNumberOfInputGates(); i++) {

			final InputGate<? extends Record> inputGate = this.environment.getInputGate(i);

			if (index >= 0 && index < inputGate.getNumberOfInputChannels()) {

				final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(index);
				return this.executionGraph.getVertexByChannelID(inputChannel.getConnectedChannelID());
			}
			index -= inputGate.getNumberOfInputChannels();
		}

		return null;
	}

	public ExecutionVertex getSuccessor(int index) {

		if (index < 0) {
			throw new IllegalArgumentException("Argument index must be greather or equal to 0");
		}

		for (int i = 0; i < this.environment.getNumberOfOutputGates(); i++) {

			final RuntimeOutputGate<? extends Record> outputGate = this.environment.getOutputGate(i);

			if (index >= 0 && index < outputGate.getNumberOfOutputChannels()) {

				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(index);
				return this.executionGraph.getVertexByChannelID(outputChannel.getConnectedChannelID());
			}
			index -= outputGate.getNumberOfOutputChannels();
		}

		return null;

	}

	/**
	 * Checks if this vertex is an input vertex in its stage, i.e. has either no
	 * incoming connections or only incoming connections to group vertices in a lower stage.
	 * 
	 * @return <code>true</code> if this vertex is an input vertex, <code>false</code> otherwise
	 */
	public boolean isInputVertex() {

		return this.groupVertex.isInputVertex();
	}

	/**
	 * Checks if this vertex is an output vertex in its stage, i.e. has either no
	 * outgoing connections or only outgoing connections to group vertices in a higher stage.
	 * 
	 * @return <code>true</code> if this vertex is an output vertex, <code>false</code> otherwise
	 */
	public boolean isOutputVertex() {

		return this.groupVertex.isOutputVertex();
	}

	public SerializableHashSet<ChannelID> constructInitialActiveOutputChannelsSet() {

		final SerializableHashSet<ChannelID> activeOutputChannels = new SerializableHashSet<ChannelID>();

		synchronized (this) {

			final int numberOfOutputGates = this.environment.getNumberOfOutputGates();
			for (int i = 0; i < numberOfOutputGates; ++i) {

				final RuntimeOutputGate<? extends Record> outputGate = this.environment.getOutputGate(i);
				final ChannelType channelType = outputGate.getChannelType();
				final int numberOfOutputChannels = outputGate.getNumberOfOutputChannels();
				for (int j = 0; j < numberOfOutputChannels; ++j) {
					final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
					if (channelType == ChannelType.FILE) {
						activeOutputChannels.add(outputChannel.getID());
						continue;
					}
					if (channelType == ChannelType.INMEMORY) {
						activeOutputChannels.add(outputChannel.getID());
						continue;
					}
					if (channelType == ChannelType.NETWORK) {

						final ExecutionVertex connectedVertex = this.executionGraph.getVertexByChannelID(outputChannel
							.getConnectedChannelID());
						final ExecutionState state = connectedVertex.getExecutionState();
						if (state == ExecutionState.READY || state == ExecutionState.STARTING
							|| state == ExecutionState.RUNNING) {
							activeOutputChannels.add(outputChannel.getID());
						}
					}
				}
			}
		}

		return activeOutputChannels;
	}

	/**
	 * Deploys and starts the task represented by this vertex
	 * on the assigned instance.
	 * 
	 * @return the result of the task submission attempt
	 */
	public TaskSubmissionResult startTask() {

		if (this.allocatedResource == null) {
			final TaskSubmissionResult result = new TaskSubmissionResult(getID(),
				AbstractTaskResult.ReturnCode.ERROR);
			result.setDescription("Assigned instance of vertex " + this.toString() + " is null!");
			return result;
		}

		final SerializableHashSet<ChannelID> activeOutputChannels = constructInitialActiveOutputChannelsSet();

		try {
			return this.allocatedResource.getInstance().submitTask(this.vertexID,
				this.executionGraph.getJobConfiguration(), this.environment,
				activeOutputChannels);
		} catch (IOException e) {
			final TaskSubmissionResult result = new TaskSubmissionResult(getID(), AbstractTaskResult.ReturnCode.ERROR);
			result.setDescription(StringUtils.stringifyException(e));
			return result;
		}
	}

	/**
	 * Cancels and removes the task represented by this vertex
	 * from the instance it is currently running on. If the task
	 * is not currently running, its execution state is simply
	 * updated to <code>CANCELLED</code>.
	 * 
	 * @return the result of the task cancel attempt
	 */
	public TaskCancelResult cancelTask() {

		final ExecutionState state = this.executionState.get();

		if (this.groupVertex.getStageNumber() != this.executionGraph.getIndexOfCurrentExecutionStage()) {
			// Set to canceled directly
			updateExecutionState(ExecutionState.CANCELED, null);
			return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
		}

		if (state == ExecutionState.FINISHED || state == ExecutionState.FAILED) {
			// Ignore this call
			return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
		}

		if (state != ExecutionState.RUNNING && state != ExecutionState.STARTING
			&& state != ExecutionState.FINISHING) {
			// Set to canceled directly
			updateExecutionState(ExecutionState.CANCELED, null);
			return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
		}

		if (this.allocatedResource == null) {
			final TaskCancelResult result = new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.ERROR);
			result.setDescription("Assigned instance of vertex " + this.toString() + " is null!");
			return result;
		}

		try {
			return this.allocatedResource.getInstance().cancelTask(this.vertexID);
		} catch (IOException e) {
			final TaskCancelResult result = new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.ERROR);
			result.setDescription(StringUtils.stringifyException(e));
			return result;
		}
	}

	/**
	 * Returns the {@link ExecutionGraph} this vertex belongs to.
	 * 
	 * @return the {@link ExecutionGraph} this vertex belongs to
	 */
	public ExecutionGraph getExecutionGraph() {

		return this.executionGraph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return getName() + " (" + (this.environment.getIndexInSubtaskGroup() + 1) + "/"
			+ (this.environment.getCurrentNumberOfSubtasks()) + ")";
	}

	/**
	 * Returns the task represented by this vertex has
	 * a retry attempt left in case of an execution
	 * failure.
	 * 
	 * @return <code>true</code> if the task has a retry attempt left, <code>false</code> otherwise
	 */
	public boolean hasRetriesLeft() {
		// TODO: Implement me
		return false;
	}

	/**
	 * Registers the {@link VertexAssignmentListener} object for this vertex. This object
	 * will be notified about reassignments of this vertex to another instance.
	 * 
	 * @param vertexAssignmentListener
	 *        the object to be notified about reassignments of this vertex to another instance
	 */
	public void registerVertexAssignmentListener(final VertexAssignmentListener vertexAssignmentListener) {

		this.vertexAssignmentListeners.addIfAbsent(vertexAssignmentListener);
	}

	/**
	 * Unregisters the {@link VertexAssignmentListener} object for this vertex. This object
	 * will no longer be notified about reassignments of this vertex to another instance.
	 * 
	 * @param vertexAssignmentListener
	 *        the listener to be unregistered
	 */
	public void unregisterVertexAssignmentListener(final VertexAssignmentListener vertexAssignmentListener) {

		this.vertexAssignmentListeners.remove(vertexAssignmentListener);
	}

	/**
	 * Registers the {@link CheckpointStateListener} object for this vertex. This object
	 * will be notified about state changes regarding the vertex's checkpoint.
	 * 
	 * @param checkpointStateListener
	 *        the object to be notified about checkpoint state changes
	 */
	public void registerCheckpointStateListener(final CheckpointStateListener checkpointStateListener) {

		this.checkpointStateListeners.addIfAbsent(checkpointStateListener);
	}

	/**
	 * Unregisters the {@link CheckpointStateListener} object for this vertex. This object
	 * will no longer be notified about state changes regarding the vertex's checkpoint.
	 * 
	 * @param checkpointStateListener
	 *        the listener to be unregistered
	 */
	public void unregisterCheckpointStateListener(final CheckpointStateListener checkpointStateListener) {

		this.checkpointStateListeners.remove(checkpointStateListener);
	}

	/**
	 * Registers the {@link ExecutionListener} object for this vertex. This object
	 * will be notified about particular events during the vertex's lifetime.
	 * 
	 * @param executionListener
	 *        the object to be notified about particular events during the vertex's lifetime
	 */
	public void registerExecutionListener(final ExecutionListener executionListener) {

		this.executionListeners.addIfAbsent(executionListener);
	}

	/**
	 * Unregisters the {@link ExecutionListener} object for this vertex. This object
	 * will no longer be notified about particular events during the vertex's lifetime.
	 * 
	 * @param checkpointStateChangeListener
	 *        the object to be unregistered
	 */
	public void unregisterExecutionListener(final ExecutionListener executionListener) {

		this.executionListeners.remove(executionListener);
	}

	/**
	 * Returns the current state of this vertex's checkpoint.
	 * 
	 * @return the current state of this vertex's checkpoint
	 */
	public CheckpointState getCheckpointState() {

		return this.checkpointState.get();
	}

	/**
	 * Sets the {@link ExecutionPipeline} this vertex shall be part of.
	 * 
	 * @param executionPipeline
	 *        the execution pipeline this vertex shall be part of
	 */
	void setExecutionPipeline(final ExecutionPipeline executionPipeline) {

		final ExecutionPipeline oldPipeline = this.executionPipeline.getAndSet(executionPipeline);
		if (oldPipeline != null) {
			oldPipeline.removeFromPipeline(this);
		}

		executionPipeline.addToPipeline(this);
	}

	/**
	 * Returns the {@link ExecutionPipeline} this vertex is part of.
	 * 
	 * @return the execution pipeline this vertex is part of
	 */
	public ExecutionPipeline getExecutionPipeline() {

		return this.executionPipeline.get();
	}
}
