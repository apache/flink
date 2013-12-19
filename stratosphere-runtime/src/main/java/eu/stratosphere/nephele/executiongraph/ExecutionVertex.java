/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.executiongraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.deployment.ChannelDeploymentDescriptor;
import eu.stratosphere.nephele.deployment.GateDeploymentDescriptor;
import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ExecutionStateTransition;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult.ReturnCode;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskKillResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.util.AtomicEnum;
import eu.stratosphere.nephele.util.SerializableArrayList;
import eu.stratosphere.util.StringUtils;

/**
 * An execution vertex represents an instance of a task in a Nephele job. An execution vertex
 * is initially created from a job vertex and always belongs to exactly one group vertex.
 * It is possible to duplicate execution vertices in order to distribute a task to several different
 * task managers and process the task in parallel.
 * <p>
 * This class is thread-safe.
 * 
 */
public final class ExecutionVertex {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionVertex.class);

	/**
	 * The ID of the vertex.
	 */
	private final ExecutionVertexID vertexID;

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
	private final AtomicReference<AllocatedResource> allocatedResource = new AtomicReference<AllocatedResource>(null);

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
	 * A map of {@link ExecutionListener} objects to be notified about the state changes of a vertex.
	 */
	private final ConcurrentMap<Integer, ExecutionListener> executionListeners = new ConcurrentSkipListMap<Integer, ExecutionListener>();

	/**
	 * The current execution state of the task represented by this vertex
	 */
	private final AtomicEnum<ExecutionState> executionState = new AtomicEnum<ExecutionState>(ExecutionState.CREATED);

	/**
	 * The output gates attached to this vertex.
	 */
	private final ExecutionGate[] outputGates;

	/**
	 * The input gates attached to his vertex.
	 */
	private final ExecutionGate[] inputGates;

	/**
	 * The index of this vertex in the vertex group.
	 */
	private volatile int indexInVertexGroup = 0;

	/**
	 * Stores the number of times the vertex may be still be started before the corresponding task is considered to be
	 * failed.
	 */
	private final AtomicInteger retriesLeft;


	/**
	 * The execution pipeline this vertex is part of.
	 */
	private final AtomicReference<ExecutionPipeline> executionPipeline = new AtomicReference<ExecutionPipeline>(null);

	/**
	 * Flag to indicate whether the vertex has been requested to cancel while in state STARTING
	 */
	private final AtomicBoolean cancelRequested = new AtomicBoolean(false);

	/**
	 * Create a new execution vertex and instantiates its environment.
	 * 
	 * @param executionGraph
	 *        the execution graph the new vertex belongs to
	 * @param groupVertex
	 *        the group vertex the new vertex belongs to
	 * @param numberOfOutputGates
	 *        the number of output gates attached to this vertex
	 * @param numberOfInputGates
	 *        the number of input gates attached to this vertex
	 */
	public ExecutionVertex(final ExecutionGraph executionGraph, final ExecutionGroupVertex groupVertex,
			final int numberOfOutputGates, final int numberOfInputGates) {
		this(new ExecutionVertexID(), executionGraph, groupVertex, numberOfOutputGates, numberOfInputGates);

		this.groupVertex.addInitialSubtask(this);
	}

	/**
	 * Private constructor used to duplicate execution vertices.
	 * 
	 * @param vertexID
	 *        the ID of the new execution vertex.
	 * @param executionGraph
	 *        the execution graph the new vertex belongs to
	 * @param groupVertex
	 *        the group vertex the new vertex belongs to
	 * @param numberOfOutputGates
	 *        the number of output gates attached to this vertex
	 * @param numberOfInputGates
	 *        the number of input gates attached to this vertex
	 */
	private ExecutionVertex(final ExecutionVertexID vertexID, final ExecutionGraph executionGraph,
			final ExecutionGroupVertex groupVertex, final int numberOfOutputGates, final int numberOfInputGates) {

		this.vertexID = vertexID;
		this.executionGraph = executionGraph;
		this.groupVertex = groupVertex;

		this.retriesLeft = new AtomicInteger(groupVertex.getNumberOfExecutionRetries());

		this.outputGates = new ExecutionGate[numberOfOutputGates];
		this.inputGates = new ExecutionGate[numberOfInputGates];

		// Register vertex with execution graph
		this.executionGraph.registerExecutionVertex(this);

		// Register the vertex itself as a listener for state changes
		registerExecutionListener(this.executionGraph);
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
	 */
	public ExecutionVertex duplicateVertex(final boolean preserveVertexID) {

		ExecutionVertexID newVertexID;
		if (preserveVertexID) {
			newVertexID = this.vertexID;
		} else {
			newVertexID = new ExecutionVertexID();
		}

		final ExecutionVertex duplicatedVertex = new ExecutionVertex(newVertexID, this.executionGraph,
			this.groupVertex, this.outputGates.length, this.inputGates.length);

		// Duplicate gates
		for (int i = 0; i < this.outputGates.length; ++i) {
			duplicatedVertex.outputGates[i] = new ExecutionGate(new GateID(), duplicatedVertex,
				this.outputGates[i].getGroupEdge(), false);
		}

		for (int i = 0; i < this.inputGates.length; ++i) {
			duplicatedVertex.inputGates[i] = new ExecutionGate(new GateID(), duplicatedVertex,
				this.inputGates[i].getGroupEdge(), true);
		}

		// TODO set new profiling record with new vertex id
		duplicatedVertex.setAllocatedResource(this.allocatedResource.get());

		return duplicatedVertex;
	}

	/**
	 * Inserts the output gate at the given position.
	 * 
	 * @param pos
	 *        the position to insert the output gate
	 * @param outputGate
	 *        the output gate to be inserted
	 */
	void insertOutputGate(final int pos, final ExecutionGate outputGate) {

		if (this.outputGates[pos] != null) {
			throw new IllegalStateException("Output gate at position " + pos + " is not null");
		}

		this.outputGates[pos] = outputGate;
	}

	/**
	 * Inserts the input gate at the given position.
	 * 
	 * @param pos
	 *        the position to insert the input gate
	 * @param outputGate
	 *        the input gate to be inserted
	 */
	void insertInputGate(final int pos, final ExecutionGate inputGate) {

		if (this.inputGates[pos] != null) {
			throw new IllegalStateException("Input gate at position " + pos + " is not null");
		}

		this.inputGates[pos] = inputGate;
	}

	/**
	 * Returns a duplicate of this execution vertex. The duplicated vertex receives
	 * a new vertex ID.
	 * 
	 * @return a duplicate of this execution vertex.
	 */
	public ExecutionVertex splitVertex() {

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
	 * Updates the vertex's current execution state through the job's executor service.
	 * 
	 * @param newExecutionState
	 *        the new execution state
	 * @param optionalMessage
	 *        an optional message related to the state change
	 */
	public void updateExecutionStateAsynchronously(final ExecutionState newExecutionState,
			final String optionalMessage) {

		final Runnable command = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {

				updateExecutionState(newExecutionState, optionalMessage);
			}
		};

		this.executionGraph.executeCommand(command);
	}

	/**
	 * Updates the vertex's current execution state through the job's executor service.
	 * 
	 * @param newExecutionState
	 *        the new execution state
	 */
	public void updateExecutionStateAsynchronously(final ExecutionState newExecutionState) {

		updateExecutionStateAsynchronously(newExecutionState, null);
	}

	/**
	 * Updates the vertex's current execution state.
	 * 
	 * @param newExecutionState
	 *        the new execution state
	 */
	public ExecutionState updateExecutionState(final ExecutionState newExecutionState) {
		return updateExecutionState(newExecutionState, null);
	}

	/**
	 * Updates the vertex's current execution state.
	 * 
	 * @param newExecutionState
	 *        the new execution state
	 * @param optionalMessage
	 *        an optional message related to the state change
	 */
	public ExecutionState updateExecutionState(ExecutionState newExecutionState, final String optionalMessage) {

		if (newExecutionState == null) {
			throw new IllegalArgumentException("Argument newExecutionState must not be null");
		}

		final ExecutionState currentExecutionState = this.executionState.get();
		if (currentExecutionState == ExecutionState.CANCELING) {

			// If we are in CANCELING, ignore state changes to FINISHING
			if (newExecutionState == ExecutionState.FINISHING) {
				return currentExecutionState;
			}

			// Rewrite FINISHED to CANCELED if the task has been marked to be canceled
			if (newExecutionState == ExecutionState.FINISHED) {
				LOG.info("Received transition from CANCELING to FINISHED for vertex " + toString()
					+ ", converting it to CANCELED");
				newExecutionState = ExecutionState.CANCELED;
			}
		}

		// Check and save the new execution state
		final ExecutionState previousState = this.executionState.getAndSet(newExecutionState);
		if (previousState == newExecutionState) {
			return previousState;
		}

		// Check the transition
		ExecutionStateTransition.checkTransition(true, toString(), previousState, newExecutionState);

		// Notify the listener objects
		final Iterator<ExecutionListener> it = this.executionListeners.values().iterator();
		while (it.hasNext()) {
			it.next().executionStateChanged(this.executionGraph.getJobID(), this.vertexID, newExecutionState,
				optionalMessage);
		}

		// The vertex was requested to be canceled by another thread
		checkCancelRequestedFlag();

		return previousState;
	}

	public boolean compareAndUpdateExecutionState(final ExecutionState expected, final ExecutionState update) {

		if (update == null) {
			throw new IllegalArgumentException("Argument update must not be null");
		}

		if (!this.executionState.compareAndSet(expected, update)) {
			return false;
		}

		// Check the transition
		ExecutionStateTransition.checkTransition(true, toString(), expected, update);

		// Notify the listener objects
		final Iterator<ExecutionListener> it = this.executionListeners.values().iterator();
		while (it.hasNext()) {
			it.next().executionStateChanged(this.executionGraph.getJobID(), this.vertexID, update,
				null);
		}

		// Check if the vertex was requested to be canceled by another thread
		checkCancelRequestedFlag();

		return true;
	}

	/**
	 * Checks if another thread requested the vertex to cancel while it was in state STARTING. If so, the method clears
	 * the respective flag and repeats the cancel request.
	 */
	private void checkCancelRequestedFlag() {

		if (this.cancelRequested.compareAndSet(true, false)) {
			final TaskCancelResult tsr = cancelTask();
			if (tsr.getReturnCode() != AbstractTaskResult.ReturnCode.SUCCESS
				&& tsr.getReturnCode() != AbstractTaskResult.ReturnCode.TASK_NOT_FOUND) {
				LOG.error("Unable to cancel vertex " + this + ": " + tsr.getReturnCode().toString()
					+ ((tsr.getDescription() != null) ? (" (" + tsr.getDescription() + ")") : ""));
			}
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

		final AllocatedResource previousResource = this.allocatedResource.getAndSet(allocatedResource);
		if (previousResource != null) {
			previousResource.removeVertexFromResource(this);
		}

		allocatedResource.assignVertexToResource(this);

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

		return this.allocatedResource.get();
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

		for (int i = 0; i < this.inputGates.length; ++i) {
			numberOfPredecessors += this.inputGates[i].getNumberOfEdges();
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

		for (int i = 0; i < this.outputGates.length; ++i) {
			numberOfSuccessors += this.outputGates[i].getNumberOfEdges();
		}

		return numberOfSuccessors;
	}

	public ExecutionVertex getPredecessor(int index) {

		if (index < 0) {
			throw new IllegalArgumentException("Argument index must be greather or equal to 0");
		}

		for (int i = 0; i < this.inputGates.length; ++i) {

			final ExecutionGate inputGate = this.inputGates[i];
			final int numberOfEdges = inputGate.getNumberOfEdges();

			if (index >= 0 && index < numberOfEdges) {

				final ExecutionEdge edge = inputGate.getEdge(index);
				return edge.getOutputGate().getVertex();
			}
			index -= numberOfEdges;
		}

		return null;
	}

	public ExecutionVertex getSuccessor(int index) {

		if (index < 0) {
			throw new IllegalArgumentException("Argument index must be greather or equal to 0");
		}

		for (int i = 0; i < this.outputGates.length; ++i) {

			final ExecutionGate outputGate = this.outputGates[i];
			final int numberOfEdges = outputGate.getNumberOfEdges();

			if (index >= 0 && index < numberOfEdges) {

				final ExecutionEdge edge = outputGate.getEdge(index);
				return edge.getInputGate().getVertex();
			}
			index -= numberOfEdges;
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

	/**
	 * Returns the index of this vertex in the vertex group.
	 * 
	 * @return the index of this vertex in the vertex group
	 */
	public int getIndexInVertexGroup() {

		return this.indexInVertexGroup;
	}

	/**
	 * Sets the vertex' index in the vertex group.
	 * 
	 * @param indexInVertexGroup
	 *        the vertex' index in the vertex group
	 */
	void setIndexInVertexGroup(final int indexInVertexGroup) {

		this.indexInVertexGroup = indexInVertexGroup;
	}

	/**
	 * Returns the number of output gates attached to this vertex.
	 * 
	 * @return the number of output gates attached to this vertex
	 */
	public int getNumberOfOutputGates() {

		return this.outputGates.length;
	}

	/**
	 * Returns the output gate with the given index.
	 * 
	 * @param index
	 *        the index of the output gate to return
	 * @return the output gate with the given index
	 */
	public ExecutionGate getOutputGate(final int index) {

		return this.outputGates[index];
	}

	/**
	 * Returns the number of input gates attached to this vertex.
	 * 
	 * @return the number of input gates attached to this vertex
	 */
	public int getNumberOfInputGates() {

		return this.inputGates.length;
	}

	/**
	 * Returns the input gate with the given index.
	 * 
	 * @param index
	 *        the index of the input gate to return
	 * @return the input gate with the given index
	 */
	public ExecutionGate getInputGate(final int index) {

		return this.inputGates[index];
	}

	/**
	 * Deploys and starts the task represented by this vertex
	 * on the assigned instance.
	 * 
	 * @return the result of the task submission attempt
	 */
	public TaskSubmissionResult startTask() {

		final AllocatedResource ar = this.allocatedResource.get();

		if (ar == null) {
			final TaskSubmissionResult result = new TaskSubmissionResult(getID(),
				AbstractTaskResult.ReturnCode.NO_INSTANCE);
			result.setDescription("Assigned instance of vertex " + this.toString() + " is null!");
			return result;
		}

		final List<TaskDeploymentDescriptor> tasks = new SerializableArrayList<TaskDeploymentDescriptor>();
		tasks.add(constructDeploymentDescriptor());

		try {
			final List<TaskSubmissionResult> results = ar.getInstance().submitTasks(tasks);

			return results.get(0);

		} catch (IOException e) {
			final TaskSubmissionResult result = new TaskSubmissionResult(getID(),
				AbstractTaskResult.ReturnCode.IPC_ERROR);
			result.setDescription(StringUtils.stringifyException(e));
			return result;
		}
	}

	/**
	 * Kills and removes the task represented by this vertex from the instance it is currently running on. If the
	 * corresponding task is not in the state <code>RUNNING</code>, this call will be ignored. If the call has been
	 * executed
	 * successfully, the task will change the state <code>FAILED</code>.
	 * 
	 * @return the result of the task kill attempt
	 */
	public TaskKillResult killTask() {

		final ExecutionState state = this.executionState.get();

		if (state != ExecutionState.RUNNING) {
			final TaskKillResult result = new TaskKillResult(getID(), AbstractTaskResult.ReturnCode.ILLEGAL_STATE);
			result.setDescription("Vertex " + this.toString() + " is in state " + state);
			return result;
		}

		final AllocatedResource ar = this.allocatedResource.get();

		if (ar == null) {
			final TaskKillResult result = new TaskKillResult(getID(), AbstractTaskResult.ReturnCode.NO_INSTANCE);
			result.setDescription("Assigned instance of vertex " + this.toString() + " is null!");
			return result;
		}

		try {
			return ar.getInstance().killTask(this.vertexID);
		} catch (IOException e) {
			final TaskKillResult result = new TaskKillResult(getID(), AbstractTaskResult.ReturnCode.IPC_ERROR);
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

		while (true) {

			final ExecutionState previousState = this.executionState.get();

			if (previousState == ExecutionState.CANCELED) {
				return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
			}

			if (previousState == ExecutionState.FAILED) {
				return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
			}

			if (previousState == ExecutionState.FINISHED) {
				return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
			}

			// The vertex has already received a cancel request
			if (previousState == ExecutionState.CANCELING) {
				return new TaskCancelResult(getID(), ReturnCode.SUCCESS);
			}

			// Do not trigger the cancel request when vertex is in state STARTING, this might cause a race between RPC
			// calls.
			if (previousState == ExecutionState.STARTING) {

				this.cancelRequested.set(true);

				// We had a race, so we unset the flag and take care of cancellation ourselves
				if (this.executionState.get() != ExecutionState.STARTING) {
					this.cancelRequested.set(false);
					continue;
				}

				return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
			}

			// Check if we had a race. If state change is accepted, send cancel request
			if (compareAndUpdateExecutionState(previousState, ExecutionState.CANCELING)) {

				if (this.groupVertex.getStageNumber() != this.executionGraph.getIndexOfCurrentExecutionStage()) {
					// Set to canceled directly
					updateExecutionState(ExecutionState.CANCELED, null);
					return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
				}

				if (previousState != ExecutionState.RUNNING && previousState != ExecutionState.FINISHING) {
					// Set to canceled directly
					updateExecutionState(ExecutionState.CANCELED, null);
					return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
				}

				final AllocatedResource ar = this.allocatedResource.get();

				if (ar == null) {
					final TaskCancelResult result = new TaskCancelResult(getID(),
						AbstractTaskResult.ReturnCode.NO_INSTANCE);
					result.setDescription("Assigned instance of vertex " + this.toString() + " is null!");
					return result;
				}

				try {
					return ar.getInstance().cancelTask(this.vertexID);

				} catch (IOException e) {
					final TaskCancelResult result = new TaskCancelResult(getID(),
						AbstractTaskResult.ReturnCode.IPC_ERROR);
					result.setDescription(StringUtils.stringifyException(e));
					return result;
				}
			}
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


	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder(this.groupVertex.getName());
		sb.append(" (");
		sb.append(this.indexInVertexGroup + 1);
		sb.append('/');
		sb.append(this.groupVertex.getCurrentNumberOfGroupMembers());
		sb.append(')');

		return sb.toString();
	}

	/**
	 * Returns the task represented by this vertex has
	 * a retry attempt left in case of an execution
	 * failure.
	 * 
	 * @return <code>true</code> if the task has a retry attempt left, <code>false</code> otherwise
	 */
	@Deprecated
	public boolean hasRetriesLeft() {
		if (this.retriesLeft.get() <= 0) {
			return false;
		}
		return true;
	}

	/**
	 * Decrements the number of retries left and checks whether another attempt to run the task is possible.
	 * 
	 * @return <code>true</code>if the task represented by this vertex can be started at least once more,
	 *         <code>false/<code> otherwise
	 */
	public boolean decrementRetriesLeftAndCheck() {

		return (this.retriesLeft.decrementAndGet() > 0);
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
	 * Registers the {@link ExecutionListener} object for this vertex. This object
	 * will be notified about particular events during the vertex's lifetime.
	 * 
	 * @param executionListener
	 *        the object to be notified about particular events during the vertex's lifetime
	 */
	public void registerExecutionListener(final ExecutionListener executionListener) {

		final Integer priority = Integer.valueOf(executionListener.getPriority());

		if (priority.intValue() < 0) {
			LOG.error("Priority for execution listener " + executionListener.getClass() + " must be non-negative.");
			return;
		}

		final ExecutionListener previousValue = this.executionListeners.putIfAbsent(priority, executionListener);

		if (previousValue != null) {
			LOG.error("Cannot register " + executionListener.getClass() + " as an execution listener. Priority "
				+ priority.intValue() + " is already taken.");
		}
	}

	/**
	 * Unregisters the {@link ExecutionListener} object for this vertex. This object
	 * will no longer be notified about particular events during the vertex's lifetime.
	 * 
	 * @param executionListener
	 *        the object to be unregistered
	 */
	public void unregisterExecutionListener(final ExecutionListener executionListener) {

		this.executionListeners.remove(Integer.valueOf(executionListener.getPriority()));
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

	/**
	 * Constructs a new task deployment descriptor for this vertex.
	 * 
	 * @return a new task deployment descriptor for this vertex
	 */
	public TaskDeploymentDescriptor constructDeploymentDescriptor() {

		final SerializableArrayList<GateDeploymentDescriptor> ogd = new SerializableArrayList<GateDeploymentDescriptor>(
			this.outputGates.length);
		for (int i = 0; i < this.outputGates.length; ++i) {

			final ExecutionGate eg = this.outputGates[i];
			final List<ChannelDeploymentDescriptor> cdd = new ArrayList<ChannelDeploymentDescriptor>(
				eg.getNumberOfEdges());
			final int numberOfOutputChannels = eg.getNumberOfEdges();
			for (int j = 0; j < numberOfOutputChannels; ++j) {

				final ExecutionEdge ee = eg.getEdge(j);
				cdd.add(new ChannelDeploymentDescriptor(ee.getOutputChannelID(), ee.getInputChannelID()));
			}

			ogd.add(new GateDeploymentDescriptor(eg.getGateID(), eg.getChannelType(), cdd));
		}

		final SerializableArrayList<GateDeploymentDescriptor> igd = new SerializableArrayList<GateDeploymentDescriptor>(
			this.inputGates.length);
		for (int i = 0; i < this.inputGates.length; ++i) {

			final ExecutionGate eg = this.inputGates[i];
			final List<ChannelDeploymentDescriptor> cdd = new ArrayList<ChannelDeploymentDescriptor>(
				eg.getNumberOfEdges());
			final int numberOfInputChannels = eg.getNumberOfEdges();
			for (int j = 0; j < numberOfInputChannels; ++j) {

				final ExecutionEdge ee = eg.getEdge(j);
				cdd.add(new ChannelDeploymentDescriptor(ee.getOutputChannelID(), ee.getInputChannelID()));
			}

			igd.add(new GateDeploymentDescriptor(eg.getGateID(), eg.getChannelType(), cdd));
		}

		final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(this.executionGraph.getJobID(),
			this.vertexID, this.groupVertex.getName(), this.indexInVertexGroup,
			this.groupVertex.getCurrentNumberOfGroupMembers(), this.executionGraph.getJobConfiguration(),
			this.groupVertex.getConfiguration(), this.groupVertex.getInvokableClass(), ogd,
			igd);

		return tdd;
	}
}
