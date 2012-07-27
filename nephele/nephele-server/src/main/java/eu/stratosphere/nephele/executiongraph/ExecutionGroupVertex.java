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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.checkpointing.CheckpointMode;
import eu.stratosphere.nephele.checkpointing.CheckpointUtils;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplit;

/**
 * An ExecutionGroupVertex is created for every JobVertex of the initial job graph. It represents a number of execution
 * vertices
 * that originate from the same job vertex.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class ExecutionGroupVertex {

	/**
	 * The default number of retries in case of an error before the task represented by this vertex is considered as
	 * failed.
	 */
	private static final int DEFAULT_EXECUTION_RETRIES = GlobalConfiguration.getInteger(
			ConfigConstants.JOB_EXECUTION_RETRIES_KEY, ConfigConstants.DEFAULT_JOB_EXECUTION_RETRIES);

	/**
	 * The name of the vertex.
	 */
	private final String name;

	/**
	 * The ID of the job vertex which is represented by this group vertex.
	 */
	private final JobVertexID jobVertexID;

	/**
	 * Stores whether the initial group member has already been added to this group vertex.
	 */
	private final AtomicBoolean initialGroupMemberAdded = new AtomicBoolean(false);

	/**
	 * The list of execution vertices which are managed by this group vertex.
	 */
	private final CopyOnWriteArrayList<ExecutionVertex> groupMembers = new CopyOnWriteArrayList<ExecutionVertex>();

	/**
	 * Maximum number of execution vertices this group vertex can manage.
	 */
	private volatile int maxMemberSize = 1;

	/**
	 * Minimum number of execution vertices this group vertex can manage.
	 */
	private volatile int minMemberSize = 1;

	/**
	 * The user defined number of execution vertices, -1 if the user has not specified it.
	 */
	private final int userDefinedNumberOfMembers;

	/**
	 * The instance type to be used for execution vertices this group vertex manages.
	 */
	private volatile InstanceType instanceType = null;

	/**
	 * Stores whether the instance type is user defined.
	 */
	private final boolean userDefinedInstanceType;

	/**
	 * Stores the number of subtasks per instance.
	 */
	private volatile int numberOfSubtasksPerInstance = -1;

	/**
	 * Stores whether the number of subtasks per instance is user defined.
	 */
	private final boolean userDefinedNumberOfSubtasksPerInstance;

	/**
	 * Number of retries in case of an error before the task represented by this vertex is considered as failed.
	 */
	private final int numberOfExecutionRetries;

	/**
	 * The execution group vertex to share instances with.
	 */
	private final AtomicReference<ExecutionGroupVertex> vertexToShareInstancesWith = new AtomicReference<ExecutionGroupVertex>(
		null);

	/**
	 * Set of execution vertices sharing instances with this vertex.
	 */
	private final CopyOnWriteArrayList<ExecutionGroupVertex> verticesSharingInstances = new CopyOnWriteArrayList<ExecutionGroupVertex>();

	/**
	 * Stores whether the group vertex to share instances with is user defined.
	 */
	private final boolean userDefinedVertexToShareInstancesWith;

	/**
	 * The cryptographic signature of the vertex.
	 */
	private final ExecutionSignature executionSignature;

	/**
	 * List of outgoing edges.
	 */
	private final CopyOnWriteArrayList<ExecutionGroupEdge> forwardLinks = new CopyOnWriteArrayList<ExecutionGroupEdge>();

	/**
	 * List of incoming edges.
	 */
	private final CopyOnWriteArrayList<ExecutionGroupEdge> backwardLinks = new CopyOnWriteArrayList<ExecutionGroupEdge>();

	/**
	 * List of input splits assigned to this group vertex.
	 */
	private volatile InputSplit[] inputSplits = null;

	/**
	 * The execution stage this vertex belongs to.
	 */
	private volatile ExecutionStage executionStage = null;

	/**
	 * The configuration object of the original job vertex.
	 */
	private final Configuration configuration;

	/**
	 * The task class that is assigned to execution vertices of this group
	 */
	private final Class<? extends AbstractInvokable> invokableClass;

	/**
	 * The environment created to execute the vertex's task.
	 */
	private final RuntimeEnvironment environment;

	/**
	 * Constructs a new group vertex.
	 * 
	 * @param name
	 *        the name of the group vertex
	 * @param jobVertexID
	 *        the ID of the job vertex which is represented by this group vertex
	 * @param executionGraph
	 *        the execution graph is group vertex belongs to
	 * @param userDefinedNumberOfMembers
	 *        the user defined number of subtasks, -1 if the user did not specify the number
	 * @param instanceType
	 *        the instance type to be used for execution vertices this group vertex manages.
	 * @param userDefinedInstanceType
	 *        <code>true</code> if the instance type is user defined, <code>false</code> otherwise
	 * @param numberOfSubtasksPerInstance
	 *        the user defined number of subtasks per instance, -1 if the user did not specify the number
	 * @param userDefinedVertexToShareInstanceWith
	 *        <code>true</code> if the user specified another vertex to share instances with, <code>false</code>
	 *        otherwise
	 * @param numberOfExecutionRetries
	 *        the number of retries in case of an error before the task represented by this vertex is considered as
	 *        failed, -1 if the user did not specify the number
	 * @param configuration
	 *        the vertex's configuration object
	 * @param signature
	 *        the cryptographic signature of the vertex
	 * @param invokableClass
	 *        the task class that is assigned to execution vertices of this group
	 * @throws Exception
	 *         throws if an error occurs while instantiating the {@link AbstractInvokable}
	 */
	public ExecutionGroupVertex(final String name, final JobVertexID jobVertexID, final ExecutionGraph executionGraph,
			final int userDefinedNumberOfMembers, final InstanceType instanceType,
			final boolean userDefinedInstanceType, final int numberOfSubtasksPerInstance,
			final boolean userDefinedVertexToShareInstanceWith, final int numberOfExecutionRetries,
			final Configuration configuration, final ExecutionSignature signature,
			final Class<? extends AbstractInvokable> invokableClass) throws Exception {

		this.name = (name != null) ? name : "";
		this.jobVertexID = jobVertexID;
		this.userDefinedNumberOfMembers = userDefinedNumberOfMembers;
		this.instanceType = instanceType;
		this.userDefinedInstanceType = userDefinedInstanceType;
		if (numberOfSubtasksPerInstance != -1) {
			this.numberOfSubtasksPerInstance = numberOfSubtasksPerInstance;
			this.userDefinedNumberOfSubtasksPerInstance = true;
		} else {
			this.numberOfSubtasksPerInstance = 1;
			this.userDefinedNumberOfSubtasksPerInstance = false;
		}
		if (numberOfExecutionRetries >= 0) {
			this.numberOfExecutionRetries = numberOfExecutionRetries;
		} else {
			this.numberOfExecutionRetries = DEFAULT_EXECUTION_RETRIES;
		}
		this.userDefinedVertexToShareInstancesWith = userDefinedVertexToShareInstanceWith;
		this.configuration = configuration;
		this.executionSignature = signature;

		this.invokableClass = invokableClass;

		this.environment = new RuntimeEnvironment(executionGraph.getJobID(), name, invokableClass, configuration,
			executionGraph.getJobConfiguration());
	}

	/**
	 * Returns the name of the group vertex, usually copied from the initial job vertex.
	 * 
	 * @return the name of the group vertex.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Returns the environment of the instantiated {@link AbstractInvokable} object.
	 * 
	 * @return the environment of the instantiated {@link AbstractInvokable} object
	 */
	public RuntimeEnvironment getEnvironment() {

		return this.environment;
	}

	/**
	 * Sets the execution stage this group vertex is associated with.
	 * 
	 * @param executionStage
	 *        The new execution stage.
	 */
	public void setExecutionStage(final ExecutionStage executionStage) {

		this.executionStage = executionStage;
	}

	/**
	 * Returns the execution stage this group vertex is associated with.
	 * 
	 * @return The execution stage this vertex is associated with.
	 */
	public ExecutionStage getExecutionStage() {

		return this.executionStage;
	}

	/**
	 * Adds the initial execution vertex to this group vertex.
	 * 
	 * @param ev
	 *        The new execution vertex to be added.
	 */
	void addInitialSubtask(final ExecutionVertex ev) {

		if (ev == null) {
			throw new IllegalArgumentException("Argument ev must not be null");
		}

		if (this.initialGroupMemberAdded.compareAndSet(false, true)) {
			this.groupMembers.add(ev);
		}
	}

	/**
	 * Returns a specific execution vertex from the list of members.
	 * 
	 * @param pos
	 *        The position of the execution vertex to be returned.
	 * @return The execution vertex at position <code>pos</code> of the member list, <code>null</code> if there is no
	 *         such position.
	 */
	public ExecutionVertex getGroupMember(final int pos) {

		if (pos < 0) {
			throw new IllegalArgumentException("Argument pos must be greater or equal to 0");
		}

		try {
			return this.groupMembers.get(pos);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
	}

	/**
	 * Sets the maximum number of members this group vertex can have.
	 * 
	 * @param maxSize
	 *        the maximum number of members this group vertex can have
	 */
	void setMaxMemberSize(final int maxSize) {

		// TODO: Add checks here

		this.maxMemberSize = maxSize;
	}

	/**
	 * Sets the minimum number of members this group vertex must have.
	 * 
	 * @param minSize
	 *        the minimum number of members this group vertex must have
	 */
	void setMinMemberSize(final int minSize) {

		// TODO: Add checks here

		this.minMemberSize = minSize;
	}

	/**
	 * Returns the current number of members this group vertex has.
	 * 
	 * @return the current number of members this group vertex has
	 */
	public int getCurrentNumberOfGroupMembers() {

		return this.groupMembers.size();
	}

	/**
	 * Returns the maximum number of members this group vertex can have.
	 * 
	 * @return the maximum number of members this group vertex can have
	 */
	public int getMaximumNumberOfGroupMembers() {
		return this.maxMemberSize;
	}

	/**
	 * Returns the minimum number of members this group vertex must have.
	 * 
	 * @return the minimum number of members this group vertex must have
	 */
	public int getMinimumNumberOfGroupMember() {
		return this.minMemberSize;
	}

	/**
	 * Wires this group vertex to the specified group vertex and creates
	 * a back link.
	 * 
	 * @param groupVertex
	 *        the group vertex that should be the target of the wiring
	 * @param indexOfInputGate
	 *        the index of the consuming task's input gate
	 * @param indexOfOutputGate
	 *        the index of the producing tasks's output gate
	 * @param channelType
	 *        the channel type to be used for this edge
	 * @param userDefinedChannelType
	 *        <code>true</code> if the channel type is user defined, <code>false</code> otherwise
	 * @param compressionLevel
	 *        the compression level to be used for this edge
	 * @param userDefinedCompressionLevel
	 *        <code>true</code> if the compression level is user defined, <code>false</code> otherwise
	 * @param distributionPattern
	 *        the distribution pattern to create the wiring between the group members
	 * @param isBroadcast
	 *        indicates that the edge is part of broadcast group
	 * @return the created edge.
	 */
	ExecutionGroupEdge wireTo(final ExecutionGroupVertex groupVertex, final int indexOfInputGate,
			final int indexOfOutputGate, final ChannelType channelType, final boolean userDefinedChannelType,
			final CompressionLevel compressionLevel, final boolean userDefinedCompressionLevel,
			final DistributionPattern distributionPattern, final boolean isBroadcast) throws GraphConversionException {

		try {
			final ExecutionGroupEdge previousEdge = this.forwardLinks.get(indexOfOutputGate);
			if (previousEdge != null) {
				throw new GraphConversionException("Output gate " + indexOfOutputGate + " of" + getName()
						+ " already has an outgoing edge");
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			// Ignore exception
		}

		final ExecutionGroupEdge edge = new ExecutionGroupEdge(this, indexOfOutputGate, groupVertex, indexOfInputGate,
			channelType, userDefinedChannelType, compressionLevel, userDefinedCompressionLevel, distributionPattern,
			isBroadcast);

		this.forwardLinks.add(edge);

		groupVertex.wireBackLink(edge);

		return edge;
	}

	/**
	 * Checks if this group vertex is wired to the given group vertex.
	 * 
	 * @param groupVertex
	 *        the group vertex to check for
	 * @return <code>true</code> if there is a wire from the current group vertex to the specified group vertex,
	 *         otherwise <code>false</code>
	 */
	boolean isWiredTo(final ExecutionGroupVertex groupVertex) {

		final Iterator<ExecutionGroupEdge> it = this.forwardLinks.iterator();
		while (it.hasNext()) {
			final ExecutionGroupEdge edge = it.next();
			if (edge.getTargetVertex() == groupVertex) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Creates a back link from the current group vertex to the specified group vertex.
	 * 
	 * @param groupVertex
	 *        the target of the back link
	 */
	private void wireBackLink(final ExecutionGroupEdge edge) {

		this.backwardLinks.add(edge);
	}

	/**
	 * Returns the number of forward links the current group vertex has.
	 * 
	 * @return the number of forward links the current group vertex has
	 */
	public int getNumberOfForwardLinks() {

		return this.forwardLinks.size();
	}

	/**
	 * Returns the number of backward links the current group vertex has.
	 * 
	 * @return the number of backward links the current group vertex has
	 */
	public int getNumberOfBackwardLinks() {

		return this.backwardLinks.size();
	}

	/**
	 * Returns the number of the stage this group vertex belongs to.
	 * 
	 * @return the number of the stage this group vertex belongs to
	 */
	public int getStageNumber() {

		return this.executionStage.getStageNumber();
	}

	/**
	 * Creates the initial execution vertices managed by this group vertex.
	 * 
	 * @param initialNumberOfVertices
	 *        the initial number of execution vertices
	 * @throws GraphConversionException
	 *         thrown if the number of execution vertices for this group vertex cannot be set to the desired value
	 */
	void createInitialExecutionVertices(final int initalNumberOfVertices) throws GraphConversionException {

		// If the requested number of group vertices does not change, do nothing
		if (initalNumberOfVertices == this.getCurrentNumberOfGroupMembers()) {
			return;
		}

		// Make sure the method is only called for the initial setup of the graph
		if (this.getCurrentNumberOfGroupMembers() != 1) {
			throw new IllegalStateException(
				"This method can only be called for the initial setup of the execution graph");
		}

		// If the number of group vertices is user defined, prevent overwriting
		if (this.userDefinedNumberOfMembers != -1) {
			if (this.userDefinedNumberOfMembers == getCurrentNumberOfGroupMembers()) { // Note that
				// this.userDefinedNumberOfMembers
				// is final and requires no
				// locking!
				throw new GraphConversionException("Cannot overwrite user defined number of group members");
			}
		}

		// Make sure the value of newNumber is valid
		// TODO: Move these checks to some other place
		/*
		 * if (this.getMinimumNumberOfGroupMember() < 1) {
		 * throw new GraphConversionException("The minimum number of members is below 1 for group vertex "
		 * + this.getName());
		 * }
		 * if ((this.getMaximumNumberOfGroupMembers() != -1)
		 * && (this.getMaximumNumberOfGroupMembers() < this.getMinimumNumberOfGroupMember())) {
		 * throw new GraphConversionException(
		 * "The maximum number of members is smaller than the minimum for group vertex " + this.getName());
		 * }
		 */

		if (initalNumberOfVertices < this.getMinimumNumberOfGroupMember()) {
			throw new GraphConversionException("Number of members must be at least "
				+ this.getMinimumNumberOfGroupMember());
		}

		if ((this.getMaximumNumberOfGroupMembers() != -1)
			&& (initalNumberOfVertices > this.getMaximumNumberOfGroupMembers())) {
			throw new GraphConversionException("Number of members cannot exceed "
				+ this.getMaximumNumberOfGroupMembers());
		}

		final ExecutionVertex originalVertex = this.getGroupMember(0);
		int currentNumberOfExecutionVertices = this.getCurrentNumberOfGroupMembers();

		while (currentNumberOfExecutionVertices++ < initalNumberOfVertices) {

			final ExecutionVertex vertex = originalVertex.splitVertex();
			vertex.setAllocatedResource(new AllocatedResource(DummyInstance
				.createDummyInstance(this.instanceType), this.instanceType, null));
			this.groupMembers.add(vertex);
		}

		// Update the index and size information attached to the vertices
		int index = 0;
		final Iterator<ExecutionVertex> it = this.groupMembers.iterator();
		while (it.hasNext()) {
			final ExecutionVertex vertex = it.next();
			vertex.setIndexInVertexGroup(index++);
		}
	}

	/**
	 * Sets the input splits that should be assigned to this group vertex.
	 * 
	 * @param inputSplits
	 *        the input splits that shall be assigned to this group vertex
	 */
	public void setInputSplits(final InputSplit[] inputSplits) {

		this.inputSplits = inputSplits;
	}

	/**
	 * Returns the input splits assigned to this group vertex.
	 * 
	 * @return the input splits, possibly <code>null</code> if the group vertex does not represent an input vertex
	 */
	public InputSplit[] getInputSplits() {

		return this.inputSplits;
	}

	public ExecutionGroupEdge getForwardEdge(int index) {

		if (index < 0) {
			throw new IllegalArgumentException("Argument index must be greater than or equal to 0");
		}

		try {
			return this.forwardLinks.get(index);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
	}

	public ExecutionGroupEdge getBackwardEdge(int index) {

		if (index < 0) {
			throw new IllegalArgumentException("Argument index must be greater than or equal to 0");
		}

		try {
			return this.backwardLinks.get(index);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
	}

	public List<ExecutionGroupEdge> getForwardEdges(final ExecutionGroupVertex groupVertex) {

		if (groupVertex == null) {
			throw new IllegalArgumentException("Argument groupVertex must not be null");
		}

		final List<ExecutionGroupEdge> edges = new ArrayList<ExecutionGroupEdge>();

		final Iterator<ExecutionGroupEdge> it = this.forwardLinks.iterator();
		while (it.hasNext()) {

			final ExecutionGroupEdge edge = it.next();
			if (edge.getTargetVertex() == groupVertex) {
				edges.add(edge);
			}
		}

		return edges;
	}

	public List<ExecutionGroupEdge> getBackwardEdges(final ExecutionGroupVertex groupVertex) {

		if (groupVertex == null) {
			throw new IllegalArgumentException("Argument groupVertex must not be null");
		}

		final List<ExecutionGroupEdge> edges = new ArrayList<ExecutionGroupEdge>();

		final Iterator<ExecutionGroupEdge> it = this.backwardLinks.iterator();
		while (it.hasNext()) {

			final ExecutionGroupEdge edge = it.next();
			if (edge.getSourceVertex() == groupVertex) {
				edges.add(edge);
			}
		}

		return edges;
	}

	boolean isNumberOfMembersUserDefined() {

		return (this.userDefinedNumberOfMembers == -1) ? false : true;
	}

	int getUserDefinedNumberOfMembers() {

		return this.userDefinedNumberOfMembers;
	}

	boolean isInstanceTypeUserDefined() {

		return this.userDefinedInstanceType;
	}

	void setInstanceType(final InstanceType instanceType) throws GraphConversionException {

		if (instanceType == null) {
			throw new IllegalArgumentException("Argument instanceType must not be null");
		}

		if (this.userDefinedInstanceType) {
			throw new GraphConversionException("Cannot overwrite user defined instance type "
				+ instanceType.getIdentifier());
		}

		this.instanceType = instanceType;

		// Reset instance allocation of all members and let reassignInstances do the work
		for (int i = 0; i < this.groupMembers.size(); i++) {
			final ExecutionVertex vertex = this.groupMembers.get(i);
			vertex.setAllocatedResource(null);
		}
	}

	InstanceType getInstanceType() {
		return this.instanceType;
	}

	boolean isNumberOfSubtasksPerInstanceUserDefined() {

		return this.userDefinedNumberOfSubtasksPerInstance;
	}

	void setNumberOfSubtasksPerInstance(final int numberOfSubtasksPerInstance) throws GraphConversionException {

		if (this.userDefinedNumberOfSubtasksPerInstance
			&& (numberOfSubtasksPerInstance != this.numberOfSubtasksPerInstance)) {
			throw new GraphConversionException("Cannot overwrite user defined number of subtasks per instance");
		}

		this.numberOfSubtasksPerInstance = numberOfSubtasksPerInstance;
	}

	int getNumberOfSubtasksPerInstance() {
		return this.numberOfSubtasksPerInstance;
	}

	/**
	 * Returns the number of retries in case of an error before the task represented by this vertex is considered as
	 * failed.
	 * 
	 * @return the number of retries in case of an error before the task represented by this vertex is considered as
	 *         failed
	 */
	int getNumberOfExecutionRetries() {
		return this.numberOfExecutionRetries;
	}

	void shareInstancesWith(final ExecutionGroupVertex groupVertex) throws GraphConversionException {

		if (this.userDefinedVertexToShareInstancesWith && this.vertexToShareInstancesWith.get() != null) {
			throw new GraphConversionException("Cannot overwrite user defined vertex to share instances with");
		}

		if (groupVertex == null) {
			throw new IllegalArgumentException("shareInstancesWith: argument is null!");
		}

		final ExecutionGroupVertex oldVertex = this.vertexToShareInstancesWith.getAndSet(groupVertex);
		if (oldVertex != null) {
			oldVertex.removeFromVerticesSharingInstances(this);
		}

		groupVertex.addToVerticesSharingInstances(this);
	}

	boolean isVertexToShareInstanceWithUserDefined() {

		return this.userDefinedVertexToShareInstancesWith;
	}

	/**
	 * Returns the configuration object of the original job vertex.
	 * 
	 * @return the configuration object of the original job vertex
	 */
	public Configuration getConfiguration() {

		return this.configuration;
	}

	/**
	 * Returns the execution signature of this vertex.
	 * 
	 * @return the execution signature of this vertex
	 */
	public ExecutionSignature getExecutionSignature() {

		return this.executionSignature;
	}

	private void addToVerticesSharingInstances(final ExecutionGroupVertex groupVertex) {

		if (groupVertex == null) {
			throw new IllegalArgumentException("Argument groupVertex must not be null");
		}

		this.verticesSharingInstances.addIfAbsent(groupVertex);
	}

	private void removeFromVerticesSharingInstances(final ExecutionGroupVertex groupVertex) {

		if (groupVertex == null) {
			throw new IllegalArgumentException("Argument groupVertex must not be null");
		}

		this.verticesSharingInstances.remove(groupVertex);

	}

	void repairSubtasksPerInstance() {

		final Iterator<ExecutionVertex> it = this.groupMembers.iterator();
		int count = 0;
		while (it.hasNext()) {

			final ExecutionVertex v = it.next();
			v.setAllocatedResource(this.groupMembers.get(
				(count++ / this.numberOfSubtasksPerInstance) * this.numberOfSubtasksPerInstance)
				.getAllocatedResource());
		}
	}

	void repairInstanceSharing(final Set<AllocatedResource> availableResources) {

		// Number of required resources by this group vertex
		final int numberOfRequiredInstances = (this.groupMembers.size() / this.numberOfSubtasksPerInstance)
			+ (((this.groupMembers.size() % this.numberOfSubtasksPerInstance) != 0) ? 1 : 0);

		// Number of resources to be replaced
		final int resourcesToBeReplaced = Math.min(availableResources.size(), numberOfRequiredInstances);

		// Build the replacement map if necessary
		final Map<AllocatedResource, AllocatedResource> replacementMap = new HashMap<AllocatedResource, AllocatedResource>();

		if (resourcesToBeReplaced > 0) {

			final Iterator<ExecutionVertex> vertexIt = this.groupMembers.iterator();
			final Iterator<AllocatedResource> resourceIt = availableResources.iterator();

			while (replacementMap.size() < resourcesToBeReplaced) {

				if (!vertexIt.hasNext()) {
					break;
				}

				if (!resourceIt.hasNext()) {
					break;
				}

				final ExecutionVertex vertex = vertexIt.next();
				final AllocatedResource originalResource = vertex.getAllocatedResource();

				if (!replacementMap.containsKey(originalResource)) {

					final AllocatedResource replacementResource = resourceIt.next();
					replacementMap.put(originalResource, replacementResource);
				}
			}
		}

		// Now replace the instances
		final Iterator<ExecutionVertex> vertexIt = this.groupMembers.iterator();
		while (vertexIt.hasNext()) {

			final ExecutionVertex vertex = vertexIt.next();
			final AllocatedResource originalResource = vertex.getAllocatedResource();
			final AllocatedResource replacementResource = replacementMap.get(originalResource);
			if (replacementResource != null) {
				vertex.setAllocatedResource(replacementResource);
			} else {
				availableResources.add(originalResource);
			}
		}

		final Iterator<ExecutionGroupVertex> groupVertexIt = this.verticesSharingInstances.iterator();
		while (groupVertexIt.hasNext()) {
			groupVertexIt.next().repairInstanceSharing(availableResources);
		}
	}

	/**
	 * Checks if this vertex is an input vertex in its stage, i.e. has either no
	 * incoming connections or only incoming connections to group vertices in a lower stage.
	 * 
	 * @return <code>true</code> if this vertex is an input vertex, <code>false</code> otherwise
	 */
	public boolean isInputVertex() {

		if (this.backwardLinks.size() == 0) {
			return true;
		}

		final Iterator<ExecutionGroupEdge> it = this.backwardLinks.iterator();
		while (it.hasNext()) {
			if (it.next().getSourceVertex().getStageNumber() == this.getStageNumber()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks if this vertex is an output vertex in its stage, i.e. has either no
	 * outgoing connections or only outgoing connections to group vertices in a higher stage.
	 * 
	 * @return <code>true</code> if this vertex is an output vertex, <code>false</code> otherwise
	 */
	public boolean isOutputVertex() {

		if (this.forwardLinks.size() == 0) {
			return true;
		}

		final Iterator<ExecutionGroupEdge> it = this.forwardLinks.iterator();
		while (it.hasNext()) {
			if (it.next().getTargetVertex().getStageNumber() == this.getStageNumber()) {
				return false;
			}
		}

		return true;
	}

	public ExecutionGroupVertex getVertexToShareInstancesWith() {
		return this.vertexToShareInstancesWith.get();
	}

	/**
	 * Returns the ID of the job vertex which is represented by
	 * this group vertex.
	 * 
	 * @return the ID of the job vertex which is represented by
	 *         this group vertex
	 */
	public JobVertexID getJobVertexID() {

		return this.jobVertexID;
	}

	/**
	 * Returns an iterator over all members of this execution group vertex.
	 * 
	 * @return an iterator over all members of this execution group vertex
	 */
	public Iterator<ExecutionVertex> iterator() {

		return this.groupMembers.iterator();
	}

	/**
	 * Recursive method to calculate the connection IDs of the {@link ExecutionGraph}.
	 * 
	 * @param currentConnectionID
	 *        the current connection ID
	 * @param alreadyVisited
	 *        the set of already visited group vertices
	 */
	void calculateConnectionID(final int currentConnectionID, final Set<ExecutionGroupVertex> alreadyVisited) {

		if (!alreadyVisited.add(this)) {
			return;
		}

		int nextConnectionID = currentConnectionID;
		for (final ExecutionGroupEdge backwardLink : this.backwardLinks) {
			backwardLink.setConnectionID(nextConnectionID);
			backwardLink.getSourceVertex().calculateConnectionID(nextConnectionID, alreadyVisited);
			++nextConnectionID;
		}
	}

	/**
	 * Calculates and returns initial checkpoint state of the vertex group.
	 * 
	 * @return the initial checkpoint state of the vertex group
	 */
	CheckpointState checkInitialCheckpointState() {

		final CheckpointMode cpm = CheckpointUtils.getCheckpointMode();

		if (cpm == CheckpointMode.ALWAYS) {

			// Always create checkpoints
			return CheckpointState.PARTIAL;

		} else if (cpm == CheckpointMode.NEVER) {

			// Check if vertex has a file channel
			for (int i = 0; i < this.forwardLinks.size(); ++i) {
				if (this.forwardLinks.get(i).getChannelType() == ChannelType.FILE) {
					return CheckpointState.PARTIAL;
				}
			}

			// Look for a user annotation
			final ForceCheckpoint forcedCheckpoint = this.environment.getInvokable().getClass()
				.getAnnotation(ForceCheckpoint.class);

			// No user annotation, go with the default configuration
			if (forcedCheckpoint == null) {
				return CheckpointState.NONE;
			}

			// User enforced checkpoint
			if (forcedCheckpoint.checkpoint()) {
				return CheckpointState.PARTIAL;
			}

			return CheckpointState.NONE;

		} else if (cpm == CheckpointMode.NETWORK) {

			// Check if vertex has a file channel or network channel
			for (int i = 0; i < this.forwardLinks.size(); ++i) {
				final ChannelType channelType = this.forwardLinks.get(i).getChannelType();
				if (channelType == ChannelType.FILE || channelType == ChannelType.NETWORK) {
					return CheckpointState.PARTIAL;
				}
			}

			// Look for a user annotation
			final ForceCheckpoint forcedCheckpoint = this.environment.getInvokable().getClass()
				.getAnnotation(ForceCheckpoint.class);

			// No user annotation, go with the default configuration
			if (forcedCheckpoint == null) {
				return CheckpointState.NONE;
			}

			// User enforced checkpoint
			if (forcedCheckpoint.checkpoint()) {
				return CheckpointState.PARTIAL;
			}

			return CheckpointState.NONE;

		} else {

			// Check if vertex has a file channel
			for (int i = 0; i < this.forwardLinks.size(); ++i) {
				if (this.forwardLinks.get(i).getChannelType() == ChannelType.FILE) {
					return CheckpointState.PARTIAL;
				}
			}

			// Look for a user annotation
			final ForceCheckpoint forcedCheckpoint = this.environment.getInvokable().getClass()
				.getAnnotation(ForceCheckpoint.class);

			// No user annotation, go with the default configuration
			if (forcedCheckpoint == null) {
				return CheckpointState.UNDECIDED;
			}

			// User enforced checkpoint
			if (forcedCheckpoint.checkpoint()) {
				return CheckpointState.PARTIAL;
			}

			return CheckpointState.NONE;
		}
	}

	/**
	 * Returns the task class that is assigned to execution vertices of this group.
	 * 
	 * @return the task class that is assigned to execution vertices of this group
	 */
	Class<? extends AbstractInvokable> getInvokableClass() {

		return this.invokableClass;
	}
}
