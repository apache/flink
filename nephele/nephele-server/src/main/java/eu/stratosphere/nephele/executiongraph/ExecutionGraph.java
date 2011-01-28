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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionSignature;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelSetupException;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkOutputChannel;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobEdge;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * In Nephele an execution graph is the main data structure for scheduling, executing and
 * observing a job. An execution graph is created from an job graph. In contrast to a job graph
 * it can contain communication edges of specific types, sub groups of vertices and information on
 * when and where (on which instance) to run particular tasks.
 * 
 * @author warneke
 */
public class ExecutionGraph implements ExecutionListener {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionGraph.class);

	/**
	 * The ID of the job this graph has been built for.
	 */
	private final JobID jobID;

	/**
	 * The name of the original job graph.
	 */
	private final String jobName;

	/**
	 * Mapping of channel IDs to execution vertices.
	 */
	private final Map<ChannelID, ExecutionVertex> channelToVertexMap = new HashMap<ChannelID, ExecutionVertex>();

	/**
	 * Mapping of channel IDs to input channels.
	 */
	private final Map<ChannelID, AbstractInputChannel<? extends Record>> inputChannelMap = new HashMap<ChannelID, AbstractInputChannel<? extends Record>>();

	/**
	 * Mapping of channel IDs to output channels.
	 */
	private final Map<ChannelID, AbstractOutputChannel<? extends Record>> outputChannelMap = new HashMap<ChannelID, AbstractOutputChannel<? extends Record>>();

	/**
	 * List of stages in the graph.
	 */
	private final List<ExecutionStage> stages = new ArrayList<ExecutionStage>();

	/**
	 * Index to the current execution stage.
	 */
	private int indexToCurrentExecutionStage = 0;

	/**
	 * The job configuration that was originally attached to the JobGraph.
	 */
	private Configuration jobConfiguration;

	/**
	 * The current status of the job which is represented by this execution graph.
	 */
	private JobStatus jobStatus = JobStatus.CREATED;

	/**
	 * List of listeners which are notified in case the status of this job has changed.
	 */
	private List<JobStatusListener> jobStatusListeners = new ArrayList<JobStatusListener>();

	/**
	 * Private constructor used for duplicating execution vertices.
	 * 
	 * @param jobID
	 *        the ID of the duplicated execution graph
	 * @param jobName
	 *        the name of the original job graph
	 */
	private ExecutionGraph(JobID jobID, String jobName) {
		this.jobID = jobID;
		this.jobName = jobName;
	}

	/**
	 * Creates a new execution graph from a job graph.
	 * 
	 * @param job
	 *        the user's job graph
	 * @param instanceManager
	 *        the instance manager
	 * @throws GraphConversionException
	 *         thrown if the job graph is not valid and no execution graph can be constructed from it
	 */
	public ExecutionGraph(JobGraph job, InstanceManager instanceManager)
																		throws GraphConversionException {
		this(job.getJobID(), job.getName());
		// Start constructing the new execution graph from given job graph
		constructExecutionGraph(job, instanceManager);
	}

	/**
	 * Applies the user defined settings to the execution graph.
	 * 
	 * @param temporaryGroupVertexMap
	 *        mapping between job vertices and the corresponding group vertices.
	 * @throws GraphConversionException
	 *         thrown if an error occurs while applying the user settings.
	 */
	private void applyUserDefinedSettings(HashMap<AbstractJobVertex, ExecutionGroupVertex> temporaryGroupVertexMap)
			throws GraphConversionException {

		// The check for cycles in the dependency chain for instance sharing is already checked in
		// <code>submitJob</code> method of the job manager

		// If there is no cycle, apply the settings to the corresponding group vertices
		Iterator<AbstractJobVertex> it = temporaryGroupVertexMap.keySet().iterator();
		while (it.hasNext()) {

			final AbstractJobVertex jobVertex = it.next();
			if (jobVertex.getVertexToShareInstancesWith() != null) {

				final AbstractJobVertex vertexToShareInstancesWith = jobVertex.getVertexToShareInstancesWith();
				final ExecutionGroupVertex groupVertex = temporaryGroupVertexMap.get(jobVertex);
				final ExecutionGroupVertex groupVertexToShareInstancesWith = temporaryGroupVertexMap
					.get(vertexToShareInstancesWith);
				groupVertex.shareInstancesWith(groupVertexToShareInstancesWith);
			}
		}

		// Second, we create the number of members each group vertex is supposed to have
		Iterator<ExecutionGroupVertex> it2 = new ExecutionGroupVertexIterator(this, true, -1);
		while (it2.hasNext()) {

			final ExecutionGroupVertex groupVertex = it2.next();
			if (groupVertex.isNumberOfMembersUserDefined()) {
				groupVertex.changeNumberOfGroupMembers(groupVertex.getUserDefinedNumberOfMembers());
			}
		}
		repairInstanceAssignment();

		// Finally, apply the channel settings channel settings
		it2 = new ExecutionGroupVertexIterator(this, true, -1);
		while (it2.hasNext()) {

			final ExecutionGroupVertex groupVertex = it2.next();
			for (int i = 0; i < groupVertex.getNumberOfForwardLinks(); i++) {

				final ExecutionGroupEdge edge = groupVertex.getForwardEdge(i);
				if (edge.isChannelTypeUserDefined()) {
					edge.changeChannelType(edge.getChannelType());
				}
				if (edge.isCompressionLevelUserDefined()) {
					edge.changeCompressionLevel(edge.getCompressionLevel());
				}
			}
		}

		// TODO: Check if calling this is really necessary, if not set visibility of reassignInstances back to protected
		it2 = new ExecutionGroupVertexIterator(this, true, -1);
		while (it2.hasNext()) {
			final ExecutionGroupVertex groupVertex = it2.next();
			if (groupVertex.getVertexToShareInstancesWith() == null) {
				groupVertex.reassignInstances();
				this.repairInstanceAssignment();
			}
		}
	}

	/**
	 * Sets up an execution graph from a job graph.
	 * 
	 * @param jobGraph
	 *        the job graph to create the execution graph from
	 * @param instanceManager
	 *        the instance manager
	 * @throws GraphConversionException
	 *         thrown if the job graph is not valid and no execution graph can be constructed from it
	 */
	private void constructExecutionGraph(JobGraph jobGraph, InstanceManager instanceManager)
			throws GraphConversionException {

		// Clean up temporary data structures
		final HashMap<AbstractJobVertex, ExecutionVertex> temporaryVertexMap = new HashMap<AbstractJobVertex, ExecutionVertex>();
		final HashMap<AbstractJobVertex, ExecutionGroupVertex> temporaryGroupVertexMap = new HashMap<AbstractJobVertex, ExecutionGroupVertex>();

		// First, store job configuration
		this.jobConfiguration = jobGraph.getJobConfiguration();

		// Initially, create only one execution stage that contains all group vertices
		final ExecutionStage initialExecutionStage = new ExecutionStage(0);
		this.stages.add(initialExecutionStage);

		// Convert job vertices to execution vertices and initialize them
		final AbstractJobVertex[] all = jobGraph.getAllJobVertices();
		for (int i = 0; i < all.length; i++) {
			final ExecutionVertex createdVertex = createVertex(all[i], instanceManager, initialExecutionStage);
			temporaryVertexMap.put(all[i], createdVertex);
			temporaryGroupVertexMap.put(all[i], createdVertex.getGroupVertex());
		}

		// Create initial network channel for every vertex
		for (int i = 0; i < all.length; i++) {
			createInitialChannels(all[i], temporaryVertexMap);
		}

		// Now that an initial graph is built, apply the user settings
		applyUserDefinedSettings(temporaryGroupVertexMap);
	}

	/**
	 * Creates the initial channels between all connected job vertices.
	 * 
	 * @param jobVertex
	 *        the job vertex from which the wiring is determined
	 * @param vertexMap
	 *        a temporary vertex map
	 * @throws GraphConversionException
	 *         if the initial wiring cannot be created
	 */
	private void createInitialChannels(AbstractJobVertex jobVertex,
			HashMap<AbstractJobVertex, ExecutionVertex> vertexMap) throws GraphConversionException {

		ExecutionVertex ev;
		if (!vertexMap.containsKey(jobVertex)) {
			throw new GraphConversionException("Cannot find mapping for vertex " + jobVertex.getName());
		}

		ev = vertexMap.get(jobVertex);

		// First compare number of output gates
		if (jobVertex.getNumberOfForwardConnections() != ev.getEnvironment().getNumberOfOutputGates()) {
			throw new GraphConversionException("Job and execution vertex " + jobVertex.getName()
				+ " have different number of outputs");
		}

		if (jobVertex.getNumberOfBackwardConnections() != ev.getEnvironment().getNumberOfInputGates()) {
			throw new GraphConversionException("Job and execution vertex " + jobVertex.getName()
				+ " have different number of inputs");
		}

		// Now assign identifiers to gates and check type
		for (int j = 0; j < jobVertex.getNumberOfForwardConnections(); j++) {
			final JobEdge edge = jobVertex.getForwardConnection(j);
			final AbstractJobVertex target = edge.getConnectedVertex();

			// find output gate of execution vertex
			final OutputGate<? extends Record> eog = ev.getEnvironment().getOutputGate(j);
			if (eog == null) {
				throw new GraphConversionException("Cannot retrieve output gate " + j + " from vertex "
					+ jobVertex.getName());
			}

			final ExecutionVertex executionTarget = vertexMap.get(target);
			if (executionTarget == null) {
				throw new GraphConversionException("Cannot find mapping for vertex " + target.getName());
			}

			final InputGate<? extends Record> eig = executionTarget.getEnvironment().getInputGate(
				edge.getIndexOfInputGate());
			if (eig == null) {
				throw new GraphConversionException("Cannot retrieve input gate " + edge.getIndexOfInputGate()
					+ " from vertex " + target.getName());
			}

			ChannelType channelType = ChannelType.NETWORK;
			CompressionLevel compressionLevel = CompressionLevel.NO_COMPRESSION;
			boolean userDefinedChannelType = false;
			boolean userDefinedCompressionLevel = false;

			// Create a network channel with no compression by default, user settings will be applied later on
			createChannel(ev, eog, executionTarget, eig, channelType, compressionLevel);

			if (edge.getChannelType() != null) {
				channelType = edge.getChannelType();
				userDefinedChannelType = true;
			}

			if (edge.getCompressionLevel() != null) {
				compressionLevel = edge.getCompressionLevel();
				userDefinedCompressionLevel = true;
			}

			// Connect the corresponding group vertices and copy the user settings from the job edge
			ev.getGroupVertex().wireTo(executionTarget.getGroupVertex(), edge.getIndexOfInputGate(), j, channelType,
				userDefinedChannelType, compressionLevel, userDefinedCompressionLevel);

		}
	}

	/**
	 * Destroys all the channels originating from the source vertex at the given output gate and arriving at the target
	 * vertex at the given
	 * input gate. All destroyed channels are completely unregistered with the {@link ExecutionGraph}.
	 * 
	 * @param source
	 *        the source vertex the channels to be removed originate from
	 * @param indexOfOutputGate
	 *        the index of the output gate the channels to be removed are assigned to
	 * @param target
	 *        the target vertex the channels to be removed arrive
	 * @param indexOfInputGate
	 *        the index of the input gate the channels to be removed are assigned to
	 * @throws GraphConversionException
	 *         thrown if an inconsistency during the unwiring process occurs
	 */
	public void unwire(ExecutionGroupVertex source, int indexOfOutputGate, ExecutionGroupVertex target,
			int indexOfInputGate) throws GraphConversionException {

		// Unwire the respective gate of the source vertices
		for (int i = 0; i < source.getCurrentNumberOfGroupMembers(); i++) {

			final ExecutionVertex sourceVertex = source.getGroupMember(i);
			final OutputGate<? extends Record> outputGate = sourceVertex.getEnvironment().getOutputGate(
				indexOfOutputGate);
			if (outputGate == null) {
				throw new GraphConversionException("unwire: " + sourceVertex.getName()
					+ " has no output gate with index " + indexOfOutputGate);
			}

			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); j++) {
				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
				this.outputChannelMap.remove(outputChannel.getID());
				this.channelToVertexMap.remove(outputChannel.getID());
			}

			outputGate.removeAllOutputChannels();
		}

		// Unwire the respective gate of the target vertices
		for (int i = 0; i < target.getCurrentNumberOfGroupMembers(); i++) {

			final ExecutionVertex targetVertex = target.getGroupMember(i);
			final InputGate<? extends Record> inputGate = targetVertex.getEnvironment().getInputGate(indexOfInputGate);
			if (inputGate == null) {
				throw new GraphConversionException("unwire: " + targetVertex.getName()
					+ " has no input gate with index " + indexOfInputGate);
			}

			for (int j = 0; j < inputGate.getNumberOfInputChannels(); j++) {
				final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(j);
				this.inputChannelMap.remove(inputChannel.getID());
				this.channelToVertexMap.remove(inputChannel.getID());
			}

			inputGate.removeAllInputChannels();
		}
	}

	public void wire(ExecutionGroupVertex source, int indexOfOutputGate, ExecutionGroupVertex target,
			int indexOfInputGate, ChannelType channelType, CompressionLevel compressionLevel)
			throws GraphConversionException {

		// Unwire the respective gate of the source vertices
		for (int i = 0; i < source.getCurrentNumberOfGroupMembers(); i++) {

			final ExecutionVertex sourceVertex = source.getGroupMember(i);
			final OutputGate<? extends Record> outputGate = sourceVertex.getEnvironment().getOutputGate(
				indexOfOutputGate);
			if (outputGate == null) {
				throw new GraphConversionException("wire: " + sourceVertex.getName()
					+ " has no output gate with index " + indexOfOutputGate);
			}
			if (outputGate.getNumberOfOutputChannels() > 0) {
				throw new GraphConversionException("wire: wire called on source " + sourceVertex.getName() + " (" + i
					+ "), but number of output channels is " + outputGate.getNumberOfOutputChannels() + "!");
			}

			for (int j = 0; j < target.getCurrentNumberOfGroupMembers(); j++) {

				final ExecutionVertex targetVertex = target.getGroupMember(j);
				final InputGate<? extends Record> inputGate = targetVertex.getEnvironment().getInputGate(
					indexOfInputGate);
				if (inputGate == null) {
					throw new GraphConversionException("wire: " + targetVertex.getName()
						+ " has no input gate with index " + indexOfInputGate);
				}
				if (inputGate.getNumberOfInputChannels() > 0 && i == 0) {
					throw new GraphConversionException("wire: wire called on target " + targetVertex.getName() + " ("
						+ j + "), but number of input channels is " + inputGate.getNumberOfInputChannels() + "!");
				}

				// Check if a wire is supposed to be created
				if (inputGate.getDistributionPattern().createWire(i, j, source.getCurrentNumberOfGroupMembers(),
					target.getCurrentNumberOfGroupMembers())) {
					createChannel(sourceVertex, outputGate, targetVertex, inputGate, channelType, compressionLevel);
				}
			}
		}

	}

	private void createChannel(ExecutionVertex source, OutputGate<? extends Record> outputGate, ExecutionVertex target,
			InputGate<? extends Record> inputGate, ChannelType channelType, CompressionLevel compressionLevel)
			throws GraphConversionException {

		AbstractOutputChannel<? extends Record> outputChannel;
		AbstractInputChannel<? extends Record> inputChannel;

		switch (channelType) {
		case NETWORK:
			outputChannel = outputGate.createNetworkOutputChannel(null, compressionLevel);
			inputChannel = inputGate.createNetworkInputChannel(null, compressionLevel);
			break;
		case INMEMORY:
			outputChannel = outputGate.createInMemoryOutputChannel(null, compressionLevel);
			inputChannel = inputGate.createInMemoryInputChannel(null, compressionLevel);
			break;
		case FILE:
			outputChannel = outputGate.createFileOutputChannel(null, compressionLevel);
			inputChannel = inputGate.createFileInputChannel(null, compressionLevel);
			break;
		default:
			throw new GraphConversionException("Cannot create channel: unknown type");
		}

		// Copy the number of the opposite channel
		inputChannel.setConnectedChannelID(outputChannel.getID());
		outputChannel.setConnectedChannelID(inputChannel.getID());

		this.outputChannelMap.put(outputChannel.getID(), outputChannel);
		this.inputChannelMap.put(inputChannel.getID(), inputChannel);
		this.channelToVertexMap.put(outputChannel.getID(), source);
		this.channelToVertexMap.put(inputChannel.getID(), target);
	}

	/**
	 * Creates an execution vertex from a job vertex.
	 * 
	 * @param jobVertex
	 *        the job vertex to create the execution vertex from
	 * @param instanceManager
	 *        the instanceManager
	 * @param initialExecutionStage
	 *        the initial execution stage all group vertices are added to
	 * @return the new execution vertex
	 * @throws GraphConversionException
	 *         thrown if the job vertex is of an unknown subclass
	 */
	private ExecutionVertex createVertex(AbstractJobVertex jobVertex, InstanceManager instanceManager,
			ExecutionStage initialExecutionStage) throws GraphConversionException {

		// If the user has requested instance type, check if the type is known by the current instance manager
		InstanceType instanceType = null;
		boolean userDefinedInstanceType = false;
		if (jobVertex.getInstanceType() != null) {

			userDefinedInstanceType = true;
			instanceType = instanceManager.getInstanceTypeByName(jobVertex.getInstanceType());
			if (instanceType == null) {
				throw new GraphConversionException("Requested instance type " + jobVertex.getInstanceType()
					+ " is not known to the instance manager");
			}
		}

		if (instanceType == null) {
			instanceType = instanceManager.getDefaultInstanceType();
		}

		// Calculate the cryptographic signature of this vertex
		final ExecutionSignature signature = ExecutionSignature.createSignature(jobVertex.getInvokableClass(),
			jobVertex.getJobGraph().getJobID());

		// Create a group vertex for the job vertex
		final ExecutionGroupVertex groupVertex = new ExecutionGroupVertex(jobVertex.getName(), jobVertex.getID(), this,
			jobVertex.getNumberOfSubtasks(), instanceType, userDefinedInstanceType, jobVertex
				.getNumberOfSubtasksPerInstance(), jobVertex.getVertexToShareInstancesWith() != null ? true : false,
			jobVertex.getConfiguration(), signature);
		// Create an initial execution vertex for the job vertex
		final Class<? extends AbstractInvokable> invokableClass = jobVertex.getInvokableClass();
		if (invokableClass == null) {
			throw new GraphConversionException("JobVertex " + jobVertex.getID() + " (" + jobVertex.getName()
				+ ") does not specify a task");
		}

		// Add group vertex to initial execution stage
		initialExecutionStage.addStageMember(groupVertex);

		final ExecutionVertex ev = new ExecutionVertex(jobVertex.getJobGraph().getJobID(), invokableClass, this,
			groupVertex);

		// Run the configuration check the user has provided for the vertex
		try {
			jobVertex.checkConfiguration(ev.getEnvironment().getInvokable());
		} catch (IllegalConfigurationException e) {
			throw new GraphConversionException(StringUtils.stringifyException(e));
		}

		// Check if the user's specifications for the number of subtasks are valid
		final int minimumNumberOfSubtasks = jobVertex.getMinimumNumberOfSubtasks(ev.getEnvironment().getInvokable());
		final int maximumNumberOfSubtasks = jobVertex.getMaximumNumberOfSubtasks(ev.getEnvironment().getInvokable());
		if (jobVertex.getNumberOfSubtasks() != -1) {
			if (jobVertex.getNumberOfSubtasks() < 1) {
				throw new GraphConversionException("Cannot split task " + jobVertex.getName() + " into "
					+ jobVertex.getNumberOfSubtasks() + " subtasks");
			}

			if (jobVertex.getNumberOfSubtasks() < minimumNumberOfSubtasks) {
				throw new GraphConversionException("Number of subtasks must be at least " + minimumNumberOfSubtasks);
			}

			if (maximumNumberOfSubtasks != -1) {
				if (jobVertex.getNumberOfSubtasks() > maximumNumberOfSubtasks) {
					throw new GraphConversionException("Number of subtasks for vertex " + jobVertex.getName()
						+ " can be at most " + maximumNumberOfSubtasks);
				}
			}
		}

		// Check number of subtasks per instance
		if (jobVertex.getNumberOfSubtasksPerInstance() != -1 && jobVertex.getNumberOfSubtasksPerInstance() < 1) {
			throw new GraphConversionException("Cannot set number of subtasks per instance to "
				+ jobVertex.getNumberOfSubtasksPerInstance() + " for vertex " + jobVertex.getName());
		}

		// Assign min/max to the group vertex (settings are actually applied in applyUserDefinedSettings)
		groupVertex.setMinMemberSize(minimumNumberOfSubtasks);
		groupVertex.setMaxMemberSize(maximumNumberOfSubtasks);

		// Assign initial instance to vertex (may be overwritten later on when user settings are applied)
		ev.setAllocatedResource(new AllocatedResource(DummyInstance.createDummyInstance(instanceType), null));

		// Register input and output vertices separately
		if (jobVertex instanceof JobInputVertex) {
			// Assign input splits
			try {
				groupVertex.setInputSplits(((JobInputVertex) jobVertex).getInputSplits());
			} catch (IllegalConfigurationException e) {
				throw new GraphConversionException("Cannot assign input splits to " + groupVertex.getName() + ": "
					+ StringUtils.stringifyException(e));
			}
		}
		// TODO: This is a quick workaround, problem can be solved in a more generic way
		if (jobVertex instanceof JobFileOutputVertex) {
			final JobFileOutputVertex jbov = (JobFileOutputVertex) jobVertex;
			jobVertex.getConfiguration().setString("outputPath", jbov.getFilePath().toString());
		}

		return ev;
	}

	/**
	 * Returns the number of input vertices registered with this execution graph.
	 * 
	 * @return the number of input vertices registered with this execution graph
	 */
	public int getNumberOfInputVertices() {

		return this.stages.get(0).getNumberOfInputExecutionVertices();
	}

	/**
	 * Returns the number of input vertices for the given stage.
	 * 
	 * @param stage
	 *        the index of the execution stage
	 * @return the number of input vertices for the given stage
	 */
	public int getNumberOfInputVertices(int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfInputExecutionVertices();
	}

	/**
	 * Returns the number of output vertices registered with this execution graph.
	 * 
	 * @return the number of output vertices registered with this execution graph
	 */
	public int getNumberOfOutputVertices() {

		return this.stages.get(0).getNumberOfOutputExecutionVertices();
	}

	/**
	 * Returns the number of output vertices for the given stage.
	 * 
	 * @param stage
	 *        the index of the execution stage
	 * @return the number of input vertices for the given stage
	 */
	public int getNumberOfOutputVertices(int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfOutputExecutionVertices();
	}

	/**
	 * Returns the input vertex with the specified index.
	 * 
	 * @param index
	 *        the index of the input vertex to return
	 * @return the input vertex with the specified index or <code>null</code> if no input vertex with such an index
	 *         exists
	 */
	public ExecutionVertex getInputVertex(int index) {

		return this.stages.get(0).getInputExecutionVertex(index);
	}

	/**
	 * Returns the output vertex with the specified index.
	 * 
	 * @param index
	 *        the index of the output vertex to return
	 * @return the output vertex with the specified index or <code>null</code> if no output vertex with such an index
	 *         exists
	 */
	public ExecutionVertex getOutputVertex(int index) {

		return this.stages.get(0).getOutputExecutionVertex(index);
	}

	/**
	 * Returns the input vertex with the specified index for the given stage
	 * 
	 * @param stage
	 *        the index of the stage
	 * @param index
	 *        the index of the input vertex to return
	 * @return the input vertex with the specified index or <code>null</code> if no input vertex with such an index
	 *         exists in that stage
	 */
	public ExecutionVertex getInputVertex(int stage, int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getInputExecutionVertex(index);
	}

	/**
	 * Returns the output vertex with the specified index for the given stage.
	 * 
	 * @param stage
	 *        the index of the stage
	 * @param index
	 *        the index of the output vertex to return
	 * @return the output vertex with the specified index or <code>null</code> if no output vertex with such an index
	 *         exists in that stage
	 */
	public ExecutionVertex getOutputVertex(int stage, int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getOutputExecutionVertex(index);
	}

	/**
	 * Returns the execution stage with number <code>num</code>.
	 * 
	 * @param num
	 *        the number of the execution stage to be returned
	 * @return the execution stage with number <code>num</code> or <code>null</code> if no such execution stage exists
	 */
	public ExecutionStage getStage(int num) {

		if (num < this.stages.size()) {
			return this.stages.get(num);
		}

		return null;
	}

	/**
	 * Returns the number of execution stages in the execution graph.
	 * 
	 * @return the number of execution stages in the execution graph
	 */
	public int getNumberOfStages() {

		return this.stages.size();
	}

	/**
	 * Identifies an execution by the specified channel ID and returns it.
	 * 
	 * @param id
	 *        the channel ID to identify the vertex with
	 * @return the execution vertex which has a channel with ID <code>id</code> or <code>null</code> if no such vertex
	 *         exists in the execution graph
	 */
	public ExecutionVertex getVertexByChannelID(ChannelID id) {

		if (!this.channelToVertexMap.containsKey(id)) {
			return null;
		}

		return this.channelToVertexMap.get(id);
	}

	/**
	 * Finds an input channel by its ID and returns it.
	 * 
	 * @param id
	 *        the channel ID to identify the input channel
	 * @return the input channel whose ID matches <code>id</code> or <code>null</code> if no such channel is known
	 */
	public AbstractInputChannel<? extends Record> getInputChannelByID(ChannelID id) {

		if (!this.inputChannelMap.containsKey(id)) {
			return null;
		}

		return this.inputChannelMap.get(id);
	}

	/**
	 * Finds an output channel by its ID and returns it.
	 * 
	 * @param id
	 *        the channel ID to identify the output channel
	 * @return the output channel whose ID matches <code>id</code> or <code>null</code> if no such channel is known
	 */
	public AbstractOutputChannel<? extends Record> getOutputChannelByID(ChannelID id) {

		if (!this.outputChannelMap.containsKey(id)) {
			return null;
		}

		return this.outputChannelMap.get(id);
	}

	/**
	 * Returns a (possibly empty) list of execution vertices which are currently assigned to the
	 * given allocated resource. The vertices in that list may have an arbitrary execution state.
	 * 
	 * @param allocatedResource
	 *        the allocated resource to check the assignment for
	 * @return a (possibly empty) list of execution vertices which are currently assigned to the given instance
	 */
	public synchronized List<ExecutionVertex> getVerticesAssignedToResource(AllocatedResource allocatedResource) {

		final List<ExecutionVertex> list = new ArrayList<ExecutionVertex>();

		if (allocatedResource == null) {
			return list;
		}

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);
		while (it.hasNext()) {
			final ExecutionVertex vertex = it.next();
			if (allocatedResource.equals(vertex.getAllocatedResource())) {
				list.add(vertex);
			}
		}

		return list;
	}

	public ExecutionVertex getVertexByID(ExecutionVertexID id) {

		if (id == null) {
			return null;
		}

		final ExecutionGraphIterator it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			if (vertex.getID().equals(id)) {
				return vertex;
			}
		}

		return null;
	}

	public ExecutionVertex getVertexByEnvironment(Environment environment) {

		if (environment == null) {
			return null;
		}

		final ExecutionGraphIterator it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			if (vertex.getEnvironment() == environment) {
				return vertex;
			}
		}

		return null;
	}

	/**
	 * Checks if the current execution stage has been successfully completed, i.e.
	 * all vertices in this stage have successfully finished their execution.
	 * 
	 * @return <code>true</code> if stage is completed, <code>false</code> otherwise
	 */
	private boolean isCurrentStageCompleted() {

		if (this.indexToCurrentExecutionStage >= this.stages.size()) {
			return true;
		}

		final ExecutionGraphIterator it = new ExecutionGraphIterator(this, this.indexToCurrentExecutionStage, true,
			true);
		while (it.hasNext()) {
			final ExecutionVertex vertex = it.next();
			if (vertex.getExecutionState() != ExecutionState.FINISHED) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks if the execution of execution graph is finished.
	 * 
	 * @return <code>true</code> if the execution of the graph is finished, <code>false</code> otherwise
	 */
	public boolean isExecutionFinished() {

		return (getJobStatus() == JobStatus.FINISHED);
	}

	public void prepareChannelsForExecution(ExecutionVertex executionVertex) throws ChannelSetupException {

		// Prepare channels
		for (int k = 0; k < executionVertex.getEnvironment().getNumberOfOutputGates(); k++) {
			final OutputGate<? extends Record> outputGate = executionVertex.getEnvironment().getOutputGate(k);
			for (int l = 0; l < outputGate.getNumberOfOutputChannels(); l++) {
				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(l);
				final AbstractInputChannel<? extends Record> inputChannel = this.inputChannelMap.get(outputChannel
					.getConnectedChannelID());
				if (inputChannel == null) {
					throw new ChannelSetupException("Cannot find input channel to output channel "
						+ outputChannel.getID());
				}

				final ExecutionVertex targetVertex = this.channelToVertexMap.get(inputChannel.getID());
				final AllocatedResource targetResources = targetVertex.getAllocatedResource();
				if (targetResources == null) {
					throw new ChannelSetupException("Cannot find allocated resources for target vertex "
						+ targetVertex.getID() + " in instance map");
				}

				if (targetResources.getInstance() instanceof DummyInstance) {
					throw new ChannelSetupException("Allocated instance for " + targetVertex.getID()
						+ " is a dummy vertex!");
				}
			}
		}
	}

	/**
	 * Returns the job ID of the job configuration this execution graph was originally constructed from.
	 * 
	 * @return the job ID of the job configuration this execution graph was originally constructed from
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	public void removeUnnecessaryNetworkChannels(int stageNumber) {

		if (stageNumber >= this.stages.size()) {
			throw new IllegalArgumentException("removeUnnecessaryNetworkChannels called on an illegal stage ("
				+ stageNumber + ")");
		}

		final ExecutionStage executionStage = this.stages.get(stageNumber);

		for (int i = 0; i < executionStage.getNumberOfStageMembers(); i++) {
			final ExecutionGroupVertex groupVertex = executionStage.getStageMember(i);

			for (int j = 0; j < groupVertex.getCurrentNumberOfGroupMembers(); j++) {
				final ExecutionVertex sourceVertex = groupVertex.getGroupMember(j);

				for (int k = 0; k < sourceVertex.getEnvironment().getNumberOfOutputGates(); k++) {
					final OutputGate<? extends Record> outputGate = sourceVertex.getEnvironment().getOutputGate(k);

					for (int l = 0; l < outputGate.getNumberOfOutputChannels(); l++) {
						final AbstractOutputChannel<? extends Record> oldOutputChannel = outputGate.getOutputChannel(l);

						// Skip if not a network channel
						if (!(oldOutputChannel instanceof NetworkOutputChannel<?>)) {
							continue;
						}

						// Get matching input channel
						final ExecutionVertex targetVertex = this.channelToVertexMap.get(oldOutputChannel
							.getConnectedChannelID());
						if (targetVertex == null) {
							throw new RuntimeException("Cannot find target vertex: Inconsistency...");
						}

						// Run on the same instance?
						if (!targetVertex.getAllocatedResource().getInstance().equals(
							sourceVertex.getAllocatedResource().getInstance())) {
							continue;
						}

						final AbstractInputChannel<? extends Record> oldInputChannel = getInputChannelByID(oldOutputChannel
							.getConnectedChannelID());
						final InputGate<? extends Record> inputGate = oldInputChannel.getInputGate();

						// Replace channels
						final AbstractOutputChannel<? extends Record> newOutputChannel = outputGate.replaceChannel(
							oldOutputChannel.getID(), ChannelType.INMEMORY);
						final AbstractInputChannel<? extends Record> newInputChannel = inputGate.replaceChannel(
							oldInputChannel.getID(), ChannelType.INMEMORY);

						// The new channels reuse the IDs of the old channels, so only the channel maps must be updated
						this.outputChannelMap.put(newOutputChannel.getID(), newOutputChannel);
						this.inputChannelMap.put(newInputChannel.getID(), newInputChannel);

					}
				}
			}
		}
	}

	/**
	 * Returns the index of the current execution stage.
	 * 
	 * @return the index of the current execution stage
	 */
	public int getIndexOfCurrentExecutionStage() {
		return this.indexToCurrentExecutionStage;
	}

	public ExecutionStage getCurrentExecutionStage() {

		if (this.indexToCurrentExecutionStage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(this.indexToCurrentExecutionStage);
	}

	/**
	 * Returns the types and numbers of instances which are required for the
	 * current execution stage to complete.
	 * 
	 * @return a map containing the types and respective numbers of instances required
	 *         for the current execution stage to complete.
	 */
	public Map<InstanceType, Integer> getInstanceTypesRequiredForCurrentStage() {

		final Map<InstanceType, Integer> instanceTypeMap = new HashMap<InstanceType, Integer>();

		if (this.indexToCurrentExecutionStage >= this.stages.size()) {
			return instanceTypeMap;
		}

		final ExecutionStage nextStage = this.stages.get(this.indexToCurrentExecutionStage);
		if (nextStage == null) {
			LOG.warn("Stage " + this.indexToCurrentExecutionStage + " is not a valid execution stage");
		}

		final Set<AbstractInstance> collectedInstances = new HashSet<AbstractInstance>();

		for (int i = 0; i < nextStage.getNumberOfStageMembers(); i++) {
			final ExecutionGroupVertex groupVertex = nextStage.getStageMember(i);

			for (int j = 0; j < groupVertex.getCurrentNumberOfGroupMembers(); j++) {
				// Get the instance type from the execution vertex if it
				final ExecutionVertex vertex = groupVertex.getGroupMember(j);
				if (vertex.getExecutionState() == ExecutionState.SCHEDULED) {
					final AbstractInstance instance = vertex.getAllocatedResource().getInstance();

					if (collectedInstances.contains(instance)) {
						continue;
					} else {
						collectedInstances.add(instance);
					}

					if (instance instanceof DummyInstance) {
						Integer num = instanceTypeMap.get(instance.getType());
						num = (num == null) ? new Integer(1) : new Integer(num.intValue() + 1);
						instanceTypeMap.put(instance.getType(), num);
					} else {
						LOG.debug("Execution Vertex " + vertex.getName() + " (" + vertex.getID()
							+ ") is already assigned to non-dummy instance, skipping...");
					}
				}
			}
		}

		return instanceTypeMap;
	}

	public void repairStages() {

		final Map<ExecutionGroupVertex, Integer> stageNumbers = new HashMap<ExecutionGroupVertex, Integer>();
		ExecutionGroupVertexIterator it = new ExecutionGroupVertexIterator(this, true, -1);

		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			int precedingNumber = 0;
			if (stageNumbers.containsKey(groupVertex)) {
				precedingNumber = stageNumbers.get(groupVertex).intValue();
			} else {
				stageNumbers.put(groupVertex, new Integer(precedingNumber));
			}

			for (int i = 0; i < groupVertex.getNumberOfForwardLinks(); i++) {

				final ExecutionGroupEdge edge = groupVertex.getForwardEdge(i);
				if (!stageNumbers.containsKey(edge.getTargetVertex())) {
					// Target vertex has not yet been discovered
					if (edge.getChannelType() != ChannelType.FILE) {
						// Same stage as preceding vertex
						stageNumbers.put(edge.getTargetVertex(), new Integer(precedingNumber));
					} else {
						// File channel, increase stage of target vertex by one
						stageNumbers.put(edge.getTargetVertex(), new Integer(precedingNumber + 1));
					}
				} else {
					final int stageNumber = stageNumbers.get(edge.getTargetVertex()).intValue();
					if (edge.getChannelType() != ChannelType.FILE) {
						if (stageNumber != precedingNumber) {
							stageNumbers.put(edge.getTargetVertex(), (int) Math.max(precedingNumber, stageNumber));
						}
					} else {
						// File channel, increase stage of target vertex by one
						if (stageNumber != (precedingNumber + 1)) {
							stageNumbers.put(edge.getTargetVertex(), (int) Math.max(precedingNumber + 1, stageNumber));
						}
					}
				}
			}
		}

		// Traverse the graph backwards (starting from the output vertices) to make sure vertices are allocated in a
		// stage as high as possible
		it = new ExecutionGroupVertexIterator(this, false, -1);

		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			final int succeedingNumber = stageNumbers.get(groupVertex);

			for (int i = 0; i < groupVertex.getNumberOfBackwardLinks(); i++) {

				final ExecutionGroupEdge edge = groupVertex.getBackwardEdge(i);
				final int stageNumber = stageNumbers.get(edge.getSourceVertex());
				if (edge.getChannelType() == ChannelType.FILE) {
					if (stageNumber < (succeedingNumber - 1)) {
						stageNumbers.put(edge.getSourceVertex(), new Integer(succeedingNumber - 1));
					}
				} else {
					if (stageNumber != succeedingNumber) {
						LOG.error(edge.getSourceVertex() + " and " + edge.getTargetVertex()
							+ " are assigned to different stages although not connected by a file channel");
					}
				}
			}
		}

		// Finally, assign the new stage numbers
		this.stages.clear();
		final Iterator<ExecutionGroupVertex> it2 = stageNumbers.keySet().iterator();
		while (it2.hasNext()) {

			final ExecutionGroupVertex groupVertex = it2.next();
			final int stageNumber = stageNumbers.get(groupVertex).intValue();
			// Prevent out of bounds exceptions
			while (this.stages.size() <= stageNumber) {
				this.stages.add(null);
			}
			ExecutionStage executionStage = this.stages.get(stageNumber);
			// If the stage not yet exists,
			if (executionStage == null) {
				executionStage = new ExecutionStage(stageNumber);
				this.stages.set(stageNumber, executionStage);
			}

			executionStage.addStageMember(groupVertex);
			groupVertex.setExecutionStage(executionStage);
		}
	}

	public void repairInstanceAssignment() {

		Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);
		while (it.hasNext()) {

			final ExecutionVertex sourceVertex = it.next();

			for (int i = 0; i < sourceVertex.getEnvironment().getNumberOfOutputGates(); i++) {

				final OutputGate<? extends Record> outputGate = sourceVertex.getEnvironment().getOutputGate(i);
				for (int j = 0; j < outputGate.getNumberOfOutputChannels(); j++) {
					final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
					final ChannelType channelType = outputChannel.getType();
					if (channelType == ChannelType.FILE || channelType == ChannelType.INMEMORY) {

						final ExecutionVertex targetVertex = getVertexByChannelID(outputChannel.getConnectedChannelID());
						targetVertex.setAllocatedResource(sourceVertex.getAllocatedResource());
					}
				}
			}
		}

		it = new ExecutionGraphIterator(this, false);
		while (it.hasNext()) {

			final ExecutionVertex targetVertex = it.next();

			for (int i = 0; i < targetVertex.getEnvironment().getNumberOfInputGates(); i++) {

				final InputGate<? extends Record> inputGate = targetVertex.getEnvironment().getInputGate(i);
				for (int j = 0; j < inputGate.getNumberOfInputChannels(); j++) {
					final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(j);
					final ChannelType channelType = inputChannel.getType();
					if (channelType == ChannelType.FILE || channelType == ChannelType.INMEMORY) {

						final ExecutionVertex sourceVertex = getVertexByChannelID(inputChannel.getConnectedChannelID());
						sourceVertex.setAllocatedResource(targetVertex.getAllocatedResource());
					}
				}
			}
		}
	}

	public ChannelType getChannelType(ExecutionVertex sourceVertex, ExecutionVertex targetVertex) {

		final ExecutionGroupVertex sourceGroupVertex = sourceVertex.getGroupVertex();
		final ExecutionGroupVertex targetGroupVertex = targetVertex.getGroupVertex();

		final List<ExecutionGroupEdge> edges = sourceGroupVertex.getForwardEdges(targetGroupVertex);
		if (edges.size() == 0) {
			return null;
		}

		// On a task level, the two vertices are connected
		final ExecutionGroupEdge edge = edges.get(0);

		// Now lets see if these two concrete subtasks are connected
		final OutputGate<? extends Record> outputGate = sourceVertex.getEnvironment().getOutputGate(
			edge.getIndexOfOutputGate());
		for (int i = 0; i < outputGate.getNumberOfOutputChannels(); i++) {

			final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(i);
			final ChannelID inputChannelID = outputChannel.getConnectedChannelID();
			if (targetVertex == this.channelToVertexMap.get(inputChannelID)) {
				return edge.getChannelType();
			}
		}

		return null;
	}

	/**
	 * Returns the job configuration that was originally attached to the job graph.
	 * 
	 * @return the job configuration that was originally attached to the job graph
	 */
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	/**
	 * Checks whether the job represented by the execution graph has the status <code>CREATED</code>.
	 * 
	 * @return <code>true</code> if the job has the status <code>CREATED</code>, <code>false</code> otherwise
	 */
	private boolean jobHasCreatedStatus() {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			if (it.next().getExecutionState() != ExecutionState.CREATED) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks whether the job represented by the execution graph has the status <code>FINISHED</code>.
	 * 
	 * @return <code>true</code> if the job has the status <code>CREATED</code>, <code>false</code> otherwise
	 */
	private boolean jobHasFinishedStatus() {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			if (it.next().getExecutionState() != ExecutionState.FINISHED) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks whether the job represented by the execution graph has the status <code>FAILED</code>.
	 * 
	 * @return <code>true</code> if the job has the status <code>FAILED</code>, <code>false</code> otherwise
	 */
	private boolean jobHasFailedStatus() {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			final ExecutionVertex ev = it.next();

			if (ev.getExecutionState() == ExecutionState.FAILED && !ev.hasRetriesLeft()) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Checks whether the job represented by the execution graph has the status <code>SCHEDULED</code>.
	 * 
	 * @return <code>true</code> if the job has the status <code>SCHEDULED</code>, <code>false</code> otherwise
	 */
	private boolean jobHasScheduledStatus() {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			final ExecutionState s = it.next().getExecutionState();
			if (s != ExecutionState.CREATED && s != ExecutionState.SCHEDULED && s != ExecutionState.ASSIGNING
				&& s != ExecutionState.ASSIGNED && s != ExecutionState.READY) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks whether the job represented by the execution graph has the status <code>CANCELED</code>.
	 * 
	 * @return <code>true</code> if the job has the status <code>CANCELED</code>, <code>false</code> otherwise
	 */
	private boolean jobHasCanceledStatus() {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			if (it.next().getExecutionState() != ExecutionState.CANCELLED) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks and updates the current execution status of the
	 * job which is represented by this execution graph.
	 */
	public synchronized void checkAndUpdateJobStatus() {

		if (jobHasCreatedStatus()) {
			this.jobStatus = JobStatus.CREATED;
			return;
		}

		if (jobHasScheduledStatus()) {
			this.jobStatus = JobStatus.SCHEDULED;
			return;
		}

		if (jobHasCanceledStatus()) {
			this.jobStatus = JobStatus.CANCELLED;
			return;
		}

		if (jobHasFinishedStatus()) {
			this.jobStatus = JobStatus.FINISHED;
			return;
		}

		if (jobHasFailedStatus()) {
			this.jobStatus = JobStatus.FAILED;
			return;
		}

		this.jobStatus = JobStatus.RUNNING;

	}

	/**
	 * Returns the current status of the job
	 * represented by this execution graph.
	 * 
	 * @return the current status of the job
	 */
	public synchronized JobStatus getJobStatus() {
		return this.jobStatus;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void executionStateChanged(Environment ee, ExecutionState newExecutionState,
			String optionalMessage) {

		final JobStatus oldStatus = this.jobStatus;
		checkAndUpdateJobStatus();

		if (newExecutionState == ExecutionState.FINISHED) {
			// It is worth checking if the current stage has complete
			if (this.isCurrentStageCompleted()) {
				// Increase current execution stage
				++this.indexToCurrentExecutionStage;
			}
		}

		if (this.jobStatus != oldStatus) {

			final Iterator<JobStatusListener> it = this.jobStatusListeners.iterator();
			while (it.hasNext()) {
				it.next().jobStatusHasChanged(this.jobID, this.jobStatus, optionalMessage);
			}
		}
	}

	/**
	 * Registers a new {@link JobStatusListener} object with this execution graph.
	 * After being registered the object will receive notifications about changes
	 * of the job status. It is not possible to register the same listener object
	 * twice.
	 * 
	 * @param jobStatusListener
	 *        the listener object to register
	 */
	public synchronized void registerJobStatusListener(JobStatusListener jobStatusListener) {

		if (jobStatusListener == null) {
			return;
		}

		if (!this.jobStatusListeners.contains(jobStatusListener)) {
			this.jobStatusListeners.add(jobStatusListener);
		}
	}

	/**
	 * Unregisters the given {@link JobStatusListener} object. After having called this
	 * method, the object will no longer receive notifications about changes of the job
	 * status.
	 * 
	 * @param jobStatusListener
	 *        the listener object to unregister
	 */
	public synchronized void unregisterJobStatusListener(JobStatusListener jobStatusListener) {

		if (jobStatusListener == null) {
			return;
		}

		this.jobStatusListeners.remove(jobStatusListener);
	}

	/**
	 * Returns the name of the original job graph.
	 * 
	 * @return the name of the original job graph, possibly <code>null</code>
	 */
	public String getJobName() {
		return this.jobName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(Environment ee, Thread userThread) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(Environment ee, Thread userThread) {
		// Nothing to do here
	}
}
