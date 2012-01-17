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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobEdge;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * In Nephele an execution graph is the main data structure for scheduling, executing and
 * observing a job. An execution graph is created from an job graph. In contrast to a job graph
 * it can contain communication edges of specific types, sub groups of vertices and information on
 * when and where (on which instance) to run particular tasks.
 * <p>
 * This class is thread-safe.
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
	private final ConcurrentMap<ChannelID, ExecutionVertex> channelToVertexMap = new ConcurrentHashMap<ChannelID, ExecutionVertex>();

	/**
	 * Mapping of channel IDs to input channels.
	 */
	private final ConcurrentMap<ChannelID, AbstractInputChannel<? extends Record>> inputChannelMap = new ConcurrentHashMap<ChannelID, AbstractInputChannel<? extends Record>>();

	/**
	 * Mapping of channel IDs to output channels.
	 */
	private final ConcurrentMap<ChannelID, AbstractOutputChannel<? extends Record>> outputChannelMap = new ConcurrentHashMap<ChannelID, AbstractOutputChannel<? extends Record>>();

	/**
	 * List of stages in the graph.
	 */
	private final CopyOnWriteArrayList<ExecutionStage> stages = new CopyOnWriteArrayList<ExecutionStage>();

	/**
	 * Index to the current execution stage.
	 */
	private volatile int indexToCurrentExecutionStage = 0;

	/**
	 * The job configuration that was originally attached to the JobGraph.
	 */
	private final Configuration jobConfiguration;

	/**
	 * The current status of the job which is represented by this execution graph.
	 */
	private final AtomicReference<InternalJobStatus> jobStatus = new AtomicReference<InternalJobStatus>(
		InternalJobStatus.CREATED);

	/**
	 * The error description of the first task which causes this job to fail.
	 */
	private volatile String errorDescription = null;

	/**
	 * List of listeners which are notified in case the status of this job has changed.
	 */
	private final CopyOnWriteArrayList<JobStatusListener> jobStatusListeners = new CopyOnWriteArrayList<JobStatusListener>();

	/**
	 * List of listeners which are notified in case the execution stage of a job has changed.
	 */
	private final CopyOnWriteArrayList<ExecutionStageListener> executionStageListeners = new CopyOnWriteArrayList<ExecutionStageListener>();

	private final CopyOnWriteArrayList<ExecutionVertex> recovering = new CopyOnWriteArrayList<ExecutionVertex>();

	/**
	 * Private constructor used for duplicating execution vertices.
	 * 
	 * @param jobID
	 *        the ID of the duplicated execution graph
	 * @param jobName
	 *        the name of the original job graph
	 * @param jobConfiguration
	 *        the configuration originally attached to the job graph
	 */
	private ExecutionGraph(final JobID jobID, final String jobName, final Configuration jobConfiguration) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		this.jobID = jobID;
		this.jobName = jobName;
		this.jobConfiguration = jobConfiguration;
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
	public ExecutionGraph(final JobGraph job, final InstanceManager instanceManager) throws GraphConversionException {
		this(job.getJobID(), job.getName(), job.getJobConfiguration());

		// Start constructing the new execution graph from given job graph
		try {
			constructExecutionGraph(job, instanceManager);
		} catch (Exception e) {
			throw new GraphConversionException(StringUtils.stringifyException(e));
		}
	}

	/**
	 * Applies the user defined settings to the execution graph.
	 * 
	 * @param temporaryGroupVertexMap
	 *        mapping between job vertices and the corresponding group vertices.
	 * @throws GraphConversionException
	 *         thrown if an error occurs while applying the user settings.
	 */
	private void applyUserDefinedSettings(final HashMap<AbstractJobVertex, ExecutionGroupVertex> temporaryGroupVertexMap)
			throws GraphConversionException {

		// The check for cycles in the dependency chain for instance sharing is already checked in
		// <code>submitJob</code> method of the job manager

		// If there is no cycle, apply the settings to the corresponding group vertices
		final Iterator<Map.Entry<AbstractJobVertex, ExecutionGroupVertex>> it = temporaryGroupVertexMap.entrySet()
			.iterator();
		while (it.hasNext()) {

			final Map.Entry<AbstractJobVertex, ExecutionGroupVertex> entry = it.next();
			final AbstractJobVertex jobVertex = entry.getKey();
			if (jobVertex.getVertexToShareInstancesWith() != null) {

				final AbstractJobVertex vertexToShareInstancesWith = jobVertex.getVertexToShareInstancesWith();
				final ExecutionGroupVertex groupVertex = entry.getValue();
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
				groupVertex.repairSubtasksPerInstance();
			}
		}

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

		// Repair the instance assignment after having changed the channel types
		repairInstanceAssignment();

		// Repair the instance sharing among different group vertices
		repairInstanceSharing();

		// Finally, repair the stages
		repairStages();
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
	private void constructExecutionGraph(final JobGraph jobGraph, final InstanceManager instanceManager)
			throws GraphConversionException {

		// Clean up temporary data structures
		final HashMap<AbstractJobVertex, ExecutionVertex> temporaryVertexMap = new HashMap<AbstractJobVertex, ExecutionVertex>();
		final HashMap<AbstractJobVertex, ExecutionGroupVertex> temporaryGroupVertexMap = new HashMap<AbstractJobVertex, ExecutionGroupVertex>();

		// Initially, create only one execution stage that contains all group vertices
		final ExecutionStage initialExecutionStage = new ExecutionStage(this, 0);
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

		// Finally, construct the execution pipelines
		reconstructExecutionPipelines();
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
	private void createInitialChannels(final AbstractJobVertex jobVertex,
			final HashMap<AbstractJobVertex, ExecutionVertex> vertexMap) throws GraphConversionException {

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
	void unwire(final ExecutionGroupVertex source, final int indexOfOutputGate, final ExecutionGroupVertex target,
			final int indexOfInputGate) throws GraphConversionException {

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

	void wire(final ExecutionGroupVertex source, final int indexOfOutputGate, final ExecutionGroupVertex target,
			final int indexOfInputGate, final ChannelType channelType, final CompressionLevel compressionLevel)
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

				// Update channel type of input gate
				inputGate.setChannelType(channelType);
			}

			// Update channel type of output gate
			outputGate.setChannelType(channelType);
		}

	}

	private void createChannel(final ExecutionVertex source, final OutputGate<? extends Record> outputGate,
			final ExecutionVertex target, final InputGate<? extends Record> inputGate, final ChannelType channelType,
			final CompressionLevel compressionLevel)
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
	private ExecutionVertex createVertex(final AbstractJobVertex jobVertex, final InstanceManager instanceManager,
			final ExecutionStage initialExecutionStage) throws GraphConversionException {

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

		ExecutionVertex ev = null;
		try {
			ev = new ExecutionVertex(jobVertex.getJobGraph().getJobID(), invokableClass, this,
				groupVertex);
		} catch (Throwable t) {
			throw new GraphConversionException(StringUtils.stringifyException(t));
		}

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
		ev.setAllocatedResource(new AllocatedResource(DummyInstance.createDummyInstance(instanceType), instanceType,
			null));

		// Register input and output vertices separately
		if (jobVertex instanceof AbstractJobInputVertex) {

			final InputSplit[] inputSplits;

			// let the task code compute the input splits
			if (ev.getEnvironment().getInvokable() instanceof AbstractInputTask) {
				try {
					inputSplits = ((AbstractInputTask<?>) ev.getEnvironment().getInvokable()).
							computeInputSplits(jobVertex.getNumberOfSubtasks());
				} catch (Exception e) {
					throw new GraphConversionException("Cannot compute input splits for " + groupVertex.getName()
						+ ": "
							+ StringUtils.stringifyException(e));
				}
			} else {
				throw new GraphConversionException(
					"BUG: JobInputVertex contained a task class which was not an input task.");
			}

			if (inputSplits == null) {
				LOG.info("Job input vertex " + jobVertex.getName() + " generated 0 input splits");
			} else {
				LOG.info("Job input vertex " + jobVertex.getName() + " generated " + inputSplits.length
					+ " input splits");
			}

			// assign input splits
			groupVertex.setInputSplits(inputSplits);
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
	public int getNumberOfOutputVertices(final int stage) {

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
	public ExecutionVertex getInputVertex(final int index) {

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
	public ExecutionVertex getOutputVertex(final int index) {

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
	public ExecutionVertex getInputVertex(final int stage, final int index) {

		try {
			final ExecutionStage s = this.stages.get(stage);
			if (s == null) {
				return null;
			}

			return s.getInputExecutionVertex(index);

		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
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
	public ExecutionVertex getOutputVertex(final int stage, final int index) {

		try {
			final ExecutionStage s = this.stages.get(stage);
			if (s == null) {
				return null;
			}

			return s.getOutputExecutionVertex(index);

		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
	}

	/**
	 * Returns the execution stage with number <code>num</code>.
	 * 
	 * @param num
	 *        the number of the execution stage to be returned
	 * @return the execution stage with number <code>num</code> or <code>null</code> if no such execution stage exists
	 */
	public ExecutionStage getStage(final int num) {

		try {
			return this.stages.get(num);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
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
	public ExecutionVertex getVertexByChannelID(final ChannelID id) {

		return this.channelToVertexMap.get(id);
	}

	/**
	 * Finds an input channel by its ID and returns it.
	 * 
	 * @param id
	 *        the channel ID to identify the input channel
	 * @return the input channel whose ID matches <code>id</code> or <code>null</code> if no such channel is known
	 */
	public AbstractInputChannel<? extends Record> getInputChannelByID(final ChannelID id) {

		return this.inputChannelMap.get(id);
	}

	/**
	 * Finds an output channel by its ID and returns it.
	 * 
	 * @param id
	 *        the channel ID to identify the output channel
	 * @return the output channel whose ID matches <code>id</code> or <code>null</code> if no such channel is known
	 */
	public AbstractOutputChannel<? extends Record> getOutputChannelByID(final ChannelID id) {

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
	public List<ExecutionVertex> getVerticesAssignedToResource(final AllocatedResource allocatedResource) {

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

	public ExecutionVertex getVertexByID(final ExecutionVertexID id) {

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

		return (getJobStatus() == InternalJobStatus.FINISHED);
	}

	/**
	 * Returns the job ID of the job configuration this execution graph was originally constructed from.
	 * 
	 * @return the job ID of the job configuration this execution graph was originally constructed from
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the index of the current execution stage.
	 * 
	 * @return the index of the current execution stage
	 */
	public int getIndexOfCurrentExecutionStage() {
		return this.indexToCurrentExecutionStage;
	}

	/**
	 * Returns the stage which is currently executed.
	 * 
	 * @return the currently executed stage or <code>null</code> if the job execution is already completed
	 */
	public ExecutionStage getCurrentExecutionStage() {

		try {
			return this.stages.get(this.indexToCurrentExecutionStage);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
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
				stageNumbers.put(groupVertex, Integer.valueOf(precedingNumber));
			}

			for (int i = 0; i < groupVertex.getNumberOfForwardLinks(); i++) {

				final ExecutionGroupEdge edge = groupVertex.getForwardEdge(i);
				if (!stageNumbers.containsKey(edge.getTargetVertex())) {
					// Target vertex has not yet been discovered
					if (edge.getChannelType() != ChannelType.FILE) {
						// Same stage as preceding vertex
						stageNumbers.put(edge.getTargetVertex(), Integer.valueOf(precedingNumber));
					} else {
						// File channel, increase stage of target vertex by one
						stageNumbers.put(edge.getTargetVertex(), Integer.valueOf(precedingNumber + 1));
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
						stageNumbers.put(edge.getSourceVertex(), Integer.valueOf(succeedingNumber - 1));
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
		final Iterator<Map.Entry<ExecutionGroupVertex, Integer>> it2 = stageNumbers.entrySet().iterator();
		while (it2.hasNext()) {

			final Map.Entry<ExecutionGroupVertex, Integer> entry = it2.next();
			final ExecutionGroupVertex groupVertex = entry.getKey();
			final int stageNumber = entry.getValue().intValue();
			// Prevent out of bounds exceptions
			while (this.stages.size() <= stageNumber) {
				this.stages.add(null);
			}
			ExecutionStage executionStage = this.stages.get(stageNumber);
			// If the stage not yet exists,
			if (executionStage == null) {
				executionStage = new ExecutionStage(this, stageNumber);
				this.stages.set(stageNumber, executionStage);
			}

			executionStage.addStageMember(groupVertex);
			groupVertex.setExecutionStage(executionStage);
		}
	}

	public void repairInstanceSharing() {

		final Set<AllocatedResource> availableResources = new LinkedHashSet<AllocatedResource>();

		final Iterator<ExecutionGroupVertex> it = new ExecutionGroupVertexIterator(this, true, -1);
		while (it.hasNext()) {
			final ExecutionGroupVertex groupVertex = it.next();
			if (groupVertex.getVertexToShareInstancesWith() == null) {
				availableResources.clear();
				groupVertex.repairInstanceSharing(availableResources);
			}
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

	public ChannelType getChannelType(final ExecutionVertex sourceVertex, final ExecutionVertex targetVertex) {

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
	 * Checks whether the job represented by the execution graph has the status <code>SCHEDULED</code>.
	 * 
	 * @return <code>true</code> if the job has the status <code>SCHEDULED</code>, <code>false</code> otherwise
	 */
	private boolean jobHasScheduledStatus() {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			final ExecutionState s = it.next().getExecutionState();
			if (s != ExecutionState.CREATED && s != ExecutionState.SCHEDULED && s != ExecutionState.READY) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks whether the job represented by the execution graph has the status <code>CANCELED</code> or
	 * <code>FAILED</code>.
	 * 
	 * @return <code>true</code> if the job has the status <code>CANCELED</code> or <code>FAILED</code>,
	 *         <code>false</code> otherwise
	 */
	private boolean jobHasFailedOrCanceledStatus() {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);

		while (it.hasNext()) {

			final ExecutionState state = it.next().getExecutionState();

			if (state != ExecutionState.CANCELED && state != ExecutionState.FAILED && state != ExecutionState.FINISHED) {
				return false;
			}
		}

		return true;
	}

	// TODO: Make this static
	private InternalJobStatus determineNewJobStatus(final ExecutionGraph eg,
			final ExecutionState latestStateChange) {

		final InternalJobStatus currentJobStatus = eg.getJobStatus();

		switch (currentJobStatus) {
		case CREATED:
			if (eg.jobHasScheduledStatus()) {
				return InternalJobStatus.SCHEDULED;
			} else if (latestStateChange == ExecutionState.CANCELED) {
				if (eg.jobHasFailedOrCanceledStatus()) {
					return InternalJobStatus.CANCELED;
				}
			}
			break;
		case SCHEDULED:
			if (latestStateChange == ExecutionState.RUNNING) {
				return InternalJobStatus.RUNNING;
			} else if (latestStateChange == ExecutionState.CANCELED) {
				if (eg.jobHasFailedOrCanceledStatus()) {
					return InternalJobStatus.CANCELED;
				}
			}
			break;
		case RUNNING:
			if (latestStateChange == ExecutionState.CANCELING || latestStateChange == ExecutionState.CANCELED) {
				return InternalJobStatus.CANCELING;
			}
			if (latestStateChange == ExecutionState.FAILED) {

				final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
				while (it.hasNext()) {

					final ExecutionVertex vertex = it.next();
					if (vertex.getExecutionState() == ExecutionState.FAILED) {
						if (!vertex.hasRetriesLeft()) {
							System.out.println(" Vertex failed finally");
							return InternalJobStatus.FAILING;
						} else {
							return InternalJobStatus.RECOVERING;
						}
					}
				}
			}
			if (latestStateChange == ExecutionState.RECOVERING) {
				return InternalJobStatus.RECOVERING;
			}
			if (eg.jobHasFinishedStatus()) {
				return InternalJobStatus.FINISHED;
			}
			break;
		case RECOVERING:
			if (latestStateChange == ExecutionState.RERUNNING) {
				if (this.recovering.isEmpty()) {
					return InternalJobStatus.RUNNING;
				}
			}
			if (latestStateChange == ExecutionState.FAILED) {
				LOG.info("Another Failed Vertex while recovering");
			}
			break;
		case FAILING:
			if (eg.jobHasFailedOrCanceledStatus()) {
				return InternalJobStatus.FAILED;
			}
			break;
		case FAILED:
			LOG.error("Received update of execute state in job status FAILED");
			break;
		case CANCELING:
			if (eg.jobHasFailedOrCanceledStatus()) {
				return InternalJobStatus.CANCELED;
			}
			break;
		case CANCELED:
			LOG.error("Received update of execute state in job status CANCELED");
			break;
		case FINISHED:
			LOG.error("Received update of execute state in job status FINISHED");
			break;
		}

		return currentJobStatus;
	}

	/**
	 * Returns the current status of the job
	 * represented by this execution graph.
	 * 
	 * @return the current status of the job
	 */
	public InternalJobStatus getJobStatus() {

		return this.jobStatus.get();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
			final ExecutionState newExecutionState, String optionalMessage) {

		if (newExecutionState == ExecutionState.RERUNNING) {
			this.recovering.remove(getVertexByID(vertexID));
		}

		final InternalJobStatus newJobStatus = determineNewJobStatus(this, newExecutionState);

		if (newExecutionState == ExecutionState.FINISHED) {
			// It is worth checking if the current stage has complete
			if (this.isCurrentStageCompleted()) {
				// Increase current execution stage
				++this.indexToCurrentExecutionStage;

				if (this.indexToCurrentExecutionStage < this.stages.size()) {
					final Iterator<ExecutionStageListener> it = this.executionStageListeners.iterator();
					final ExecutionStage nextExecutionStage = getCurrentExecutionStage();
					while (it.hasNext()) {
						it.next().nextExecutionStageEntered(jobID, nextExecutionStage);
					}
				}
			}
		}
		if (newExecutionState == ExecutionState.FAILED && newJobStatus == InternalJobStatus.RECOVERING) {
			LOG.info("RECOVERING");
			// FIXME (marrus) see if we even need that
			if (!this.recovering.contains(vertexID)) {
				this.recovering.add(this.getVertexByID(vertexID));
			}
		}

		updateJobStatus(newJobStatus, optionalMessage);
	}

	/**
	 * Updates the job status to given status and triggers the execution of the {@link JobStatusListener} objects.
	 * 
	 * @param newJobStatus
	 *        the new job status
	 * @param optionalMessage
	 *        an optional message providing details on the reasons for the state change
	 */
	public void updateJobStatus(final InternalJobStatus newJobStatus, String optionalMessage) {

		// Check if the new job status equals the old one
		if (this.jobStatus.getAndSet(newJobStatus) == newJobStatus) {
			return;
		}

		// The task caused the entire job to fail, save the error description
		if (newJobStatus == InternalJobStatus.FAILING) {
			this.errorDescription = optionalMessage;
		}

		// If this is the final failure state change, reuse the saved error description
		if (newJobStatus == InternalJobStatus.FAILED) {
			optionalMessage = this.errorDescription;
		}

		final Iterator<JobStatusListener> it = this.jobStatusListeners.iterator();
		while (it.hasNext()) {
			it.next().jobStatusHasChanged(this, newJobStatus, optionalMessage);
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
	public void registerJobStatusListener(final JobStatusListener jobStatusListener) {

		if (jobStatusListener == null) {
			throw new IllegalArgumentException("Argument jobStatusListener must not be null");
		}

		this.jobStatusListeners.addIfAbsent(jobStatusListener);
	}

	/**
	 * Unregisters the given {@link JobStatusListener} object. After having called this
	 * method, the object will no longer receive notifications about changes of the job
	 * status.
	 * 
	 * @param jobStatusListener
	 *        the listener object to unregister
	 */
	public void unregisterJobStatusListener(final JobStatusListener jobStatusListener) {

		if (jobStatusListener == null) {
			throw new IllegalArgumentException("Argument jobStatusListener must not be null");
		}

		this.jobStatusListeners.remove(jobStatusListener);
	}

	/**
	 * Registers a new {@link ExecutionStageListener} object with this execution graph. After being registered the
	 * object will receive a notification whenever the job has entered its next execution stage. Note that a
	 * notification is not sent when the job has entered its initial execution stage.
	 * 
	 * @param executionStageListener
	 *        the listener object to register
	 */
	public void registerExecutionStageListener(final ExecutionStageListener executionStageListener) {

		if (executionStageListener == null) {
			throw new IllegalArgumentException("Argument executionStageListener must not be null");
		}

		this.executionStageListeners.addIfAbsent(executionStageListener);
	}

	/**
	 * Unregisters the given {@link ExecutionStageListener} object. After having called this method, the object will no
	 * longer receiver notifications about the execution stage progress.
	 * 
	 * @param executionStageListener
	 *        the listener object to unregister
	 */
	public void unregisterExecutionStageListener(final ExecutionStageListener executionStageListener) {

		if (executionStageListener == null) {
			throw new IllegalArgumentException("Argument executionStageListener must not be null");
		}

		this.executionStageListeners.remove(executionStageListener);
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
	 * Returns a list of vertices which are contained in this execution graph and have a finished checkpoint.
	 * 
	 * @return list of vertices which are contained in this execution graph and have a finished checkpoint
	 */
	public List<ExecutionVertex> getVerticesWithCheckpoints() {

		final List<ExecutionVertex> list = new ArrayList<ExecutionVertex>();
		final Iterator<ExecutionGroupVertex> it = new ExecutionGroupVertexIterator(this, true, -1);

		// In the current implementation we just look for vertices which have outgoing file channels
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			for (int i = 0; i < groupVertex.getNumberOfForwardLinks(); i++) {

				if (groupVertex.getForwardEdge(i).getChannelType() == ChannelType.FILE) {

					for (int j = 0; j < groupVertex.getCurrentNumberOfGroupMembers(); j++) {
						list.add(groupVertex.getGroupMember(j));
					}

					break;
				}
			}
		}

		return list;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialExecutionResourcesExhausted(final JobID jobID, final ExecutionVertexID vertexID,
			final ResourceUtilizationSnapshot resourceUtilizationSnapshot) {

		// Nothing to do here
	}

	public List<ExecutionVertex> getFailedVertices() {

		return this.recovering;
	}

	/**
	 * Reconstructs the execution pipelines for the entire execution graph.
	 */
	private void reconstructExecutionPipelines() {

		final Iterator<ExecutionStage> it = this.stages.iterator();
		while (it.hasNext()) {

			it.next().reconstructExecutionPipelines();
		}
	}

	/**
	 * Returns an iterator over all execution stages contained in this graph.
	 * 
	 * @return an iterator over all execution stages contained in this graph
	 */
	public Iterator<ExecutionStage> iterator() {

		return this.stages.iterator();
	}
}
