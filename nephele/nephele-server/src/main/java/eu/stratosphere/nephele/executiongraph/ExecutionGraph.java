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
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
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
	 * Mapping of channel IDs to edges.
	 */
	private final ConcurrentMap<ChannelID, ExecutionEdge> edgeMap = new ConcurrentHashMap<ChannelID, ExecutionEdge>(
		1024 * 1024);

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
	public ExecutionGraph(final JobGraph job, final InstanceManager instanceManager)
																					throws GraphConversionException {
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

		// Second, we create the number of execution vertices each group vertex is supposed to manage
		Iterator<ExecutionGroupVertex> it2 = new ExecutionGroupVertexIterator(this, true, -1);
		while (it2.hasNext()) {

			final ExecutionGroupVertex groupVertex = it2.next();
			if (groupVertex.isNumberOfMembersUserDefined()) {
				groupVertex.createInitialExecutionVertices(groupVertex.getUserDefinedNumberOfMembers());
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

				// Create edges between execution vertices
				createExecutionEdgesForGroupEdge(edge);
			}

			// Update initial checkpoint state for all group members
			final CheckpointState ics = groupVertex.checkInitialCheckpointState();
			final int currentNumberOfGroupMembers = groupVertex.getCurrentNumberOfGroupMembers();
			for (int i = 0; i < currentNumberOfGroupMembers; ++i) {
				groupVertex.getGroupMember(i).updateCheckpointState(ics);
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
			final ExecutionVertex createdVertex = createVertex(all[i], instanceManager, initialExecutionStage,
				jobGraph.getJobConfiguration());
			temporaryVertexMap.put(all[i], createdVertex);
			temporaryGroupVertexMap.put(all[i], createdVertex.getGroupVertex());
		}

		// Create initial edges between the vertices
		createInitialGroupEdges(temporaryVertexMap);

		// Now that an initial graph is built, apply the user settings
		applyUserDefinedSettings(temporaryGroupVertexMap);

		// Calculate the connection IDs
		calculateConnectionIDs();

		// Finally, construct the execution pipelines
		reconstructExecutionPipelines();
	}

	private void createExecutionEdgesForGroupEdge(final ExecutionGroupEdge groupEdge) {

		final ExecutionGroupVertex source = groupEdge.getSourceVertex();
		final int indexOfOutputGate = groupEdge.getIndexOfOutputGate();
		final ExecutionGroupVertex target = groupEdge.getTargetVertex();
		final int indexOfInputGate = groupEdge.getIndexOfInputGate();

		final Map<GateID, List<ExecutionEdge>> inputChannelMap = new HashMap<GateID, List<ExecutionEdge>>();

		// Unwire the respective gate of the source vertices
		final int currentNumberOfSourceNodes = source.getCurrentNumberOfGroupMembers();
		for (int i = 0; i < currentNumberOfSourceNodes; ++i) {

			final ExecutionVertex sourceVertex = source.getGroupMember(i);
			final ExecutionGate outputGate = sourceVertex.getOutputGate(indexOfOutputGate);
			if (outputGate == null) {
				throw new IllegalStateException("wire: " + sourceVertex.getName()
					+ " has no output gate with index " + indexOfOutputGate);
			}

			if (outputGate.getNumberOfEdges() > 0) {
				throw new IllegalStateException("wire: wire called on source " + sourceVertex.getName() + " (" + i
					+ "), but number of output channels is " + outputGate.getNumberOfEdges() + "!");
			}

			final int currentNumberOfTargetNodes = target.getCurrentNumberOfGroupMembers();
			final List<ExecutionEdge> outputChannels = new ArrayList<ExecutionEdge>();

			for (int j = 0; j < currentNumberOfTargetNodes; ++j) {

				final ExecutionVertex targetVertex = target.getGroupMember(j);
				final ExecutionGate inputGate = targetVertex.getInputGate(indexOfInputGate);
				if (inputGate == null) {
					throw new IllegalStateException("wire: " + targetVertex.getName()
						+ " has no input gate with index " + indexOfInputGate);
				}

				if (inputGate.getNumberOfEdges() > 0 && i == 0) {
					throw new IllegalStateException("wire: wire called on target " + targetVertex.getName() + " ("
						+ j + "), but number of input channels is " + inputGate.getNumberOfEdges() + "!");
				}

				// Check if a wire is supposed to be created
				if (DistributionPatternProvider.createWire(groupEdge.getDistributionPattern(),
					i, j, currentNumberOfSourceNodes, currentNumberOfTargetNodes)) {

					final ChannelID outputChannelID = new ChannelID();
					final ChannelID inputChannelID = new ChannelID();

					final ExecutionEdge edge = new ExecutionEdge(outputGate, inputGate, groupEdge, outputChannelID,
						inputChannelID, outputGate.getNumberOfEdges(), inputGate.getNumberOfEdges());

					this.edgeMap.put(outputChannelID, edge);
					this.edgeMap.put(inputChannelID, edge);

					outputChannels.add(edge);

					List<ExecutionEdge> inputChannels = inputChannelMap.get(inputGate.getGateID());
					if (inputChannels == null) {
						inputChannels = new ArrayList<ExecutionEdge>();
						inputChannelMap.put(inputGate.getGateID(), inputChannels);
					}

					inputChannels.add(edge);
				}
			}

			outputGate.replaceAllEdges(outputChannels);
		}

		// Finally, set the channels for the input gates
		final int currentNumberOfTargetNodes = target.getCurrentNumberOfGroupMembers();
		for (int i = 0; i < currentNumberOfTargetNodes; ++i) {

			final ExecutionVertex targetVertex = target.getGroupMember(i);
			final ExecutionGate inputGate = targetVertex.getInputGate(indexOfInputGate);

			final List<ExecutionEdge> inputChannels = inputChannelMap.get(inputGate.getGateID());
			if (inputChannels == null) {
				LOG.error("Cannot find input channels for gate ID " + inputGate.getGateID());
				continue;
			}

			inputGate.replaceAllEdges(inputChannels);
		}

	}

	/**
	 * Creates the initial edges between the group vertices
	 * 
	 * @param vertexMap
	 *        the temporary vertex map
	 * @throws GraphConversionException
	 *         if the initial wiring cannot be created
	 */
	private void createInitialGroupEdges(final HashMap<AbstractJobVertex, ExecutionVertex> vertexMap)
			throws GraphConversionException {

		Iterator<Map.Entry<AbstractJobVertex, ExecutionVertex>> it = vertexMap.entrySet().iterator();

		while (it.hasNext()) {

			final Map.Entry<AbstractJobVertex, ExecutionVertex> entry = it.next();
			final AbstractJobVertex sjv = entry.getKey();
			final ExecutionVertex sev = entry.getValue();
			final ExecutionGroupVertex sgv = sev.getGroupVertex();

			// First compare number of output gates
			if (sjv.getNumberOfForwardConnections() != sgv.getEnvironment().getNumberOfOutputGates()) {
				throw new GraphConversionException("Job and execution vertex " + sjv.getName()
					+ " have different number of outputs");
			}

			if (sjv.getNumberOfBackwardConnections() != sgv.getEnvironment().getNumberOfInputGates()) {
				throw new GraphConversionException("Job and execution vertex " + sjv.getName()
					+ " have different number of inputs");
			}

			// First, build the group edges
			for (int i = 0; i < sjv.getNumberOfForwardConnections(); ++i) {

				final boolean isBroadcast = sgv.getEnvironment().getOutputGate(i).isBroadcast();

				final JobEdge edge = sjv.getForwardConnection(i);
				final AbstractJobVertex tjv = edge.getConnectedVertex();

				final ExecutionVertex tev = vertexMap.get(tjv);
				final ExecutionGroupVertex tgv = tev.getGroupVertex();
				// Use NETWORK as default channel type if nothing else is defined by the user
				ChannelType channelType = edge.getChannelType();
				boolean userDefinedChannelType = true;
				if (channelType == null) {
					userDefinedChannelType = false;
					channelType = ChannelType.NETWORK;
				}
				// Use NO_COMPRESSION as default compression level if nothing else is defined by the user
				CompressionLevel compressionLevel = edge.getCompressionLevel();
				boolean userDefinedCompressionLevel = true;
				if (compressionLevel == null) {
					userDefinedCompressionLevel = false;
					compressionLevel = CompressionLevel.NO_COMPRESSION;
				}

				final DistributionPattern distributionPattern = edge.getDistributionPattern();

				// Connect the corresponding group vertices and copy the user settings from the job edge
				final ExecutionGroupEdge groupEdge = sgv.wireTo(tgv, edge.getIndexOfInputGate(), i, channelType,
					userDefinedChannelType, compressionLevel, userDefinedCompressionLevel, distributionPattern,
					isBroadcast);

				final ExecutionGate outputGate = new ExecutionGate(new GateID(), sev, groupEdge, false);
				sev.insertOutputGate(i, outputGate);
				final ExecutionGate inputGate = new ExecutionGate(new GateID(), tev, groupEdge, true);
				tev.insertInputGate(edge.getIndexOfInputGate(), inputGate);
			}
		}
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
	 * @param jobConfiguration
	 *        the configuration object originally attached to the {@link JobGraph}
	 * @return the new execution vertex
	 * @throws GraphConversionException
	 *         thrown if the job vertex is of an unknown subclass
	 */
	private ExecutionVertex createVertex(final AbstractJobVertex jobVertex, final InstanceManager instanceManager,
			final ExecutionStage initialExecutionStage, final Configuration jobConfiguration)
			throws GraphConversionException {

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

		// Create an initial execution vertex for the job vertex
		final Class<? extends AbstractInvokable> invokableClass = jobVertex.getInvokableClass();
		if (invokableClass == null) {
			throw new GraphConversionException("JobVertex " + jobVertex.getID() + " (" + jobVertex.getName()
				+ ") does not specify a task");
		}

		// Calculate the cryptographic signature of this vertex
		final ExecutionSignature signature = ExecutionSignature.createSignature(jobVertex.getInvokableClass(),
			jobVertex.getJobGraph().getJobID());

		// Create a group vertex for the job vertex

		ExecutionGroupVertex groupVertex = null;
		try {
			groupVertex = new ExecutionGroupVertex(jobVertex.getName(), jobVertex.getID(), this,
				jobVertex.getNumberOfSubtasks(), instanceType, userDefinedInstanceType,
				jobVertex.getNumberOfSubtasksPerInstance(), jobVertex.getVertexToShareInstancesWith() != null ? true
					: false, jobVertex.getNumberOfExecutionRetries(), jobVertex.getConfiguration(), signature,
				invokableClass);
		} catch (Throwable t) {
			throw new GraphConversionException(StringUtils.stringifyException(t));
		}

		// Run the configuration check the user has provided for the vertex
		try {
			jobVertex.checkConfiguration(groupVertex.getEnvironment().getInvokable());
		} catch (IllegalConfigurationException e) {
			throw new GraphConversionException(StringUtils.stringifyException(e));
		}

		// Check if the user's specifications for the number of subtasks are valid
		final int minimumNumberOfSubtasks = jobVertex.getMinimumNumberOfSubtasks(groupVertex.getEnvironment()
			.getInvokable());
		final int maximumNumberOfSubtasks = jobVertex.getMaximumNumberOfSubtasks(groupVertex.getEnvironment()
			.getInvokable());
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

		// Register input and output vertices separately
		if (jobVertex instanceof AbstractJobInputVertex) {

			final InputSplit[] inputSplits;

			// let the task code compute the input splits
			if (groupVertex.getEnvironment().getInvokable() instanceof AbstractInputTask) {
				try {
					inputSplits = ((AbstractInputTask<?>) groupVertex.getEnvironment().getInvokable())
						.computeInputSplits(jobVertex.getNumberOfSubtasks());
				} catch (Exception e) {
					throw new GraphConversionException("Cannot compute input splits for " + groupVertex.getName()
						+ ": " + StringUtils.stringifyException(e));
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

		// Add group vertex to initial execution stage
		initialExecutionStage.addStageMember(groupVertex);

		final ExecutionVertex ev = new ExecutionVertex(this, groupVertex, jobVertex.getNumberOfForwardConnections(),
			jobVertex.getNumberOfBackwardConnections());

		// Assign initial instance to vertex (may be overwritten later on when user settings are applied)
		ev.setAllocatedResource(new AllocatedResource(DummyInstance.createDummyInstance(instanceType), instanceType,
			null));

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

		final ExecutionEdge edge = this.edgeMap.get(id);
		if (edge == null) {
			return null;
		}

		if (id.equals(edge.getOutputChannelID())) {
			return edge.getOutputGate().getVertex();
		}

		return edge.getInputGate().getVertex();
	}

	/**
	 * Finds an {@link ExecutionEdge} by its ID and returns it.
	 * 
	 * @param id
	 *        the channel ID to identify the edge
	 * @return the edge whose ID matches <code>id</code> or <code>null</code> if no such edge is known
	 */
	public ExecutionEdge getEdgeByID(final ChannelID id) {

		return this.edgeMap.get(id);
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

			for (int i = 0; i < sourceVertex.getNumberOfOutputGates(); ++i) {

				final ExecutionGate outputGate = sourceVertex.getOutputGate(i);
				final ChannelType channelType = outputGate.getChannelType();
				if (channelType == ChannelType.FILE || channelType == ChannelType.INMEMORY) {
					final int numberOfOutputChannels = outputGate.getNumberOfEdges();
					for (int j = 0; j < numberOfOutputChannels; ++j) {
						final ExecutionEdge outputChannel = outputGate.getEdge(j);
						outputChannel.getInputGate().getVertex()
							.setAllocatedResource(sourceVertex.getAllocatedResource());
					}
				}
			}
		}

		it = new ExecutionGraphIterator(this, false);
		while (it.hasNext()) {

			final ExecutionVertex targetVertex = it.next();

			for (int i = 0; i < targetVertex.getNumberOfInputGates(); ++i) {

				final ExecutionGate inputGate = targetVertex.getInputGate(i);
				final ChannelType channelType = inputGate.getChannelType();
				if (channelType == ChannelType.FILE || channelType == ChannelType.INMEMORY) {
					final int numberOfInputChannels = inputGate.getNumberOfEdges();
					for (int j = 0; j < numberOfInputChannels; ++j) {
						final ExecutionEdge inputChannel = inputGate.getEdge(j);
						inputChannel.getOutputGate().getVertex()
							.setAllocatedResource(targetVertex.getAllocatedResource());
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
		final ExecutionGate outputGate = sourceVertex.getOutputGate(edge.getIndexOfOutputGate());
		for (int i = 0; i < outputGate.getNumberOfEdges(); ++i) {

			final ExecutionEdge outputChannel = outputGate.getEdge(i);
			if (targetVertex == outputChannel.getInputGate().getVertex()) {
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

	private static InternalJobStatus determineNewJobStatus(final ExecutionGraph eg,
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
			if (latestStateChange == ExecutionState.CANCELED) {
				return InternalJobStatus.CANCELING;
			}
			if (latestStateChange == ExecutionState.FAILED) {

				final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
				while (it.hasNext()) {

					final ExecutionVertex vertex = it.next();
					if (vertex.getExecutionState() == ExecutionState.FAILED) {
						return InternalJobStatus.FAILING;
					}
				}
			}
			if (eg.jobHasFinishedStatus()) {
				return InternalJobStatus.FINISHED;
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
			LOG.error("Received update of execute state in job status CANCELED: " + eg.getJobID());
			break;
		case FINISHED:
			LOG.error("Received update of execute state in job status FINISHED: " + eg.getJobID() + " "
				+ StringUtils.stringifyException(new Throwable()));
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

		// Do not use the parameter newExecutionState here as it may already be out-dated

		final ExecutionVertex vertex = getVertexByID(vertexID);
		if (vertex == null) {
			LOG.error("Cannot find execution vertex with the ID " + vertexID);
		}

		final ExecutionState actualExecutionState = vertex.getExecutionState();

		final InternalJobStatus newJobStatus = determineNewJobStatus(this, actualExecutionState);

		if (actualExecutionState == ExecutionState.FINISHED) {
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
	 * Reconstructs the execution pipelines for the entire execution graph.
	 */
	private void reconstructExecutionPipelines() {

		final Iterator<ExecutionStage> it = this.stages.iterator();
		while (it.hasNext()) {

			it.next().reconstructExecutionPipelines();
		}
	}

	/**
	 * Calculates the connection IDs of the graph to avoid deadlocks in the data flow at runtime.
	 */
	private void calculateConnectionIDs() {

		final Set<ExecutionGroupVertex> alreadyVisited = new HashSet<ExecutionGroupVertex>();
		final ExecutionStage lastStage = getStage(getNumberOfStages() - 1);

		for (int i = 0; i < lastStage.getNumberOfStageMembers(); ++i) {

			final ExecutionGroupVertex groupVertex = lastStage.getStageMember(i);
			if (groupVertex.isOutputVertex()) {
				groupVertex.calculateConnectionID(0, alreadyVisited);
			}
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPriority() {

		return 1;
	}
}
