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

package eu.stratosphere.nephele.jobmanager.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertexIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionPipeline;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;
import eu.stratosphere.util.StringUtils;

/**
 * This abstract scheduler must be extended by a scheduler implementations for Nephele. The abstract class defines the
 * fundamental methods for scheduling and removing jobs. While Nephele's
 * {@link eu.stratosphere.nephele.jobmanager.JobManager} is responsible for requesting the required instances for the
 * job at the {@link eu.stratosphere.nephele.instance.InstanceManager}, the scheduler is in charge of assigning the
 * individual tasks to the instances.
 * 
 */
public abstract class AbstractScheduler implements InstanceListener {

	/**
	 * The LOG object to report events within the scheduler.
	 */
	protected static final Log LOG = LogFactory.getLog(AbstractScheduler.class);

	/**
	 * The instance manager assigned to this scheduler.
	 */
	private final InstanceManager instanceManager;

	/**
	 * The deployment manager assigned to this scheduler.
	 */
	private final DeploymentManager deploymentManager;

	/**
	 * Stores the vertices to be restarted once they have switched to the <code>CANCELED</code> state.
	 */
	private final Map<ExecutionVertexID, ExecutionVertex> verticesToBeRestarted = new ConcurrentHashMap<ExecutionVertexID, ExecutionVertex>();

	/**
	 * Constructs a new abstract scheduler.
	 * 
	 * @param deploymentManager
	 *        the deployment manager assigned to this scheduler
	 * @param instanceManager
	 *        the instance manager to be used with this scheduler
	 */
	protected AbstractScheduler(final DeploymentManager deploymentManager, final InstanceManager instanceManager) {

		this.deploymentManager = deploymentManager;
		this.instanceManager = instanceManager;
		this.instanceManager.setInstanceListener(this);
	}

	/**
	 * Adds a job represented by an {@link ExecutionGraph} object to the scheduler. The job is then executed according
	 * to the strategies of the concrete scheduler implementation.
	 * 
	 * @param executionGraph
	 *        the job to be added to the scheduler
	 * @throws SchedulingException
	 *         thrown if an error occurs and the scheduler does not accept the new job
	 */
	public abstract void schedulJob(ExecutionGraph executionGraph) throws SchedulingException;

	/**
	 * Returns the execution graph which is associated with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID to search the execution graph for
	 * @return the execution graph which belongs to the given job ID or <code>null</code if no such execution graph
	 *         exists
	 */
	public abstract ExecutionGraph getExecutionGraphByID(JobID jobID);

	/**
	 * Returns the {@link InstanceManager} object which is used by the current scheduler.
	 * 
	 * @return the {@link InstanceManager} object which is used by the current scheduler
	 */
	public InstanceManager getInstanceManager() {
		return this.instanceManager;
	}

	// void removeJob(JobID jobID);

	/**
	 * Shuts the scheduler down. After shut down no jobs can be added to the scheduler.
	 */
	public abstract void shutdown();

	/**
	 * Collects the instances required to run the job from the given {@link ExecutionStage} and requests them at the
	 * loaded instance manager.
	 * 
	 * @param executionStage
	 *        the execution stage to collect the required instances from
	 * @throws InstanceException
	 *         thrown if the given execution graph is already processing its final stage
	 */
	protected void requestInstances(final ExecutionStage executionStage) throws InstanceException {

		final ExecutionGraph executionGraph = executionStage.getExecutionGraph();
		final InstanceRequestMap instanceRequestMap = new InstanceRequestMap();

		synchronized (executionStage) {

			executionStage.collectRequiredInstanceTypes(instanceRequestMap, ExecutionState.CREATED);

			final Iterator<Map.Entry<InstanceType, Integer>> it = instanceRequestMap.getMinimumIterator();
			LOG.info("Requesting the following instances for job " + executionGraph.getJobID());
			while (it.hasNext()) {
				final Map.Entry<InstanceType, Integer> entry = it.next();
				LOG.info(" " + entry.getKey() + " [" + entry.getValue().intValue() + ", "
					+ instanceRequestMap.getMaximumNumberOfInstances(entry.getKey()) + "]");
			}

			if (instanceRequestMap.isEmpty()) {
				return;
			}

			this.instanceManager.requestInstance(executionGraph.getJobID(), executionGraph.getJobConfiguration(),
				instanceRequestMap, null);

			// Switch vertex state to assigning
			final ExecutionGraphIterator it2 = new ExecutionGraphIterator(executionGraph, executionGraph
				.getIndexOfCurrentExecutionStage(), true, true);
			while (it2.hasNext()) {

				it2.next().compareAndUpdateExecutionState(ExecutionState.CREATED, ExecutionState.SCHEDULED);
			}
		}
	}

	void findVerticesToBeDeployed(final ExecutionVertex vertex,
			final Map<AbstractInstance, List<ExecutionVertex>> verticesToBeDeployed,
			final Set<ExecutionVertex> alreadyVisited) {

		if (!alreadyVisited.add(vertex)) {
			return;
		}

		if (vertex.compareAndUpdateExecutionState(ExecutionState.ASSIGNED, ExecutionState.READY)) {
			final AbstractInstance instance = vertex.getAllocatedResource().getInstance();

			if (instance instanceof DummyInstance) {
				LOG.error("Inconsistency: Vertex " + vertex + " is about to be deployed on a DummyInstance");
			}

			List<ExecutionVertex> verticesForInstance = verticesToBeDeployed.get(instance);
			if (verticesForInstance == null) {
				verticesForInstance = new ArrayList<ExecutionVertex>();
				verticesToBeDeployed.put(instance, verticesForInstance);
			}

			verticesForInstance.add(vertex);
		}

		final int numberOfOutputGates = vertex.getNumberOfOutputGates();
		for (int i = 0; i < numberOfOutputGates; ++i) {

			final ExecutionGate outputGate = vertex.getOutputGate(i);
			boolean deployTarget;

			switch (outputGate.getChannelType()) {
			case NETWORK:
				deployTarget = false;
				break;
			case IN_MEMORY:
				deployTarget = true;
				break;
			default:
				throw new IllegalStateException("Unknown channel type");
			}

			if (deployTarget) {

				final int numberOfOutputChannels = outputGate.getNumberOfEdges();
				for (int j = 0; j < numberOfOutputChannels; ++j) {
					final ExecutionEdge outputChannel = outputGate.getEdge(j);
					final ExecutionVertex connectedVertex = outputChannel.getInputGate().getVertex();
					findVerticesToBeDeployed(connectedVertex, verticesToBeDeployed, alreadyVisited);
				}
			}
		}
	}

	/**
	 * Collects all execution vertices with the state ASSIGNED starting from the given start vertex and
	 * deploys them on the assigned {@link AllocatedResource} objects.
	 * 
	 * @param startVertex
	 *        the execution vertex to start the deployment from
	 */
	public void deployAssignedVertices(final ExecutionVertex startVertex) {

		final JobID jobID = startVertex.getExecutionGraph().getJobID();

		final Map<AbstractInstance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<AbstractInstance, List<ExecutionVertex>>();
		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();

		findVerticesToBeDeployed(startVertex, verticesToBeDeployed, alreadyVisited);

		if (!verticesToBeDeployed.isEmpty()) {

			final Iterator<Map.Entry<AbstractInstance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
				.entrySet()
				.iterator();

			while (it2.hasNext()) {

				final Map.Entry<AbstractInstance, List<ExecutionVertex>> entry = it2.next();
				this.deploymentManager.deploy(jobID, entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * Collects all execution vertices with the state ASSIGNED from the given pipeline and deploys them on the assigned
	 * {@link AllocatedResource} objects.
	 * 
	 * @param pipeline
	 *        the execution pipeline to be deployed
	 */
	public void deployAssignedPipeline(final ExecutionPipeline pipeline) {

		final JobID jobID = null;

		final Map<AbstractInstance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<AbstractInstance, List<ExecutionVertex>>();
		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();

		final Iterator<ExecutionVertex> it = pipeline.iterator();
		while (it.hasNext()) {
			findVerticesToBeDeployed(it.next(), verticesToBeDeployed, alreadyVisited);
		}

		if (!verticesToBeDeployed.isEmpty()) {

			final Iterator<Map.Entry<AbstractInstance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
				.entrySet()
				.iterator();

			while (it2.hasNext()) {

				final Map.Entry<AbstractInstance, List<ExecutionVertex>> entry = it2.next();
				this.deploymentManager.deploy(jobID, entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * Collects all execution vertices with the state ASSIGNED starting from the given collection of start vertices and
	 * deploys them on the assigned {@link AllocatedResource} objects.
	 * 
	 * @param startVertices
	 *        the collection of execution vertices to start the deployment from
	 */
	public void deployAssignedVertices(final Collection<ExecutionVertex> startVertices) {

		JobID jobID = null;

		final Map<AbstractInstance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<AbstractInstance, List<ExecutionVertex>>();
		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();

		for (final ExecutionVertex startVertex : startVertices) {

			if (jobID == null) {
				jobID = startVertex.getExecutionGraph().getJobID();
			}

			findVerticesToBeDeployed(startVertex, verticesToBeDeployed, alreadyVisited);
		}

		if (!verticesToBeDeployed.isEmpty()) {

			final Iterator<Map.Entry<AbstractInstance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
				.entrySet()
				.iterator();

			while (it2.hasNext()) {

				final Map.Entry<AbstractInstance, List<ExecutionVertex>> entry = it2.next();
				this.deploymentManager.deploy(jobID, entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * Collects all execution vertices with the state ASSIGNED starting from the input vertices of the current execution
	 * stage and deploys them on the assigned {@link AllocatedResource} objects.
	 * 
	 * @param executionGraph
	 *        the execution graph to collect the vertices from
	 */
	public void deployAssignedInputVertices(final ExecutionGraph executionGraph) {

		final Map<AbstractInstance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<AbstractInstance, List<ExecutionVertex>>();
		final ExecutionStage executionStage = executionGraph.getCurrentExecutionStage();

		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();

		for (int i = 0; i < executionStage.getNumberOfStageMembers(); ++i) {

			final ExecutionGroupVertex startVertex = executionStage.getStageMember(i);
			if (!startVertex.isInputVertex()) {
				continue;
			}

			for (int j = 0; j < startVertex.getCurrentNumberOfGroupMembers(); ++j) {
				final ExecutionVertex vertex = startVertex.getGroupMember(j);
				findVerticesToBeDeployed(vertex, verticesToBeDeployed, alreadyVisited);
			}
		}

		if (!verticesToBeDeployed.isEmpty()) {

			final Iterator<Map.Entry<AbstractInstance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
				.entrySet()
				.iterator();

			while (it2.hasNext()) {

				final Map.Entry<AbstractInstance, List<ExecutionVertex>> entry = it2.next();
				this.deploymentManager.deploy(executionGraph.getJobID(), entry.getKey(), entry.getValue());
			}
		}
	}


	@Override
	public void resourcesAllocated(final JobID jobID, final List<AllocatedResource> allocatedResources) {

		if (allocatedResources == null) {
			LOG.error("Resource to lock is null!");
			return;
		}

		for (final AllocatedResource allocatedResource : allocatedResources) {
			if (allocatedResource.getInstance() instanceof DummyInstance) {
				LOG.debug("Available instance is of type DummyInstance!");
				return;
			}
		}

		final ExecutionGraph eg = getExecutionGraphByID(jobID);

		if (eg == null) {
			/*
			 * The job have have been canceled in the meantime, in this case
			 * we release the instance immediately.
			 */
			try {
				for (final AllocatedResource allocatedResource : allocatedResources) {
					getInstanceManager().releaseAllocatedResource(jobID, null, allocatedResource);
				}
			} catch (InstanceException e) {
				LOG.error(e);
			}
			return;
		}

		final Runnable command = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {

				final ExecutionStage stage = eg.getCurrentExecutionStage();

				synchronized (stage) {

					for (final AllocatedResource allocatedResource : allocatedResources) {

						AllocatedResource resourceToBeReplaced = null;
						// Important: only look for instances to be replaced in the current stage
						final Iterator<ExecutionGroupVertex> groupIterator = new ExecutionGroupVertexIterator(eg, true,
							stage.getStageNumber());
						while (groupIterator.hasNext()) {

							final ExecutionGroupVertex groupVertex = groupIterator.next();
							for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); ++i) {

								final ExecutionVertex vertex = groupVertex.getGroupMember(i);

								if (vertex.getExecutionState() == ExecutionState.SCHEDULED
									&& vertex.getAllocatedResource() != null) {
									// In local mode, we do not consider any topology, only the instance type
									if (vertex.getAllocatedResource().getInstanceType().equals(
										allocatedResource.getInstanceType())) {
										resourceToBeReplaced = vertex.getAllocatedResource();
										break;
									}
								}
							}

							if (resourceToBeReplaced != null) {
								break;
							}
						}

						// For some reason, we don't need this instance
						if (resourceToBeReplaced == null) {
							LOG.error("Instance " + allocatedResource.getInstance() + " is not required for job"
								+ eg.getJobID());
							try {
								getInstanceManager().releaseAllocatedResource(jobID, eg.getJobConfiguration(),
									allocatedResource);
							} catch (InstanceException e) {
								LOG.error(e);
							}
							return;
						}

						// Replace the selected instance
						final Iterator<ExecutionVertex> it = resourceToBeReplaced.assignedVertices();
						while (it.hasNext()) {
							final ExecutionVertex vertex = it.next();
							vertex.setAllocatedResource(allocatedResource);
							vertex.updateExecutionState(ExecutionState.ASSIGNED);
						}
					}
				}

				// Deploy the assigned vertices
				deployAssignedInputVertices(eg);

			}

		};

		eg.executeCommand(command);
	}

	/**
	 * Checks if the given {@link AllocatedResource} is still required for the
	 * execution of the given execution graph. If the resource is no longer
	 * assigned to a vertex that is either currently running or about to run
	 * the given resource is returned to the instance manager for deallocation.
	 * 
	 * @param executionGraph
	 *        the execution graph the provided resource has been used for so far
	 * @param allocatedResource
	 *        the allocated resource to check the assignment for
	 */
	public void checkAndReleaseAllocatedResource(final ExecutionGraph executionGraph,
			final AllocatedResource allocatedResource) {

		if (allocatedResource == null) {
			LOG.error("Resource to lock is null!");
			return;
		}

		if (allocatedResource.getInstance() instanceof DummyInstance) {
			LOG.debug("Available instance is of type DummyInstance!");
			return;
		}

		boolean resourceCanBeReleased = true;
		final Iterator<ExecutionVertex> it = allocatedResource.assignedVertices();
		while (it.hasNext()) {
			final ExecutionVertex vertex = it.next();
			final ExecutionState state = vertex.getExecutionState();

			if (state != ExecutionState.CREATED && state != ExecutionState.FINISHED
				&& state != ExecutionState.FAILED && state != ExecutionState.CANCELED) {

				resourceCanBeReleased = false;
				break;
			}
		}

		if (resourceCanBeReleased) {

			LOG.info("Releasing instance " + allocatedResource.getInstance());
			try {
				getInstanceManager().releaseAllocatedResource(executionGraph.getJobID(), executionGraph
					.getJobConfiguration(), allocatedResource);
			} catch (InstanceException e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}
	}

	DeploymentManager getDeploymentManager() {
		return this.deploymentManager;
	}

	protected void replayCheckpointsFromPreviousStage(final ExecutionGraph executionGraph) {

		final int currentStageIndex = executionGraph.getIndexOfCurrentExecutionStage();
		final ExecutionStage previousStage = executionGraph.getStage(currentStageIndex - 1);

		final List<ExecutionVertex> verticesToBeReplayed = new ArrayList<ExecutionVertex>();

		for (int i = 0; i < previousStage.getNumberOfOutputExecutionVertices(); ++i) {

			final ExecutionVertex vertex = previousStage.getOutputExecutionVertex(i);
			vertex.updateExecutionState(ExecutionState.ASSIGNED);
			verticesToBeReplayed.add(vertex);
		}

		deployAssignedVertices(verticesToBeReplayed);
	}

	/**
	 * Returns a map of vertices to be restarted once they have switched to their <code>CANCELED</code> state.
	 * 
	 * @return the map of vertices to be restarted
	 */
	Map<ExecutionVertexID, ExecutionVertex> getVerticesToBeRestarted() {

		return this.verticesToBeRestarted;
	}


	@Override
	public void allocatedResourcesDied(final JobID jobID, final List<AllocatedResource> allocatedResources) {

		final ExecutionGraph eg = getExecutionGraphByID(jobID);

		if (eg == null) {
			LOG.error("Cannot find execution graph for job with ID " + jobID);
			return;
		}

		final Runnable command = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {

				synchronized (eg) {

					for (final AllocatedResource allocatedResource : allocatedResources) {

						LOG.info("Resource " + allocatedResource.getInstance().getName() + " for Job " + jobID
							+ " died.");

						final ExecutionGraph executionGraph = getExecutionGraphByID(jobID);

						if (executionGraph == null) {
							LOG.error("Cannot find execution graph for job " + jobID);
							return;
						}

						Iterator<ExecutionVertex> vertexIter = allocatedResource.assignedVertices();

						// Assign vertices back to a dummy resource.
						final DummyInstance dummyInstance = DummyInstance.createDummyInstance(allocatedResource
							.getInstance()
							.getType());
						final AllocatedResource dummyResource = new AllocatedResource(dummyInstance,
							allocatedResource.getInstanceType(), new AllocationID());

						while (vertexIter.hasNext()) {
							final ExecutionVertex vertex = vertexIter.next();
							vertex.setAllocatedResource(dummyResource);
						}

						final String failureMessage = allocatedResource.getInstance().getName() + " died";

						vertexIter = allocatedResource.assignedVertices();

						while (vertexIter.hasNext()) {
							final ExecutionVertex vertex = vertexIter.next();
							final ExecutionState state = vertex.getExecutionState();

							switch (state) {
							case ASSIGNED:
							case READY:
							case STARTING:
							case RUNNING:
							case FINISHING:

							vertex.updateExecutionState(ExecutionState.FAILED, failureMessage);

							break;
						default:
							}
					}

					// TODO: Fix this
					/*
					 * try {
					 * requestInstances(this.executionVertex.getGroupVertex().getExecutionStage());
					 * } catch (InstanceException e) {
					 * e.printStackTrace();
					 * // TODO: Cancel the entire job in this case
					 * }
					 */
				}
			}

			final InternalJobStatus js = eg.getJobStatus();
			if (js != InternalJobStatus.FAILING && js != InternalJobStatus.FAILED) {

				// TODO: Fix this
				// deployAssignedVertices(eg);

				final ExecutionStage stage = eg.getCurrentExecutionStage();

				try {
					requestInstances(stage);
				} catch (InstanceException e) {
					e.printStackTrace();
					// TODO: Cancel the entire job in this case
				}
			}
		}
		};

		eg.executeCommand(command);
	}
}
