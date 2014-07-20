/**
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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * The scheduler is responsible for distributing the ready-to-run tasks and assigning them to instances and
 * slots.
 * <p>
 * The scheduler's bookkeeping on the available instances is lazy: It is not modified once an
 * instance is dead, but it will lazily remove the instance from its pool as soon as it tries
 * to allocate a resource on that instance and it fails with an {@link InstanceDiedException}.
 */
public class DefaultScheduler implements InstanceListener {

	protected static final Logger LOG = LoggerFactory.getLogger(DefaultScheduler.class);

	
	private final Object lock = new Object();
	
	/** All instances that the scheduler can deploy to */
	private final Set<Instance> allInstances = new HashSet<Instance>();
	
	/** All instances that still have available resources */
	private final Queue<Instance> instancesWithAvailableResources = new LifoSetQueue<Instance>();

	
	private final ConcurrentHashMap<ResourceId, AllocatedSlot> allocatedSlots = new ConcurrentHashMap<ResourceId, AllocatedSlot>();
	
//	/** A cache that remembers the last resource IDs it has seen, to co-locate future
//	 *  deployments of tasks with the same resource ID to the same instance.
//	 */
//	private final Cache<ResourceId, Instance> ghostCache;
	
	
	/** All tasks pending to be scheduled */
	private final LinkedBlockingQueue<ScheduledUnit> taskQueue = new LinkedBlockingQueue<ScheduledUnit>();

	
	/** The thread that runs the scheduling loop, picking up tasks to be scheduled and scheduling them. */
	private final Thread schedulerThread;
	
	
	/** Atomic flag to safely control the shutdown */
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	
	/** Flag indicating whether the scheduler should reject a unit if it cannot find a resource
	 * for it at the time of scheduling */
	private final boolean rejectIfNoResourceAvailable;
	

	
	public DefaultScheduler() {
		this(true);
	}
	
	public DefaultScheduler(boolean rejectIfNoResourceAvailable) {
		this.rejectIfNoResourceAvailable = rejectIfNoResourceAvailable;
		
		
//		this.ghostCache = CacheBuilder.newBuilder()
//				.initialCapacity(64)	// easy start
//				.maximumSize(1024)		// retain some history
//				.weakValues()			// do not prevent dead instances from being collected
//				.build();
		
		// set up (but do not start) the scheduling thread
		Runnable loopRunner = new Runnable() {
			@Override
			public void run() {
				runSchedulerLoop();
			}
		};
		this.schedulerThread = new Thread(loopRunner, "Scheduling Thread");
	}
	
	public void start() {
		if (shutdown.get()) {
			throw new IllegalStateException("Scheduler has been shut down.");
		}
		
		try {
			this.schedulerThread.start();
		}
		catch (IllegalThreadStateException e) {
			throw new IllegalStateException("The scheduler has already been started.");
		}
	}
	
	/**
	 * Shuts the scheduler down. After shut down no more tasks can be added to the scheduler.
	 */
	public void shutdown() {
		if (this.shutdown.compareAndSet(false, true)) {
			// clear the task queue and add the termination signal, to let
			// the scheduling loop know that things are done
			this.taskQueue.clear();
			this.taskQueue.add(TERMINATION_SIGNAL);
			
			// interrupt the scheduling thread, in case it was waiting for resources to
			// show up to deploy a task
			this.schedulerThread.interrupt();
		}
	}
	
	public void setUncaughtExceptionHandler(UncaughtExceptionHandler handler) {
		if (this.schedulerThread.getState() != Thread.State.NEW) {
			throw new IllegalStateException("Can only add exception handler before starting the scheduler.");
		}
		this.schedulerThread.setUncaughtExceptionHandler(handler);
	}

//	/**
//	 * Removes the job represented by the given {@link ExecutionGraph} from the scheduler.
//	 *
//	 * @param executionGraphToRemove
//	 *        the job to be removed
//	 */
//	void removeJobFromSchedule(final ExecutionGraph executionGraphToRemove) {
//
//		boolean removedFromQueue = false;
//
//		synchronized (this.jobQueue) {
//
//			final Iterator<ExecutionGraph> it = this.jobQueue.iterator();
//			while (it.hasNext()) {
//
//				final ExecutionGraph executionGraph = it.next();
//				if (executionGraph.getJobID().equals(executionGraphToRemove.getJobID())) {
//					removedFromQueue = true;
//					it.remove();
//					break;
//				}
//			}
//		}
//
//		if (!removedFromQueue) {
//			LOG.error("Cannot find job " + executionGraphToRemove.getJobName() + " ("
//					+ executionGraphToRemove.getJobID() + ") to remove");
//		}
//	}
//
//	/**
//	 * Adds a job represented by an {@link ExecutionGraph} object to the scheduler. The job is then executed according
//	 * to the strategies of the concrete scheduler implementation.
//	 *
//	 * @param executionGraph
//	 *        the job to be added to the scheduler
//	 * @throws SchedulingException
//	 *         thrown if an error occurs and the scheduler does not accept the new job
//	 */
//	public void scheduleJob(final ExecutionGraph executionGraph) throws SchedulingException {
//
//		final int requiredSlots = executionGraph.getRequiredSlots();
//		final int availableSlots = this.getInstanceManager().getNumberOfSlots();
//
//		if(requiredSlots > availableSlots){
//			throw new SchedulingException(String.format(
//					"Not enough available task slots to run job %s (%s). Required: %d Available: %d . "
//					+ "Either reduce the parallelism of your program, wait for other programs to finish, or increase "
//					+ "the number of task slots in the cluster by adding more machines or increasing the number of slots "
//					+ "per machine in conf/flink-conf.yaml .", 
//					executionGraph.getJobName(), executionGraph.getJobID(), requiredSlots, availableSlots));
//		}
//
//		// Subscribe to job status notifications
//		executionGraph.registerJobStatusListener(this);
//
//		// Register execution listener for each vertex
//		final ExecutionGraphIterator it2 = new ExecutionGraphIterator(executionGraph, true);
//		while (it2.hasNext()) {
//
//			final ExecutionVertex vertex = it2.next();
//			vertex.registerExecutionListener(new DefaultExecutionListener(this, vertex));
//		}
//
//		// Register the scheduler as an execution stage listener
//		executionGraph.registerExecutionStageListener(this);
//
//		// Add job to the job queue (important to add job to queue before requesting instances)
//		synchronized (this.jobQueue) {
//			this.jobQueue.add(executionGraph);
//		}
//
//		// Request resources for the first stage of the job
//
//		final ExecutionStage executionStage = executionGraph.getCurrentExecutionStage();
//		try {
//			requestInstances(executionStage);
//		} catch (InstanceException e) {
//			final String exceptionMessage = StringUtils.stringifyException(e);
//			LOG.error(exceptionMessage);
//			this.jobQueue.remove(executionGraph);
//			throw new SchedulingException(exceptionMessage);
//		}
//	}
//
//	/**
//	 * Returns the execution graph which is associated with the given job ID.
//	 *
//	 * @param jobID
//	 *        the job ID to search the execution graph for
//	 * @return the execution graph which belongs to the given job ID or <code>null</code if no such execution graph
//	 *         exists
//	 */
//	public ExecutionGraph getExecutionGraphByID(final JobID jobID) {
//
//		synchronized (this.jobQueue) {
//
//			final Iterator<ExecutionGraph> it = this.jobQueue.iterator();
//			while (it.hasNext()) {
//
//				final ExecutionGraph executionGraph = it.next();
//				if (executionGraph.getJobID().equals(jobID)) {
//					return executionGraph;
//				}
//			}
//		}
//
//		return null;
//	}
//
//
//
//	public void jobStatusHasChanged(final ExecutionGraph executionGraph, final InternalJobStatus newJobStatus,
//									final String optionalMessage) {
//
//		if (newJobStatus == InternalJobStatus.FAILED || newJobStatus == InternalJobStatus.FINISHED
//				|| newJobStatus == InternalJobStatus.CANCELED) {
//			removeJobFromSchedule(executionGraph);
//		}
//	}
//
//	public void nextExecutionStageEntered(final JobID jobID, final ExecutionStage executionStage) {
//
//		// Request new instances if necessary
//		try {
//			requestInstances(executionStage);
//		} catch (InstanceException e) {
//			// TODO: Handle error correctly
//			LOG.error(StringUtils.stringifyException(e));
//		}
//
//		// Deploy the assigned vertices
//		deployAssignedInputVertices(executionStage.getExecutionGraph());
//	}
//
//
//	/**
//	 * Returns the {@link org.apache.flink.runtime.instance.InstanceManager} object which is used by the current scheduler.
//	 * 
//	 * @return the {@link org.apache.flink.runtime.instance.InstanceManager} object which is used by the current scheduler
//	 */
//	public InstanceManager getInstanceManager() {
//		return this.instanceManager;
//	}
//
//
//	/**
//	 * Collects the instances required to run the job from the given {@link ExecutionStage} and requests them at the
//	 * loaded instance manager.
//	 * 
//	 * @param executionStage
//	 *        the execution stage to collect the required instances from
//	 * @throws InstanceException
//	 *         thrown if the given execution graph is already processing its final stage
//	 */
//	protected void requestInstances(final ExecutionStage executionStage) throws InstanceException {
//
//		final ExecutionGraph executionGraph = executionStage.getExecutionGraph();
//
//		synchronized (executionStage) {
//
//			final int requiredSlots = executionStage.getRequiredSlots();
//
//			LOG.info("Requesting " + requiredSlots + " slots for job " + executionGraph.getJobID());
//
//			this.instanceManager.requestInstance(executionGraph.getJobID(), executionGraph.getJobConfiguration(),
//				requiredSlots);
//
//			// Switch vertex state to assigning
//			final ExecutionGraphIterator it2 = new ExecutionGraphIterator(executionGraph, executionGraph
//				.getIndexOfCurrentExecutionStage(), true, true);
//			while (it2.hasNext()) {
//
//				it2.next().compareAndUpdateExecutionState(ExecutionState.CREATED, ExecutionState.SCHEDULED);
//			}
//		}
//	}
//
//	void findVerticesToBeDeployed(final ExecutionVertex vertex,
//			final Map<Instance, List<ExecutionVertex>> verticesToBeDeployed,
//			final Set<ExecutionVertex> alreadyVisited) {
//
//		if (!alreadyVisited.add(vertex)) {
//			return;
//		}
//
//		if (vertex.compareAndUpdateExecutionState(ExecutionState.ASSIGNED, ExecutionState.READY)) {
//			final Instance instance = vertex.getAllocatedResource().getInstance();
//
//			if (instance instanceof DummyInstance) {
//				LOG.error("Inconsistency: Vertex " + vertex + " is about to be deployed on a DummyInstance");
//			}
//
//			List<ExecutionVertex> verticesForInstance = verticesToBeDeployed.get(instance);
//			if (verticesForInstance == null) {
//				verticesForInstance = new ArrayList<ExecutionVertex>();
//				verticesToBeDeployed.put(instance, verticesForInstance);
//			}
//
//			verticesForInstance.add(vertex);
//		}
//
//		final int numberOfOutputGates = vertex.getNumberOfOutputGates();
//		for (int i = 0; i < numberOfOutputGates; ++i) {
//
//			final ExecutionGate outputGate = vertex.getOutputGate(i);
//			boolean deployTarget;
//
//			switch (outputGate.getChannelType()) {
//			case NETWORK:
//				deployTarget = false;
//				break;
//			case IN_MEMORY:
//				deployTarget = true;
//				break;
//			default:
//				throw new IllegalStateException("Unknown channel type");
//			}
//
//			if (deployTarget) {
//
//				final int numberOfOutputChannels = outputGate.getNumberOfEdges();
//				for (int j = 0; j < numberOfOutputChannels; ++j) {
//					final ExecutionEdge outputChannel = outputGate.getEdge(j);
//					final ExecutionVertex connectedVertex = outputChannel.getInputGate().getVertex();
//					findVerticesToBeDeployed(connectedVertex, verticesToBeDeployed, alreadyVisited);
//				}
//			}
//		}
//	}
//
//	/**
//	 * Collects all execution vertices with the state ASSIGNED starting from the given start vertex and
//	 * deploys them on the assigned {@link org.apache.flink.runtime.instance.AllocatedResource} objects.
//	 * 
//	 * @param startVertex
//	 *        the execution vertex to start the deployment from
//	 */
//	public void deployAssignedVertices(final ExecutionVertex startVertex) {
//
//		final JobID jobID = startVertex.getExecutionGraph().getJobID();
//
//		final Map<Instance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<Instance, List<ExecutionVertex>>();
//		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();
//
//		findVerticesToBeDeployed(startVertex, verticesToBeDeployed, alreadyVisited);
//
//		if (!verticesToBeDeployed.isEmpty()) {
//
//			final Iterator<Map.Entry<Instance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
//				.entrySet()
//				.iterator();
//
//			while (it2.hasNext()) {
//
//				final Map.Entry<Instance, List<ExecutionVertex>> entry = it2.next();
//				this.deploymentManager.deploy(jobID, entry.getKey(), entry.getValue());
//			}
//		}
//	}
//
//	/**
//	 * Collects all execution vertices with the state ASSIGNED from the given pipeline and deploys them on the assigned
//	 * {@link org.apache.flink.runtime.instance.AllocatedResource} objects.
//	 * 
//	 * @param pipeline
//	 *        the execution pipeline to be deployed
//	 */
//	public void deployAssignedPipeline(final ExecutionPipeline pipeline) {
//
//		final JobID jobID = null;
//
//		final Map<Instance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<Instance, List<ExecutionVertex>>();
//		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();
//
//		final Iterator<ExecutionVertex> it = pipeline.iterator();
//		while (it.hasNext()) {
//			findVerticesToBeDeployed(it.next(), verticesToBeDeployed, alreadyVisited);
//		}
//
//		if (!verticesToBeDeployed.isEmpty()) {
//
//			final Iterator<Map.Entry<Instance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
//				.entrySet()
//				.iterator();
//
//			while (it2.hasNext()) {
//
//				final Map.Entry<Instance, List<ExecutionVertex>> entry = it2.next();
//				this.deploymentManager.deploy(jobID, entry.getKey(), entry.getValue());
//			}
//		}
//	}
//
//	/**
//	 * Collects all execution vertices with the state ASSIGNED starting from the given collection of start vertices and
//	 * deploys them on the assigned {@link org.apache.flink.runtime.instance.AllocatedResource} objects.
//	 * 
//	 * @param startVertices
//	 *        the collection of execution vertices to start the deployment from
//	 */
//	public void deployAssignedVertices(final Collection<ExecutionVertex> startVertices) {
//
//		JobID jobID = null;
//
//		final Map<Instance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<Instance, List<ExecutionVertex>>();
//		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();
//
//		for (final ExecutionVertex startVertex : startVertices) {
//
//			if (jobID == null) {
//				jobID = startVertex.getExecutionGraph().getJobID();
//			}
//
//			findVerticesToBeDeployed(startVertex, verticesToBeDeployed, alreadyVisited);
//		}
//
//		if (!verticesToBeDeployed.isEmpty()) {
//
//			final Iterator<Map.Entry<Instance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
//				.entrySet()
//				.iterator();
//
//			while (it2.hasNext()) {
//
//				final Map.Entry<Instance, List<ExecutionVertex>> entry = it2.next();
//				this.deploymentManager.deploy(jobID, entry.getKey(), entry.getValue());
//			}
//		}
//	}
//
//	/**
//	 * Collects all execution vertices with the state ASSIGNED starting from the input vertices of the current execution
//	 * stage and deploys them on the assigned {@link org.apache.flink.runtime.instance.AllocatedResource} objects.
//	 * 
//	 * @param executionGraph
//	 *        the execution graph to collect the vertices from
//	 */
//	public void deployAssignedInputVertices(final ExecutionGraph executionGraph) {
//
//		final Map<Instance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<Instance, List<ExecutionVertex>>();
//		final ExecutionStage executionStage = executionGraph.getCurrentExecutionStage();
//
//		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();
//
//		for (int i = 0; i < executionStage.getNumberOfStageMembers(); ++i) {
//
//			final ExecutionGroupVertex startVertex = executionStage.getStageMember(i);
//			if (!startVertex.isInputVertex()) {
//				continue;
//			}
//
//			for (int j = 0; j < startVertex.getCurrentNumberOfGroupMembers(); ++j) {
//				final ExecutionVertex vertex = startVertex.getGroupMember(j);
//				findVerticesToBeDeployed(vertex, verticesToBeDeployed, alreadyVisited);
//			}
//		}
//
//		if (!verticesToBeDeployed.isEmpty()) {
//
//			final Iterator<Map.Entry<Instance, List<ExecutionVertex>>> it2 = verticesToBeDeployed
//				.entrySet()
//				.iterator();
//
//			while (it2.hasNext()) {
//
//				final Map.Entry<Instance, List<ExecutionVertex>> entry = it2.next();
//				this.deploymentManager.deploy(executionGraph.getJobID(), entry.getKey(), entry.getValue());
//			}
//		}
//	}
//
//
//	@Override
//	public void resourcesAllocated(final JobID jobID, final List<AllocatedResource> allocatedResources) {
//
//		if (allocatedResources == null) {
//			LOG.error("Resource to lock is null!");
//			return;
//		}
//
//		for (final AllocatedResource allocatedResource : allocatedResources) {
//			if (allocatedResource.getInstance() instanceof DummyInstance) {
//				LOG.debug("Available instance is of type DummyInstance!");
//				return;
//			}
//		}
//
//		final ExecutionGraph eg = getExecutionGraphByID(jobID);
//
//		if (eg == null) {
//			/*
//			 * The job have have been canceled in the meantime, in this case
//			 * we release the instance immediately.
//			 */
//			try {
//				for (final AllocatedResource allocatedResource : allocatedResources) {
//					getInstanceManager().releaseAllocatedResource(allocatedResource);
//				}
//			} catch (InstanceException e) {
//				LOG.error(e);
//			}
//			return;
//		}
//
//		final Runnable command = new Runnable() {
//
//			/**
//			 * {@inheritDoc}
//			 */
//			@Override
//			public void run() {
//
//				final ExecutionStage stage = eg.getCurrentExecutionStage();
//
//				synchronized (stage) {
//
//					for (final AllocatedResource allocatedResource : allocatedResources) {
//
//						AllocatedResource resourceToBeReplaced = null;
//						// Important: only look for instances to be replaced in the current stage
//						final Iterator<ExecutionGroupVertex> groupIterator = new ExecutionGroupVertexIterator(eg, true,
//							stage.getStageNumber());
//						while (groupIterator.hasNext()) {
//
//							final ExecutionGroupVertex groupVertex = groupIterator.next();
//							for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); ++i) {
//
//								final ExecutionVertex vertex = groupVertex.getGroupMember(i);
//
//								if (vertex.getExecutionState() == ExecutionState.SCHEDULED
//									&& vertex.getAllocatedResource() != null) {
//										resourceToBeReplaced = vertex.getAllocatedResource();
//										break;
//								}
//							}
//
//							if (resourceToBeReplaced != null) {
//								break;
//							}
//						}
//
//						// For some reason, we don't need this instance
//						if (resourceToBeReplaced == null) {
//							LOG.error("Instance " + allocatedResource.getInstance() + " is not required for job"
//								+ eg.getJobID());
//							try {
//								getInstanceManager().releaseAllocatedResource(allocatedResource);
//							} catch (InstanceException e) {
//								LOG.error(e);
//							}
//							return;
//						}
//
//						// Replace the selected instance
//						final Iterator<ExecutionVertex> it = resourceToBeReplaced.assignedVertices();
//						while (it.hasNext()) {
//							final ExecutionVertex vertex = it.next();
//							vertex.setAllocatedResource(allocatedResource);
//							vertex.updateExecutionState(ExecutionState.ASSIGNED);
//						}
//					}
//				}
//
//				// Deploy the assigned vertices
//				deployAssignedInputVertices(eg);
//
//			}
//
//		};
//
//		eg.executeCommand(command);
//	}
//
//	/**
//	 * Checks if the given {@link AllocatedResource} is still required for the
//	 * execution of the given execution graph. If the resource is no longer
//	 * assigned to a vertex that is either currently running or about to run
//	 * the given resource is returned to the instance manager for deallocation.
//	 * 
//	 * @param executionGraph
//	 *        the execution graph the provided resource has been used for so far
//	 * @param allocatedResource
//	 *        the allocated resource to check the assignment for
//	 */
//	public void checkAndReleaseAllocatedResource(final ExecutionGraph executionGraph,
//			final AllocatedResource allocatedResource) {
//
//		if (allocatedResource == null) {
//			LOG.error("Resource to lock is null!");
//			return;
//		}
//
//		if (allocatedResource.getInstance() instanceof DummyInstance) {
//			LOG.debug("Available instance is of type DummyInstance!");
//			return;
//		}
//
//		boolean resourceCanBeReleased = true;
//		final Iterator<ExecutionVertex> it = allocatedResource.assignedVertices();
//		while (it.hasNext()) {
//			final ExecutionVertex vertex = it.next();
//			final ExecutionState state = vertex.getExecutionState();
//
//			if (state != ExecutionState.CREATED && state != ExecutionState.FINISHED
//				&& state != ExecutionState.FAILED && state != ExecutionState.CANCELED) {
//
//				resourceCanBeReleased = false;
//				break;
//			}
//		}
//
//		if (resourceCanBeReleased) {
//
//			LOG.info("Releasing instance " + allocatedResource.getInstance());
//			try {
//				getInstanceManager().releaseAllocatedResource(allocatedResource);
//			} catch (InstanceException e) {
//				LOG.error(StringUtils.stringifyException(e));
//			}
//		}
//	}
//
//	DeploymentManager getDeploymentManager() {
//		return this.deploymentManager;
//	}
//
//
//
//	@Override
//	public void allocatedResourcesDied(final JobID jobID, final List<AllocatedResource> allocatedResources) {
//
//		final ExecutionGraph eg = getExecutionGraphByID(jobID);
//
//		if (eg == null) {
//			LOG.error("Cannot find execution graph for job with ID " + jobID);
//			return;
//		}
//
//		final Runnable command = new Runnable() {
//
//			/**
//			 * {@inheritDoc}
//			 */
//			@Override
//			public void run() {
//
//				synchronized (eg) {
//
//					for (final AllocatedResource allocatedResource : allocatedResources) {
//
//						LOG.info("Resource " + allocatedResource.getInstance().getName() + " for Job " + jobID
//							+ " died.");
//
//						final ExecutionGraph executionGraph = getExecutionGraphByID(jobID);
//
//						if (executionGraph == null) {
//							LOG.error("Cannot find execution graph for job " + jobID);
//							return;
//						}
//
//						Iterator<ExecutionVertex> vertexIter = allocatedResource.assignedVertices();
//
//						// Assign vertices back to a dummy resource.
//						final DummyInstance dummyInstance = DummyInstance.createDummyInstance();
//						final AllocatedResource dummyResource = new AllocatedResource(dummyInstance,
//								new AllocationID());
//
//						while (vertexIter.hasNext()) {
//							final ExecutionVertex vertex = vertexIter.next();
//							vertex.setAllocatedResource(dummyResource);
//						}
//
//						final String failureMessage = allocatedResource.getInstance().getName() + " died";
//
//						vertexIter = allocatedResource.assignedVertices();
//
//						while (vertexIter.hasNext()) {
//							final ExecutionVertex vertex = vertexIter.next();
//							final ExecutionState state = vertex.getExecutionState();
//
//							switch (state) {
//							case ASSIGNED:
//							case READY:
//							case STARTING:
//							case RUNNING:
//							case FINISHING:
//
//							vertex.updateExecutionState(ExecutionState.FAILED, failureMessage);
//
//							break;
//						default:
//							}
//					}
//
//					// TODO: Fix this
//					/*
//					 * try {
//					 * requestInstances(this.executionVertex.getGroupVertex().getExecutionStage());
//					 * } catch (InstanceException e) {
//					 * e.printStackTrace();
//					 * // TODO: Cancel the entire job in this case
//					 * }
//					 */
//				}
//			}
//
//			final InternalJobStatus js = eg.getJobStatus();
//			if (js != InternalJobStatus.FAILING && js != InternalJobStatus.FAILED) {
//
//				// TODO: Fix this
//				// deployAssignedVertices(eg);
//
//				final ExecutionStage stage = eg.getCurrentExecutionStage();
//
//				try {
//					requestInstances(stage);
//				} catch (InstanceException e) {
//					e.printStackTrace();
//					// TODO: Cancel the entire job in this case
//				}
//			}
//		}
//		};
//
//		eg.executeCommand(command);
//	}
	
	// --------------------------------------------------------------------------------------------
	//  Canceling
	// --------------------------------------------------------------------------------------------
	
	public void removeAllTasksForJob(JobID job) {
		
	}

	// --------------------------------------------------------------------------------------------
	//  Instance Availability
	// --------------------------------------------------------------------------------------------
	
	
	@Override
	public void newInstanceAvailable(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		if (instance.getNumberOfAvailableSlots() <= 0) {
			throw new IllegalArgumentException("The given instance has no resources.");
		}
		if (!instance.isAlive()) {
			throw new IllegalArgumentException("The instance is not alive.");
		}
		
		// synchronize globally for instance changes
		synchronized (this.lock) {
			// check we do not already use this instance
			if (!this.allInstances.add(instance)) {
				throw new IllegalArgumentException("The instance is already contained.");
			}
			
			// add it to the available resources and let potential waiters know
			this.instancesWithAvailableResources.add(instance);
			this.lock.notifyAll();
		}
	}
	
	@Override
	public void instanceDied(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		
		instance.markDead();
		
		// we only remove the instance from the pools, we do not care about the 
		synchronized (this.lock) {
			// the instance must not be available anywhere any more
			this.allInstances.remove(instance);
			this.instancesWithAvailableResources.remove(instance);
		}
	}
	
	public int getNumberOfAvailableInstances() {
		synchronized (lock) {
			return allInstances.size();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Scheduling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Schedules the given unit to an available resource. This call blocks if no resource
	 * is currently available
	 * 
	 * @param unit The unit to be scheduled.
	 */
	protected void scheduleNextUnit(ScheduledUnit unit) {
		if (unit == null) {
			throw new IllegalArgumentException("Unit to schedule must not be null.");
		}
		
		// see if the resource Id has already an assigned resource
		AllocatedSlot resource = this.allocatedSlots.get(unit.getSharedResourceId());
		
		if (resource == null) {
			// not yet allocated. find a slot to schedule to
			try {
				resource = getResourceToScheduleUnit(unit, this.rejectIfNoResourceAvailable);
				if (resource == null) {
					throw new RuntimeException("Error: The resource to schedule to is null.");
				}
			}
			catch (Exception e) {
				// we cannot go on, the task needs to know what to do now.
				unit.getTaskVertex().handleException(e);
				return;
			}
		}
		
		resource.runTask(unit.getTaskVertex());
	}
	
	/**
	 * Acquires a resource to schedule the given unit to. This call may block if no
	 * resource is currently available, or throw an exception, based on the given flag.
	 * 
	 * @param unit The unit to find a resource for.
	 * @param exceptionOnNoAvailability If true, this call should not block is no resource is available,
	 *                                  but throw a {@link NoResourceAvailableException}.
	 * @return The resource to schedule the execution of the given unit on.
	 * 
	 * @throws NoResourceAvailableException If the {@code exceptionOnNoAvailability} flag is true and the scheduler
	 *                                      has currently no resources available.
	 */
	protected AllocatedSlot getResourceToScheduleUnit(ScheduledUnit unit, boolean exceptionOnNoAvailability) 
		throws NoResourceAvailableException
	{
		AllocatedSlot slot = null;
		
		while (true) {
			synchronized (this.lock) {
				Instance instanceToUse = this.instancesWithAvailableResources.poll();
				
				// if there is nothing, throw an exception or wait, depending on what is configured
				if (instanceToUse == null) {
					if (exceptionOnNoAvailability) {
						throw new NoResourceAvailableException(unit);
					}
					else {
						try {
							do {
								this.lock.wait(2000);
							}
							while (!shutdown.get() && 
									(instanceToUse = this.instancesWithAvailableResources.poll()) == null);
						}
						catch (InterruptedException e) {
							throw new NoResourceAvailableException("The scheduler was interrupted.");
						}
					}
				}
				
				// at this point, we have an instance. request a slot from the instance
				try {
					slot = instanceToUse.allocateSlot(unit.getJobId(), unit.getSharedResourceId());
				}
				catch (InstanceDiedException e) {
					// the instance died it has not yet been propagated to this scheduler
					// remove the instance from the set of available instances
					this.allInstances.remove(instanceToUse);
				}
				
				// if the instance has further available slots, re-add it to the set of available
				// resources.
				// if it does not, but asynchronously 
				if (instanceToUse.hasResourcesAvailable()) {
					this.instancesWithAvailableResources.add(instanceToUse);
				}
				
				if (slot != null) {
					return slot;
				}
				// else fall through the loop
			}
		}
	}
	
	protected void runSchedulerLoop() {
		// while the scheduler is alive
		while (!shutdown.get()) {
			
			// get the next unit
			ScheduledUnit next = null;
			try {
				next = this.taskQueue.take();
			}
			catch (InterruptedException e) {
				if (shutdown.get()) {
					return;
				} else {
					LOG.error("Scheduling loop was interrupted.");
				}
			}
			
			// if we see this special unit, it means we are done
			if (next == TERMINATION_SIGNAL) {
				return;
			}
			
			// deploy the next scheduling unit
			try {
				scheduleNextUnit(next);
			}
			catch (Throwable t) {
				// ignore the errors in the presence of a shutdown
				if (!shutdown.get()) {
					if (t instanceof Error) {
						throw (Error) t;
					} else if (t instanceof RuntimeException) {
						throw (RuntimeException) t;
					} else {
						throw new RuntimeException("Critical error in scheduler thread.", t);
					}
				}
			}
		}
	}
	
	private static final ScheduledUnit TERMINATION_SIGNAL = new ScheduledUnit();
}
