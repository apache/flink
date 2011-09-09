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

package eu.stratosphere.nephele.jobmanager.scheduler.local;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.StringUtils;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionStageListener;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;
import eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;

public class LocalScheduler extends AbstractScheduler implements JobStatusListener, ExecutionStageListener {

	/**
	 * The job queue of the scheduler
	 */
	private Deque<ExecutionGraph> jobQueue = new ArrayDeque<ExecutionGraph>();

	/**
	 * Constructs a new local scheduler.
	 * 
	 * @param deploymentManager
	 *        the deployment manager assigned to this scheduler
	 * @param instanceManager
	 *        the instance manager to be used with this scheduler
	 */
	public LocalScheduler(final DeploymentManager deploymentManager, final InstanceManager instanceManager) {
		super(deploymentManager, instanceManager);
	}

	void removeJobFromSchedule(ExecutionGraph executionGraphToRemove) {

		boolean removedFromQueue = false;

		synchronized (this.jobQueue) {

			final Iterator<ExecutionGraph> it = this.jobQueue.iterator();
			while (it.hasNext()) {

				final ExecutionGraph executionGraph = it.next();
				if (executionGraph.getJobID().equals(executionGraphToRemove.getJobID())) {
					removedFromQueue = true;
					it.remove();
					break;
				}
			}
		}

		if (!removedFromQueue) {
			LOG.error("Cannot find job " + executionGraphToRemove.getJobName() + " ("
				+ executionGraphToRemove.getJobID() + ") to remove");
		}
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
	void checkAndReleaseAllocatedResource(ExecutionGraph executionGraph, AllocatedResource allocatedResource) {

		if (allocatedResource == null) {
			LOG.error("Resource to lock is null!");
			return;
		}

		if (allocatedResource.getInstance() instanceof DummyInstance) {
			LOG.debug("Available instance is of type DummyInstance!");
			return;
		}

		final List<ExecutionVertex> assignedVertices = executionGraph
			.getVerticesAssignedToResource(allocatedResource);
		if (assignedVertices.isEmpty()) {
			return;
		}

		synchronized (this.jobQueue) {

			boolean instanceCanBeReleased = true;
			final Iterator<ExecutionVertex> it = assignedVertices.iterator();
			while (it.hasNext()) {
				final ExecutionVertex vertex = it.next();
				final ExecutionState state = vertex.getExecutionState();

				if (state == ExecutionState.ASSIGNED || state == ExecutionState.READY
					|| state == ExecutionState.RUNNING || state == ExecutionState.FINISHING
					|| state == ExecutionState.CANCELING) {
					instanceCanBeReleased = false;
					break;
				}
			}

			if (instanceCanBeReleased) {
				LOG.info("Releasing instance " + allocatedResource.getInstance());
				try {
					getInstanceManager().releaseAllocatedResource(executionGraph.getJobID(), executionGraph
						.getJobConfiguration(), allocatedResource);
				} catch (InstanceException e) {
					LOG.error(StringUtils.stringifyException(e));
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void schedulJob(ExecutionGraph executionGraph) throws SchedulingException {

		// First, check if there are enough resources to run this job
		final Map<InstanceType, InstanceTypeDescription> availableInstances = getInstanceManager()
			.getMapOfAvailableInstanceTypes();

		for (int i = 0; i < executionGraph.getNumberOfStages(); i++) {

			final Map<InstanceType, Integer> requiredInstanceTypes = new HashMap<InstanceType, Integer>();
			final ExecutionStage stage = executionGraph.getStage(i);
			stage.collectRequiredInstanceTypes(requiredInstanceTypes, ExecutionState.CREATED);

			final Iterator<Map.Entry<InstanceType, Integer>> it = requiredInstanceTypes.entrySet().iterator();
			while (it.hasNext()) {

				final Map.Entry<InstanceType, Integer> entry = it.next();
				final InstanceTypeDescription descr = availableInstances.get(entry.getKey());
				if (descr == null) {
					throw new SchedulingException("Unable to schedule job: No instance of type " + entry.getKey()
						+ " available");
				}

				if (descr.getMaximumNumberOfAvailableInstances() != -1
					&& descr.getMaximumNumberOfAvailableInstances() < entry.getValue().intValue()) {
					throw new SchedulingException("Unable to schedule job: " + entry.getValue().intValue()
						+ " instances of type " + entry.getKey() + " required, but only "
						+ descr.getMaximumNumberOfAvailableInstances() + " are available");
				}
			}
		}

		// Subscribe to job status notifications
		executionGraph.registerJobStatusListener(this);

		// Set state of each vertex for scheduled
		final ExecutionGraphIterator it2 = new ExecutionGraphIterator(executionGraph, true);
		while (it2.hasNext()) {

			final ExecutionVertex vertex = it2.next();
			if (vertex.getExecutionState() != ExecutionState.CREATED) {
				LOG.error("Execution vertex " + vertex + " has state " + vertex.getExecutionState() + ", expected "
					+ ExecutionState.CREATED);
			}

			vertex.getEnvironment().registerExecutionListener(new LocalExecutionListener(this, vertex));
			vertex.setExecutionState(ExecutionState.SCHEDULED);

		}

		// Register the scheduler as an execution stage listener
		executionGraph.registerExecutionStageListener(this);

		// Add job to the job queue (important to add job to queue before requesting instances)
		synchronized (this.jobQueue) {
			this.jobQueue.add(executionGraph);

			// Request resources for the first stage of the job
			final ExecutionStage executionStage = executionGraph.getCurrentExecutionStage();
			try {
				requestInstances(executionStage);
			} catch (InstanceException e) {
				final String exceptionMessage = StringUtils.stringifyException(e);
				LOG.error(exceptionMessage);
				this.jobQueue.remove(executionGraph);
				throw new SchedulingException(exceptionMessage);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionGraph getExecutionGraphByID(JobID jobID) {

		synchronized (this.jobQueue) {

			final Iterator<ExecutionGraph> it = this.jobQueue.iterator();
			while (it.hasNext()) {

				final ExecutionGraph executionGraph = it.next();
				if (executionGraph.getJobID().equals(jobID)) {
					return executionGraph;
				}
			}
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void resourceAllocated(JobID jobID, AllocatedResource allocatedResource) {

		if (allocatedResource == null) {
			LOG.error("Resource to lock is null!");
			return;
		}

		if (allocatedResource.getInstance() instanceof DummyInstance) {
			LOG.debug("Available instance is of type DummyInstance!");
			return;
		}

		synchronized (this.jobQueue) {

			final ExecutionGraph eg = getExecutionGraphByID(jobID);
			if (eg == null) {
				/*
				 * The job have have been canceled in the meantime, in this case
				 * we release the instance immediately.
				 */
				try {
					getInstanceManager().releaseAllocatedResource(jobID, null, allocatedResource);
				} catch (InstanceException e) {
					LOG.error(e);
				}
				return;
			}

			final int indexOfCurrentStage = eg.getIndexOfCurrentExecutionStage();

			AllocatedResource resourceToBeReplaced = null;
			// Important: only look for instances to be replaced in the current stage
			ExecutionGraphIterator it = new ExecutionGraphIterator(eg, indexOfCurrentStage, true, true);
			while (it.hasNext()) {

				final ExecutionVertex vertex = it.next();
				if (vertex.getExecutionState() == ExecutionState.ASSIGNING && vertex.getAllocatedResource() != null) {
					// In local mode, we do not consider any topology, only the instance type
					if (vertex.getAllocatedResource().getInstanceType().equals(
						allocatedResource.getInstanceType())) {
						resourceToBeReplaced = vertex.getAllocatedResource();
						break;
					}
				}
			}

			// For some reason, we don't need this instance
			if (resourceToBeReplaced == null) {
				LOG.error("Instance " + allocatedResource.getInstance() + " is not required for job" + eg.getJobID());
				try {
					getInstanceManager().releaseAllocatedResource(jobID, eg.getJobConfiguration(), allocatedResource);
				} catch (InstanceException e) {
					LOG.error(e);
				}
				return;
			}

			// Replace the selected instance in the entire graph with the new instance
			it = new ExecutionGraphIterator(eg, true);
			while (it.hasNext()) {
				final ExecutionVertex vertex = it.next();
				if (vertex.getAllocatedResource().equals(resourceToBeReplaced)) {
					vertex.setAllocatedResource(allocatedResource);
					vertex.setExecutionState(ExecutionState.ASSIGNED);
				}
			}

			// Deploy the assigned vertices
			deployAssignedVertices(eg);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void allocatedResourceDied(final JobID jobID, final AllocatedResource allocatedResource) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {

		synchronized (this.jobQueue) {
			this.jobQueue.clear();
		}

	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.jobmanager.scheduler.Scheduler#reportPersistenCheckpoint(eu.stratosphere.nephele.executiongraph.ExecutionVertexID, eu.stratosphere.nephele.jobgraph.JobID)
	 */
	@Override
	public void reportPersistenCheckpoint(ExecutionVertexID executionVertexID, JobID jobID) {
		getExecutionGraphByID(jobID).getVertexByID(executionVertexID).setCheckpoint();
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void jobStatusHasChanged(final ExecutionGraph executionGraph, final InternalJobStatus newJobStatus,
			final String optionalMessage) {

		if (newJobStatus == InternalJobStatus.FAILED || newJobStatus == InternalJobStatus.FINISHED
			|| newJobStatus == InternalJobStatus.CANCELED) {
			removeJobFromSchedule(executionGraph);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void nextExecutionStageEntered(final JobID jobID, final ExecutionStage executionStage) {

		synchronized (this.jobQueue) {

			// Request new instances if necessary
			try {
				requestInstances(executionStage);
			} catch (InstanceException e) {
				// TODO: Handle this error correctly
				LOG.error(StringUtils.stringifyException(e));
			}

			// Deploy the assigned vertices
			deployAssignedVertices(executionStage.getExecutionGraph());
		}

	}
}
