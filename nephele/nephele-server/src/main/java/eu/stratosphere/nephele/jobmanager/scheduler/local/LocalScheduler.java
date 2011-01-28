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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.Scheduler;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingListener;

public class LocalScheduler implements Scheduler {

	/**
	 * The LOG object to report events within the scheduler.
	 */
	private static final Log LOG = LogFactory.getLog(LocalScheduler.class);

	private Deque<ExecutionGraph> jobQueue = new ArrayDeque<ExecutionGraph>();

	private final InstanceManager instanceManager;

	private final SchedulingListener schedulingListener;

	public LocalScheduler(SchedulingListener schedulingListener, InstanceManager instanceManager) {

		this.schedulingListener = schedulingListener;

		// Set the instance manager
		this.instanceManager = instanceManager;
		this.instanceManager.setInstanceListener(this);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ExecutionVertex> getVerticesReadyToBeExecuted() {

		final Set<ExecutionVertex> verticesReadyToRun = new HashSet<ExecutionVertex>();

		synchronized (this.jobQueue) {

			if (jobQueue.isEmpty()) {
				// Return empty queue
				return verticesReadyToRun;
			}

			final ExecutionGraph executionGraph = jobQueue.getFirst();

			/*
			 * Request any instance of the job that might still be missing
			 * for completing the job's current execution stage.
			 */
			try {
				requestInstances(executionGraph);
			} catch (InstanceException e) {
				// This exception will never occur for the local instance manager
				LOG.error(e);
			}

			final ExecutionGraphIterator it = new ExecutionGraphIterator(executionGraph, executionGraph
				.getIndexOfCurrentExecutionStage(), true, true);
			while (it.hasNext()) {
				final ExecutionVertex vertex = it.next();
				if (vertex.getExecutionState() == ExecutionState.ASSIGNED) {
					vertex.setExecutionState(ExecutionState.READY);
					verticesReadyToRun.add(vertex);
				}
			}
		}

		return verticesReadyToRun;
	}

	/**
	 * Collects the instances required to run the job from the given {@link ExecutionGraph} and requests them at the
	 * loaded instance
	 * manager.
	 * 
	 * @param executionGraph
	 *        the execution graph to collect the required instances from
	 * @throws InstanceException
	 *         thrown if the given execution graph is already processing its final stage
	 */
	private void requestInstances(ExecutionGraph executionGraph) throws InstanceException {

		final Map<InstanceType, Integer> requiredInstanceTypes = new HashMap<InstanceType, Integer>();

		executionGraph.collectInstanceTypesRequiredForCurrentStage(requiredInstanceTypes, ExecutionState.SCHEDULED);

		// Switch vertex state to assigning
		final ExecutionGraphIterator it2 = new ExecutionGraphIterator(executionGraph, executionGraph
			.getIndexOfCurrentExecutionStage(), true, true);
		while (it2.hasNext()) {

			final ExecutionVertex vertex = it2.next();
			if (vertex.getExecutionState() == ExecutionState.SCHEDULED) {
				vertex.setExecutionState(ExecutionState.ASSIGNING);
			}
		}

		/*
		 * In the current version we try to allocate one instance per WS call. In the future might
		 * be preferable to allocate all instances at once.
		 */
		final Iterator<InstanceType> it = requiredInstanceTypes.keySet().iterator();
		while (it.hasNext()) {

			final InstanceType type = it.next();

			for (int i = 0; i < requiredInstanceTypes.get(type).intValue(); i++) {
				LOG.info("Trying to allocate instance of type " + type.getIdentifier());
				this.instanceManager.requestInstance(executionGraph.getJobID(), executionGraph.getJobConfiguration(),
					type);
			}
		}
	}

	void removeJobFromSchedule(ExecutionGraph executionGraphToRemove) {

		boolean removedFromQueue = false;
		;
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

		if (removedFromQueue) {
			this.schedulingListener.jobRemovedFromScheduler(executionGraphToRemove);
		} else {
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

		synchronized (this.jobQueue) {

			final List<ExecutionVertex> assignedVertices = executionGraph
				.getVerticesAssignedToResource(allocatedResource);
			if (assignedVertices.isEmpty()) {
				return;
			}

			boolean instanceCanBeReleased = true;
			final Iterator<ExecutionVertex> it = assignedVertices.iterator();
			while (it.hasNext()) {
				final ExecutionVertex vertex = it.next();
				final ExecutionState state = vertex.getExecutionState();

				if (state == ExecutionState.ASSIGNED || state == ExecutionState.READY
					|| state == ExecutionState.RUNNING || state == ExecutionState.FINISHING
					|| state == ExecutionState.CANCELLING) {
					instanceCanBeReleased = false;
					break;
				}
			}

			if (instanceCanBeReleased) {
				LOG.info("Releasing instance " + allocatedResource.getInstance());
				try {
					this.instanceManager.releaseAllocatedResource(executionGraph.getJobID(), executionGraph
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
		final Map<InstanceType, InstanceTypeDescription> availableInstances = this.instanceManager
			.getMapOfAvailableInstanceTypes();

		for (int i = 0; i < executionGraph.getNumberOfStages(); i++) {

			final Map<InstanceType, Integer> requiredInstanceTypes = new HashMap<InstanceType, Integer>();
			executionGraph.collectInstanceTypesRequiredForStage(i, requiredInstanceTypes, ExecutionState.CREATED);

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

		synchronized (this.jobQueue) {
			this.jobQueue.add(executionGraph);
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

		// Check if all required libraries are available on the instance
		try {
			allocatedResource.getInstance().checkLibraryAvailability(jobID);
		} catch (IOException ioe) {
			LOG.error("Cannot check library availability: " + StringUtils.stringifyException(ioe));
		}

		synchronized (this.jobQueue) {

			final ExecutionGraph eg = getExecutionGraphByID(jobID);
			if (eg == null) {
				/*
				 * The job have have been canceled in the meantime, in this case
				 * we release the instance immediately.
				 */
				try {
					this.instanceManager.releaseAllocatedResource(jobID, null, allocatedResource);
				} catch (InstanceException e) {
					LOG.error(e);
				}
			}

			AllocatedResource resourceToBeReplaced = null;
			// Important: only look for instances to be replaced in the current stage
			ExecutionGraphIterator it = new ExecutionGraphIterator(eg, eg.getIndexOfCurrentExecutionStage(), true, true);
			while (it.hasNext()) {

				final ExecutionVertex vertex = it.next();
				if (vertex.getExecutionState() == ExecutionState.ASSIGNING && vertex.getAllocatedResource() != null) {
					// In local mode, we do not consider any topology, only the instance type
					if (vertex.getAllocatedResource().getInstance().getType().equals(
						allocatedResource.getInstance().getType())) {
						resourceToBeReplaced = vertex.getAllocatedResource();
						break;
					}
				}
			}

			// For some reason, we don't need this instance
			if (resourceToBeReplaced == null) {
				LOG.error("Instance " + allocatedResource.getInstance() + " is not required for job" + eg.getJobID());
				try {
					this.instanceManager.releaseAllocatedResource(jobID, eg.getJobConfiguration(), allocatedResource);
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
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void allocatedResourceDied(JobID jobID, AllocatedResource allocatedResource) {
		// TODO Auto-generated method stub

	}

	@Override
	public InstanceManager getInstanceManager() {
		return this.instanceManager;
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
}
