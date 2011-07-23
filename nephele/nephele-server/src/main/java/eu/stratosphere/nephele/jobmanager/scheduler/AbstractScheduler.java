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

package eu.stratosphere.nephele.jobmanager.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;

/**
 * This abstract scheduler must be extended by a scheduler implementations for Nephele. The abstract class defines the
 * fundamental methods for scheduling and removing jobs. While Nephele's
 * {@link eu.stratosphere.nephele.jobmanager.JobManager} is responsible for requesting the required instances for the
 * job at the {@link eu.stratosphere.nephele.instance.InstanceManager}, the scheduler is in charge of assigning the
 * individual tasks to the instances.
 * 
 * @author warneke
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
		executionStage.collectRequiredInstanceTypes(instanceRequestMap, ExecutionState.SCHEDULED);

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

		/*
		 * ... stuff moved to instance manager
		 * final Iterator<Map.Entry<InstanceType, Integer>> it = requiredInstances.entrySet().iterator();
		 * while (it.hasNext()) {
		 * final Map.Entry<InstanceType, Integer> entry = it.next();
		 * for (int i = 0; i < entry.getValue().intValue(); i++) {
		 * LOG.info("Trying to allocate instance of type " + entry.getKey().getIdentifier());
		 * this.instanceManager.requestInstance(executionGraph.getJobID(), executionGraph.getJobConfiguration(),
		 * entry.getKey());
		 * }
		 * this.instanceManager.requestInstance(executionGraph.getJobID(), executionGraph.getJobConfiguration(),
		 * entry.getKey(), entry.getValue().intValue());
		 * }
		 */

		// Switch vertex state to assigning
		final ExecutionGraphIterator it2 = new ExecutionGraphIterator(executionGraph, executionGraph
			.getIndexOfCurrentExecutionStage(), true, true);
		while (it2.hasNext()) {

			final ExecutionVertex vertex = it2.next();
			if (vertex.getExecutionState() == ExecutionState.SCHEDULED) {
				vertex.setExecutionState(ExecutionState.ASSIGNING);
			}
		}
	}

	/**
	 * Collects all execution vertices with the state ASSIGNED from the current execution stage and deploys them on the
	 * assigned {@link AllocatedResource} objects.
	 * 
	 * @param executionGraph
	 *        the execution graph to collect the vertices from
	 */
	public void deployAssignedVertices(final ExecutionGraph executionGraph) {

		final Map<AbstractInstance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<AbstractInstance, List<ExecutionVertex>>();
		final int indexOfCurrentExecutionStage = executionGraph.getIndexOfCurrentExecutionStage();

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(executionGraph, indexOfCurrentExecutionStage,
			true, true);

		while (it.hasNext()) {
			final ExecutionVertex vertex = it.next();
			if (vertex.getExecutionState() == ExecutionState.ASSIGNED) {
				final AbstractInstance instance = vertex.getAllocatedResource().getInstance();

				if (instance instanceof DummyInstance) {
					LOG.error("Inconsistency: Vertex " + vertex.getName() + "("
						+ vertex.getEnvironment().getIndexInSubtaskGroup() + "/"
						+ vertex.getEnvironment().getCurrentNumberOfSubtasks()
						+ ") is about to be deployed on a DummyInstance");
				}

				List<ExecutionVertex> verticesForInstance = verticesToBeDeployed.get(instance);
				if (verticesForInstance == null) {
					verticesForInstance = new ArrayList<ExecutionVertex>();
					verticesToBeDeployed.put(instance, verticesForInstance);
				}

				verticesForInstance.add(vertex);
				vertex.setExecutionState(ExecutionState.READY);
			}
		}

		if (!verticesToBeDeployed.isEmpty()) {

			final Iterator<Map.Entry<AbstractInstance, List<ExecutionVertex>>> it2 = verticesToBeDeployed.entrySet()
				.iterator();

			while (it2.hasNext()) {

				final Map.Entry<AbstractInstance, List<ExecutionVertex>> entry = it2.next();
				this.deploymentManager.deploy(executionGraph.getJobID(), entry.getKey(), entry.getValue());
			}
		}
	}
}
