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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionPipeline;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.local.LocalScheduler;
import eu.stratosphere.nephele.types.Record;

public abstract class AbstractExecutionListener implements ExecutionListener {

	/**
	 * The instance of the {@link LocalScheduler}.
	 */
	private final AbstractScheduler scheduler;

	/**
	 * The {@link ExecutionVertex} this wrapper object belongs to.
	 */
	private final ExecutionVertex executionVertex;

	/**
	 * Constructs a new wrapper object for the given {@link ExecutionVertex}.
	 * 
	 * @param AbstractScheduler
	 *        the instance of the {@link AbstractScheduler}
	 * @param executionVertex
	 *        the {@link ExecutionVertex} the received notification refer to
	 */
	public AbstractExecutionListener(final AbstractScheduler scheduler, final ExecutionVertex executionVertex) {
		this.scheduler = scheduler;
		this.executionVertex = executionVertex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
			final ExecutionState newExecutionState, final String optionalMessage) {

		final ExecutionGraph eg = this.executionVertex.getExecutionGraph();

		synchronized (eg) {

			// Check if we can deploy a new pipeline.
			if (newExecutionState == ExecutionState.FINISHING) {

				final ExecutionPipeline pipeline = this.executionVertex.getExecutionPipeline();
				if (!pipeline.isFinishing()) {
					// Some tasks of the pipeline are still running
					return;
				}

				// Find another vertex in the group which is still in SCHEDULED state and get its pipeline.
				final ExecutionGroupVertex groupVertex = this.executionVertex.getGroupVertex();
				for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); ++i) {
					final ExecutionVertex groupMember = groupVertex.getGroupMember(i);
					if (groupMember.getExecutionState() == ExecutionState.SCHEDULED) {

						final ExecutionPipeline pipelineToBeDeployed = groupMember.getExecutionPipeline();
						pipelineToBeDeployed.setAllocatedResource(this.executionVertex.getAllocatedResource());
						pipelineToBeDeployed.updateExecutionState(ExecutionState.ASSIGNED);

						this.scheduler.deployAssignedVertices(eg);
						return;
					}
				}
			}

			if (newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
				|| newExecutionState == ExecutionState.FAILED) {
				// Check if instance can be released
				this.scheduler.checkAndReleaseAllocatedResource(eg, this.executionVertex.getAllocatedResource());
			}

			// In case of an error, check if vertex can be rescheduled
			if (newExecutionState == ExecutionState.FAILED) {
				if (this.executionVertex.hasRetriesLeft()) {
					// Reschedule vertex
					this.executionVertex.updateExecutionState(ExecutionState.SCHEDULED);
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialExecutionResourcesExhausted(final JobID jobID, final ExecutionVertexID vertexID,
			final ResourceUtilizationSnapshot resourceUtilizationSnapshot) {

		final ExecutionGraph executionGraph = this.executionVertex.getExecutionGraph();

		System.out.println(this.executionVertex + " has run out of execution resources");

		final Map<ExecutionVertex, Long> targetVertices = new HashMap<ExecutionVertex, Long>();
		final Map<AllocatedResource, Long> availableResources = new HashMap<AllocatedResource, Long>();

		synchronized (executionGraph) {

			final Environment ee = this.executionVertex.getEnvironment();

			for (int i = 0; i < ee.getNumberOfOutputGates(); ++i) {
				final OutputGate<? extends Record> outputGate = ee.getOutputGate(i);
				for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
					final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
					final long transmittedData = resourceUtilizationSnapshot.getAmountOfDataTransmitted(outputChannel
						.getID());

					final ExecutionVertex connectedVertex = executionGraph.getVertexByChannelID(outputChannel
						.getConnectedChannelID());
					final ExecutionState state = connectedVertex.getExecutionState();
					if (state == ExecutionState.SCHEDULED || state == ExecutionState.ASSIGNED) {
						targetVertices.put(connectedVertex, Long.valueOf(transmittedData));
						final AllocatedResource allocatedResource = connectedVertex.getAllocatedResource();
						if (!(allocatedResource.getInstance() instanceof DummyInstance)) {
							availableResources.put(allocatedResource, Long.valueOf(0L));
						}
					}
				}
			}

			if (targetVertices.isEmpty()) {
				return;
			}

			final Queue<ExecutionVertex> vertexQueue = new PriorityQueue<ExecutionVertex>(targetVertices.size(),
				new Comparator<ExecutionVertex>() {

					@Override
					public int compare(final ExecutionVertex arg0, final ExecutionVertex arg1) {

						final Long l0 = targetVertices.get(arg0);
						final Long l1 = targetVertices.get(arg1);

						if (l0.longValue() == l1.longValue()) {
							return 0;
						}

						if (l0.longValue() < l1.longValue()) {
							return 1;
						}

						return -1;
					}
				});

			final Queue<AllocatedResource> resourceQueue = new PriorityQueue<AllocatedResource>(
				availableResources.size(), new Comparator<AllocatedResource>() {

					@Override
					public int compare(final AllocatedResource arg0, final AllocatedResource arg1) {

						final Long l0 = availableResources.get(arg0);
						final Long l1 = availableResources.get(arg1);

						if (l0.longValue() == l1.longValue()) {
							return 0;
						}

						if (l0.longValue() < l1.longValue()) {
							return -1;
						}

						return 1;
					}
				});

			Iterator<ExecutionVertex> vertexIt = targetVertices.keySet().iterator();
			while (vertexIt.hasNext()) {
				vertexQueue.add(vertexIt.next());
			}

			final Iterator<AllocatedResource> resourceIt = availableResources.keySet().iterator();
			while (resourceIt.hasNext()) {
				resourceQueue.add(resourceIt.next());
			}

			while (!vertexQueue.isEmpty()) {

				final ExecutionVertex v = vertexQueue.poll();
				final long vertexLoad = targetVertices.get(v);
				System.out.println(v + ": " + vertexLoad);
				final AllocatedResource ar = resourceQueue.poll();
				final long resourceLoad = availableResources.get(ar).longValue();
				System.out.println(ar + ": " + resourceLoad);
				availableResources.put(ar, Long.valueOf(vertexLoad + resourceLoad));
				resourceQueue.add(ar);
				reassignGraphFragment(v, v.getAllocatedResource(), ar);
			}

			final Map<AbstractInstance, List<ExecutionVertex>> verticesToBeDeployed = new HashMap<AbstractInstance, List<ExecutionVertex>>();
			vertexIt = targetVertices.keySet().iterator();
			while (vertexIt.hasNext()) {

				this.scheduler.findVerticesToBeDeployed(vertexIt.next(), verticesToBeDeployed);
			}

			final Iterator<Map.Entry<AbstractInstance, List<ExecutionVertex>>> deploymentIt = verticesToBeDeployed
				.entrySet().iterator();

			while (deploymentIt.hasNext()) {
				final Map.Entry<AbstractInstance, List<ExecutionVertex>> entry = deploymentIt.next();
				this.scheduler.getDeploymentManager().deploy(executionGraph.getJobID(), entry.getKey(),
					entry.getValue());
			}
		}
	}

	private void reassignGraphFragment(final ExecutionVertex vertex, final AllocatedResource oldResource,
			final AllocatedResource newResource) {

		synchronized (vertex.getExecutionGraph()) {

			if (oldResource.equals(vertex.getAllocatedResource())) {
				vertex.setAllocatedResource(newResource);
				if (vertex.getExecutionState() == ExecutionState.SCHEDULED) {
					vertex.updateExecutionState(ExecutionState.ASSIGNED);
				}

				final int numberOfOutputGates = vertex.getEnvironment().getNumberOfOutputGates();
				for (int i = 0; i < numberOfOutputGates; ++i) {
					final OutputGate<? extends Record> outputGate = vertex.getEnvironment().getOutputGate(i);

					if (outputGate.getChannelType() == ChannelType.NETWORK) {
						continue;
					}

					final int numberOfOutputChannels = outputGate.getNumberOfOutputChannels();
					for (int j = 0; j < numberOfOutputChannels; ++j) {
						final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
						final ExecutionVertex connectedVertex = vertex.getExecutionGraph().getVertexByChannelID(
							outputChannel.getConnectedChannelID());
						reassignGraphFragment(connectedVertex, oldResource, newResource);
					}
				}
			}
		}
	}
}
