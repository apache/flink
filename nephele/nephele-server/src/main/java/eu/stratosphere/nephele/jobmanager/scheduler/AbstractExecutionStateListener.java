/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionPipeline;
import eu.stratosphere.nephele.executiongraph.ExecutionStateListener;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.local.LocalScheduler;

public abstract class AbstractExecutionStateListener implements ExecutionStateListener {

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
	public AbstractExecutionStateListener(final AbstractScheduler scheduler, final ExecutionVertex executionVertex) {
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
				if (groupMember.compareAndUpdateExecutionState(ExecutionState.SCHEDULED, ExecutionState.ASSIGNED)) {

					final ExecutionPipeline pipelineToBeDeployed = groupMember.getExecutionPipeline();
					pipelineToBeDeployed.setAllocatedResource(this.executionVertex.getAllocatedResource());
					pipelineToBeDeployed.updateExecutionState(ExecutionState.ASSIGNED);

					this.scheduler.deployAssignedPipeline(pipelineToBeDeployed);
					return;
				}
			}
		}

		if (newExecutionState == ExecutionState.CANCELED || newExecutionState == ExecutionState.FINISHED) {

			synchronized (eg) {

				if (this.scheduler.getVerticesToBeRestarted().remove(this.executionVertex.getID()) != null) {

					if (eg.getJobStatus() == InternalJobStatus.FAILING) {
						return;
					}

					this.executionVertex.updateExecutionState(ExecutionState.ASSIGNED, "Restart as part of recovery");

					// Run through the deployment procedure
					this.scheduler.deployAssignedVertices(this.executionVertex);
					return;
				}
			}
		}

		if (newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
			|| newExecutionState == ExecutionState.FAILED) {
			// Check if instance can be released
			this.scheduler.checkAndReleaseAllocatedResource(eg, this.executionVertex.getAllocatedResource());
		}

		// In case of an error, check if the vertex shall be recovered
		if (newExecutionState == ExecutionState.FAILED) {
			if (this.executionVertex.decrementRetriesLeftAndCheck()) {

				final Set<ExecutionVertex> assignedVertices = new HashSet<ExecutionVertex>();

				boolean recover = false;
				try {
					recover = RecoveryLogic.recover(this.executionVertex, this.scheduler.getVerticesToBeRestarted(),
						assignedVertices);
				} catch (InterruptedException ie) {
					return;
				}

				if (recover) {

					if (RecoveryLogic.hasInstanceAssigned(this.executionVertex)) {
						// Run through the deployment procedure
						this.scheduler.deployAssignedVertices(assignedVertices);
					}

				} else {

					// Make sure the map with the vertices to be restarted is cleaned up properly
					synchronized (eg) {

						final Iterator<ExecutionVertex> it = this.scheduler.getVerticesToBeRestarted().values()
							.iterator();

						while (it.hasNext()) {
							if (eg.equals(it.next().getExecutionGraph())) {
								it.remove();
							}
						}
					}

					// Actual cancellation of job is performed by job manager
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPriority() {

		return 0;
	}
}
